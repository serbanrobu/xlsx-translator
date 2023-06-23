use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::Duration,
};

use calamine::{open_workbook, DataType, Reader, Xlsx};
use clap::Parser;
use color_eyre::{
    eyre::{bail, eyre, ContextCompat},
    Result,
};
use indicatif::ProgressBar;
use reqwest::{
    header::{HeaderMap, AUTHORIZATION},
    Client,
};
use serde::{Deserialize, Serialize};
use tiktoken_rs::get_completion_max_tokens;
use tokio::{sync::mpsc, time};
use xlsxwriter::Workbook;

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[arg(short('k'), long, env("OPENAI_API_KEY"), help("OpenAI API key"))]
    api_key: String,
    /// The path to a dictionary file containing entries in the following format:
    /// ```
    /// key – value
    /// ```
    #[arg(help(r#"Dictionary file path"#))]
    dictionary_path: PathBuf,
    #[arg(help("Source xlsx file path"))]
    source_path: PathBuf,
    #[arg(help("Destination xlsx file path"))]
    destination_path: PathBuf,
}

#[derive(Debug, Serialize)]
struct Request {
    model: &'static str,
    prompt: String,
    max_tokens: usize,
    temperature: f32,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Response {
    Ok { choices: Vec<Choice> },
    Err { error: Error },
}

#[derive(Debug, Deserialize)]
struct Choice {
    text: String,
}

#[derive(Debug, Deserialize)]
struct Error {
    message: String,
}

const MODEL: &str = "text-davinci-003";

const WORKSHEET: &str = "Worksheet";

async fn translate(prompt: String, client: &Client) -> Result<String> {
    let max_tokens = get_completion_max_tokens(MODEL, &prompt).map_err(|e| eyre!(e))?;

    let request = Request {
        model: MODEL,
        prompt,
        max_tokens,
        temperature: 0.,
    };

    let response = client
        .post("https://api.openai.com/v1/completions")
        .json(&request)
        .send()
        .await?
        .json::<Response>()
        .await?;

    let mut choices = match response {
        Response::Ok { choices } => choices,
        Response::Err { error } => bail!("{}", error.message),
    };

    let choice = choices.pop().wrap_err("No choice received")?;

    Ok(choice.text)
}

const RPM: usize = 60;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let args = Args::parse();

    let mut dictionary = BTreeMap::new();
    let file = File::open(args.dictionary_path)?;
    let reader = BufReader::new(file);

    for (i, result) in reader.lines().enumerate() {
        let line = result?;

        let (key, value) = line
            .split_once('–')
            .wrap_err_with(|| eyre!("Invalid entry at line #{}", i + 1))?;

        dictionary.insert(key.trim().to_lowercase(), value.trim().to_string());
    }

    let mut workbook: Xlsx<_> = open_workbook(args.source_path)?;

    let range = workbook
        .worksheet_range(WORKSHEET)
        .wrap_err(format!("No worksheet named '{}'", WORKSHEET))??;

    let filename = args
        .destination_path
        .to_str()
        .wrap_err("Invalid destination filename")?;

    let workbook = Workbook::new(filename)?;
    let mut worksheet = workbook.add_worksheet(Some(WORKSHEET))?;
    let mut untranslated = BTreeMap::<String, Vec<(u32, u16)>>::new();

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {}", args.api_key).parse()?);

    let client = Client::builder().default_headers(headers).build()?;
    let bar = ProgressBar::new((range.width() * range.height()) as u64);
    let (tx, mut rx) = mpsc::channel(RPM);

    let mut futures = vec![];

    for (row, column, data) in range.cells() {
        let DataType::String(value) = data else {
            bar.inc(1);
            continue;
        };

        let row = row as u32;
        let column = column as u16;
        let value = value.trim();

        if value.is_empty() || row == 0 {
            worksheet.write_string(row, column, value, None)?;
            bar.inc(1);
            continue;
        }

        let key = value.to_lowercase();

        if let Some(value) = dictionary.get(&key) {
            worksheet.write_string(row, column, value, None)?;
            bar.inc(1);
            continue;
        }

        if let Some(cells) = untranslated.get_mut(&key) {
            cells.push((row, column));
            continue;
        }

        untranslated.insert(key.clone(), vec![(row, column)]);

        let mut prompt = String::new();
        let mut translations = String::new();

        for (k, v) in &dictionary {
            if key.contains(k) {
                translations.push_str(k);
                translations.push_str(" – ");
                translations.push_str(v);
                translations.push('\n');
            }
        }

        if !translations.is_empty() {
            prompt.push_str("Considering the following translations:\n");
            prompt.push_str(&translations);
            prompt.push('\n');
        }

        prompt.push_str("Translate this into Romanian:\n");
        prompt.push_str(value);
        prompt.push_str("\n\nRomanian:\n");

        let client = client.clone();
        let tx = tx.clone();

        futures.push(async move {
            let result = translate(prompt, &client).await.map(|v| (key, v));
            tx.send(result).await
        });
    }

    drop(tx);

    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            for _ in 0..RPM {
                let Some(future) = futures.pop() else {
                    return Ok(()) as Result<_>;
                };

                tokio::spawn(future);
            }
        }
    });

    while let Some(result) = rx.recv().await {
        match result {
            Ok((ref key, ref value)) => {
                for (row, column) in untranslated[key].iter().copied() {
                    worksheet.write_string(row, column, value, None)?;
                    bar.inc(1);
                }
            }
            Err(e) => bar.println(format!("{:#}", e)),
        }
    }

    bar.finish_and_clear();

    Ok(())
}
