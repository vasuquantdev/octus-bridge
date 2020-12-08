use anyhow::anyhow;
use anyhow::Error;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Password, Select};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use structopt::StructOpt;

fn parse_url(url: &str) -> Result<Url, Error> {
    Ok(Url::parse(url)?)
}

#[derive(StructOpt)]
struct Arguments {
    #[structopt(short, long, parse(try_from_str = parse_url))]
    server_addr: Url,
}

#[derive(Serialize, Debug)]
struct InitData {
    ton_seed: String,
    eth_seed: String,
    password: String,
    language: String,
}

fn provide_ton_seed() -> Result<String, Error> {
    let input: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Provide ton seed words. 12 words are needed.")
        .interact_text()?;
    let words: Vec<String> = input.split(' ').map(|x| x.to_string()).collect();
    if words.len() != 12 {
        return Err(anyhow!("{} words for ton seed are provided", words.len()));
    }
    Ok(words.join(" "))
}

fn provide_eth_seed() -> Result<String, Error> {
    let input: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Provide eth seed words.")
        .interact_text()?;
    let words: Vec<String> = input.split(' ').map(|x| x.to_string()).collect();
    if words.len() < 12 {
        return Err(anyhow!(
            "{} words for eth seed are provided which is not enough for high entropy",
            words.len()
        ));
    }
    Ok(words.join(" "))
}

fn provide_language() -> Result<String, Error> {
    let langs = ["en", "zh-hans", "zh-hant", "fr", "it", "ja", "ko", "es"];
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Choose bip39 mnemonic language for eth")
        .items(&langs)
        .default(0)
        .interact()?;
    Ok(langs[selection].to_string())
}

fn provide_password() -> Result<String, Error> {
    let password = Password::with_theme(&ColorfulTheme::default())
        .with_prompt("Password, longer then 8 symbols")
        .with_confirmation("Repeat password", "Error: the passwords don't match.")
        .interact()?;
    if password.len() < 8 {
        return Err(anyhow!("Password len is {}", password.len()));
    }
    Ok(password)
}

fn unlock_node() -> Result<String, Error> {
    let password = Password::with_theme(&ColorfulTheme::default())
        .with_prompt("Password:")
        .interact()?;
    Ok(password)
}

fn rescan_eth() -> Result<u64, Error> {
    let input: u64 = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Give block number")
        .interact_text()?;
    Ok(input)
}

fn init() -> Result<InitData, Error> {
    let ton_seed = provide_ton_seed()?;
    let language = provide_language()?;
    let eth_seed = provide_eth_seed()?;
    let password = provide_password()?;
    Ok(InitData {
        password,
        eth_seed,
        ton_seed,
        language,
    })
}
#[derive(Serialize)]
struct PasswordData {
    password: String,
}

#[derive(Deserialize, Serialize)]
struct Status {
    init_data_needed: bool,
    is_working: bool,
    password_needed: bool,
}

#[derive(Deserialize, Serialize)]
pub struct RescanEthData {
    pub block: u64,
}

fn main() -> Result<(), Error> {
    let args: Arguments = Arguments::from_args();
    const ACTIONS: &[&str; 4] = &["Init", "Provide password", "Get status", "Set block"];
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("What do you want?")
        .default(0)
        .items(&ACTIONS[..])
        .interact()
        .unwrap();
    let client = reqwest::blocking::Client::new();
    let (url, data) = if selection == 0 {
        let init_data = init()?;
        (args.server_addr.join("init")?, json!(init_data))
    } else if selection == 1 {
        let password = unlock_node()?;
        (
            args.server_addr.join("unlock")?,
            json!(PasswordData { password }),
        )
    } else if selection == 2 {
        let url = args.server_addr.join("status")?;
        let response: Status = client.get(url).send()?.json()?;
        println!("Status:\n {}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    } else if selection == 3 {
        let url = args.server_addr.join("rescan_eth")?;
        let block = rescan_eth()?;
        (url, json!(RescanEthData { block }))
    } else {
        unreachable!()
    };
    dbg!(&data);
    let response = client.post(url).json(&data).send()?;
    if response.status().is_success() {
        println!("Success");
    } else {
        println!("Failed: {}", response.text()?);
    }
    Ok(())
}
