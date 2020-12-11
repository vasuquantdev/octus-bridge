use anyhow::anyhow;
use anyhow::Error;
use dialoguer::theme::{ColorfulTheme, Theme};
use dialoguer::{Input, Password, Select};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Arguments {
    #[structopt(short, long, parse(try_from_str = parse_url))]
    server_addr: Url,
}

fn main() -> Result<(), Error> {
    let args: Arguments = Arguments::from_args();

    let theme = ColorfulTheme::default();

    Prompt::new(&theme, "Select action", args.server_addr)
        .item("Get status", Client::get_status)
        .item("Init", Client::init_bridge)
        .item("Provide password", Client::unlock_bridge)
        .item("Set eth block", Client::set_eth_block)
        .item("Add event configuration", Client::add_event_configuration)
        .item(
            "Vote for event configuration",
            Client::vote_event_configuration,
        )
        .execute()
}

struct Client {
    url: Url,
    client: reqwest::blocking::Client,
}

impl Client {
    pub fn new(url: Url) -> Self {
        let client = reqwest::blocking::Client::new();
        Self { url, client }
    }

    pub fn get_status(&self) -> Result<(), Error> {
        let status: Status = self.get("status")?;

        println!("Status: {}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }

    pub fn init_bridge(&self) -> Result<(), Error> {
        let ton_seed = provide_ton_seed()?;
        let language = provide_language()?;
        let eth_seed = provide_eth_seed()?;
        let password = provide_password()?;

        let _ = self.post_raw(
            "init",
            &InitData {
                password,
                eth_seed,
                ton_seed,
                language,
            },
        )?;

        println!("Success!");
        Ok(())
    }

    pub fn unlock_bridge(&self) -> Result<(), Error> {
        let password = Password::with_theme(&ColorfulTheme::default())
            .with_prompt("Password:")
            .interact()?;

        let _ = self.post_raw("unlock", &PasswordData { password })?;

        println!("Success!");
        Ok(())
    }

    pub fn set_eth_block(&self) -> Result<(), Error> {
        let block: u64 = Input::with_theme(&ColorfulTheme::default())
            .with_prompt("Give block number")
            .interact_text()?;

        let _ = self.post_raw("rescan_eth", &RescanEthData { block })?;

        println!("Success!");
        Ok(())
    }

    pub fn add_event_configuration(&self) -> Result<(), Error> {
        todo!()
    }

    pub fn vote_event_configuration(&self) -> Result<(), Error> {
        let theme = ColorfulTheme::default();

        let known_configs: Vec<EventConfiguration> = self.get("event_configurations")?;

        let mut selection = Select::with_theme(&theme);
        selection.with_prompt("Select config to vote").default(0);

        for (i, config) in known_configs.iter().enumerate() {
            selection.item(format!("Config #{}: {}", i, config));
        }

        let selected_config = selection.interact()?;
        let config_address = known_configs[selected_config].address.clone();

        let selected_vote = Select::with_theme(&theme)
            .with_prompt("Voting for event configuration contract")
            .item("Confirm")
            .item("Reject")
            .interact_opt()?
            .ok_or_else(|| anyhow!("You must confirm or reject selection"))?;

        let voting = match selected_vote {
            0 => Voting::Confirm(config_address),
            1 => Voting::Reject(config_address),
            _ => unreachable!(),
        };

        let _ = self.post_raw("event_configurations/vote", &voting)?;

        println!("Success!");
        Ok(())
    }

    fn get<T>(&self, url: &str) -> Result<T, Error>
    where
        for<'de> T: Deserialize<'de>,
    {
        let url = self.url.join(url)?;
        let response = self.client.get(url).send()?.prepare()?;
        Ok(response.json()?)
    }

    fn post_json<T, B>(&self, url: &str, body: &B) -> Result<T, Error>
    where
        for<'de> T: Deserialize<'de>,
        B: Serialize,
    {
        let url = self.url.join(url)?;
        let response = self.client.post(url).json(body).send()?.prepare()?;
        Ok(response.json()?)
    }

    fn post_raw<B>(&self, url: &str, body: &B) -> Result<String, Error>
    where
        B: Serialize,
    {
        let url = self.url.join(url)?;
        let response = self.client.post(url).json(body).send()?.prepare()?;
        Ok(response.text()?)
    }
}

trait ResponseExt: Sized {
    fn prepare(self) -> Result<Self, Error>;
}

impl ResponseExt for reqwest::blocking::Response {
    fn prepare(self) -> Result<Self, Error> {
        if self.status().is_success() {
            Ok(self)
        } else {
            Err(anyhow!(
                "{}: {}",
                self.status().canonical_reason().unwrap_or("Unknown error"),
                self.text()?
            ))
        }
    }
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

#[derive(Serialize, Debug)]
struct InitData {
    ton_seed: String,
    eth_seed: String,
    password: String,
    language: String,
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "vote", content = "address")]
pub enum Voting {
    Confirm(String),
    Reject(String),
}

#[derive(Deserialize, Serialize)]
pub struct EventConfiguration {
    pub address: String,

    pub ethereum_event_abi: String,
    pub ethereum_event_address: String,
    pub ethereum_address: String,
    pub event_proxy_address: String,
    pub ethereum_event_blocks_to_confirm: u64,
    pub required_confirmations: u64,
    pub required_rejections: u64,

    pub confirm_keys: Vec<String>,
    pub reject_keys: Vec<String>,
    pub active: bool,
}

impl std::fmt::Display for EventConfiguration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let addr_len = self.address.len();

        f.write_fmt(format_args!(
            "ETH 0x{} -> TON {}...{}, confirms {}/{}, rejects {}/{}",
            self.ethereum_event_address,
            &self.address[0..6],
            &self.address[addr_len - 4..addr_len],
            self.confirm_keys.len(),
            self.required_confirmations,
            self.reject_keys.len(),
            self.required_rejections
        ))
    }
}

fn parse_url(url: &str) -> Result<Url, Error> {
    Ok(Url::parse(url)?)
}

struct Prompt<'a> {
    client: Client,
    select: Select<'a>,
    items: Vec<Box<dyn FnMut(&Client) -> Result<(), Error>>>,
}

impl<'a> Prompt<'a> {
    pub fn new(theme: &'a dyn Theme, title: &str, url: Url) -> Self {
        let client = Client::new(url);
        let mut select = Select::with_theme(theme);
        select.with_prompt(title).default(0);

        Self {
            client,
            select,
            items: Vec::new(),
        }
    }

    pub fn item<F>(&mut self, name: &'static str, f: F) -> &mut Self
    where
        F: FnMut(&Client) -> Result<(), Error> + 'static,
    {
        self.select.item(name);
        self.items.push(Box::new(f));
        self
    }

    pub fn execute(&mut self) -> Result<(), Error> {
        let selection = self.select.interact()?;
        self.items[selection](&self.client)
    }
}

trait PrepareRequest {
    fn prepare(self) -> Result<(Url, serde_json::Value), Error>;
}

impl<T> PrepareRequest for Result<(Url, T), Error>
where
    T: Serialize,
{
    fn prepare(self) -> Result<(Url, serde_json::Value), Error> {
        self.map(|(url, value)| (url, json!(value)))
    }
}
