use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::Error;
use clap::Clap;
use colored_json::{to_colored_json_auto, ColorMode, ToColoredJson};
use dialoguer::theme::{ColorfulTheme, Theme};
use dialoguer::{Input, Password, Select};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;

use relay_models::models::{
    EthTonTransactionView, EventConfiguration, InitData, Password as PasswordData, RescanEthData,
    Status, TxStatView, Voting,
};

#[derive(Clap)]
struct Arguments {
    #[clap(short, long, parse(try_from_str = parse_url))]
    server_addr: Url,
}

fn main() -> Result<(), Error> {
    let args = Arguments::parse();

    let theme = ColorfulTheme::default();

    Prompt::new(&theme, "Select action", args.server_addr)
        .item("Get status", Client::get_status)
        .item("Init", Client::init_bridge)
        .item("Provide password", Client::unlock_bridge)
        .item("Set eth block", Client::set_eth_block)
        .item("Retry failed votes", Client::retry_failed_votes)
        .item(
            "Vote for ETH event configuration",
            Client::vote_for_ethereum_event_configuration,
        )
        .item("Get pending transactions", Client::get_pending_transactions)
        .item("Get failed transactions", Client::get_failed_transactions)
        .item("Get all confirmed transactions", Client::get_stats)
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

        println!(
            "Status: {}",
            serde_json::to_string_pretty(&status)?.to_colored_json_auto()?
        );
        Ok(())
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

    pub fn retry_failed_votes(&self) -> Result<(), Error> {
        self.post_raw("status/failed/retry", &())?;
        println!("Success!");
        Ok(())
    }

    pub fn get_pending_transactions(&self) -> Result<(), Error> {
        let response: Vec<EthTonTransactionView> = self.get("status/pending")?;
        pager::Pager::new().setup();
        println!(
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        );
        Ok(())
    }

    pub fn get_failed_transactions(&self) -> Result<(), Error> {
        let response: Vec<EthTonTransactionView> = self.get("status/failed")?;
        pager::Pager::new().setup();
        println!(
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        );
        Ok(())
    }

    pub fn get_stats(&self) -> Result<(), Error> {
        let our_key = self
            .get::<Status>("status")?
            .ton_pubkey
            .ok_or_else(|| anyhow!("Relay is locked or not initialized"))?;
        let theme = ColorfulTheme::default();
        let mut selection = Select::with_theme(&theme);
        selection
            .with_prompt(format!("Select relay key. Our key is: {}", our_key))
            .default(0);
        let response: HashMap<String, Vec<TxStatView>> = self.get("status/relay")?;
        let keys: Vec<_> = response.keys().cloned().collect();
        selection.items(&keys);
        let selection = &keys[selection.interact()?];
        pager::Pager::new().setup();
        println!(
            "{}",
            serde_json::to_string_pretty(&response[selection])?.to_colored_json(ColorMode::On)?
        );
        Ok(())
    }

    pub fn vote_for_ethereum_event_configuration(&self) -> Result<(), Error> {
        let theme = ColorfulTheme::default();

        let known_configs: Vec<EventConfiguration> = self.get("event_configurations")?;

        let mut selection = Select::with_theme(&theme);
        selection.with_prompt("Select config to vote").default(0);

        for (i, config) in known_configs.iter().enumerate() {
            selection.item(format!(
                "Config #{}: {}",
                i,
                EventConfigurationWrapper(&config)
            ));
        }

        let selected_config = &known_configs[selection.interact()?];

        println!(
            "Config:\n{}\n",
            serde_json::to_string_pretty(&selected_config)?.to_colored_json_auto()?
        );

        let config_address = selected_config.address.clone();

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

    #[allow(dead_code)]
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

#[derive(Serialize, Deserialize)]
pub struct VotingAddress {
    pub address: String,
}

struct EventConfigurationWrapper<'a>(&'a EventConfiguration);

impl<'a> std::fmt::Display for EventConfigurationWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let config = self.0;

        let eth_addr_len = config.ethereum_event_address.len();
        let ton_addr_len = config.address.len();

        f.write_fmt(format_args!(
            "ETH 0x{}...{} -> TON {}...{}",
            &config.ethereum_event_address[0..4],
            &config.ethereum_event_address[eth_addr_len - 4..eth_addr_len],
            &config.address[0..6],
            &config.address[ton_addr_len - 4..ton_addr_len],
        ))
    }
}

fn parse_url(url: &str) -> Result<Url, Error> {
    Ok(Url::parse(url)?)
}

type CommandHandler = Box<dyn FnMut(&Client) -> Result<(), Error>>;

struct Prompt<'a> {
    client: Client,
    select: Select<'a>,
    items: Vec<CommandHandler>,
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
