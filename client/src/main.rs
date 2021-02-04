use std::collections::HashMap;
use std::fmt::Write;

use anyhow::anyhow;
use anyhow::Error;
use clap::Clap;
use colored_json::{ColorMode, ToColoredJson};
use dialoguer::theme::{ColorfulTheme, Theme};
use dialoguer::{Input, Password, Select};
use minus::Pager;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;

use relay_models::models::{
    BridgeConfigurationView, EthEventVoteDataView, EthTonTransactionView, EthTxStatView,
    EventConfiguration, EventConfigurationType, InitData, NewEventConfiguration,
    Password as PasswordData, RescanEthData, Status, TonEthTransactionView, TonEventVoteDataView,
    TonTxStatView, Voting,
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
        .item("Set ETH block", Client::set_eth_block)
        .item("Retry failed votes", Client::retry_failed_votes)
        .item(
            "Add new event configuration",
            Client::add_new_event_configuration,
        )
        .item(
            "Vote for event configuration",
            Client::vote_for_event_configuration,
        )
        .item("Get event configurations", Client::get_event_configurations)
        .item(
            "Get pending transactions ETH->TON",
            Client::get_pending_transactions_eth_to_ton,
        )
        .item(
            "Get failed transactions ETH->TON",
            Client::get_failed_transactions_eth_to_ton,
        )
        .item(
            "Get queued transactions ETH->TON",
            Client::get_queued_transactions_eth_to_ton,
        )
        .item(
            "Get all confirmed transactions from ETH",
            Client::get_eth_stats,
        )
        .item(
            "Get pending transactions TON->ETH",
            Client::get_pending_transactions_ton_to_eth,
        )
        .item(
            "Get failed transactions TON->ETH",
            Client::get_failed_transactions_ton_to_eth,
        )
        .item(
            "Get queued transactions TON->ETH",
            Client::get_queued_transactions_ton_to_eth,
        )
        .item(
            "Get all confirmed transactions from TON",
            Client::get_ton_stats,
        )
        .item(
            "Update bridge configuration",
            Client::update_bridge_configuration,
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

        println!(
            "Status: {}",
            serde_json::to_string_pretty(&status)?.to_colored_json_auto()?
        );
        Ok(())
    }

    pub fn init_bridge(&self) -> Result<(), Error> {
        let language = provide_language()?;
        let ton_seed = provide_ton_seed()?;
        let ton_derivation_path = provide_ton_derivation_path()?;
        let eth_seed = provide_eth_seed()?;
        let eth_derivation_path = provide_eth_derivation_path()?;
        let password = provide_password()?;
        let _ = self.post_raw(
            "init",
            &InitData {
                password,
                language,
                eth_seed,
                ton_seed,
                ton_derivation_path: Some(ton_derivation_path),
                eth_derivation_path: Some(eth_derivation_path),
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

        let _ = self.post_raw("rescan-eth", &RescanEthData { block })?;

        println!("Success!");
        Ok(())
    }

    pub fn retry_failed_votes(&self) -> Result<(), Error> {
        self.post_raw("retry-failed", &())?;
        println!("Success!");
        Ok(())
    }

    pub fn get_pending_transactions_eth_to_ton(&self) -> Result<(), Error> {
        let response: Vec<EthTonTransactionView> = self.get("eth-to-ton/pending")?;
        let mut output = Pager::new().set_prompt("Pending transactions");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn get_pending_transactions_ton_to_eth(&self) -> Result<(), Error> {
        let response: Vec<TonEthTransactionView> = self.get("ton-to-eth/pending")?;
        let mut output = Pager::new().set_prompt("Pending transactions");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn get_failed_transactions_eth_to_ton(&self) -> Result<(), Error> {
        let response: Vec<EthTonTransactionView> = self.get("eth-to-ton/failed")?;
        let mut output = Pager::new().set_prompt("Failed transactions");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn get_failed_transactions_ton_to_eth(&self) -> Result<(), Error> {
        let response: Vec<TonEthTransactionView> = self.get("ton-to-eth/failed")?;
        let mut output = Pager::new().set_prompt("Failed transactions");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn get_queued_transactions_eth_to_ton(&self) -> Result<(), Error> {
        let response: HashMap<u64, Vec<EthEventVoteDataView>> = self.get("eth-to-ton/queued")?;
        let mut output = Pager::new().set_prompt("Queued transactions");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn get_queued_transactions_ton_to_eth(&self) -> Result<(), Error> {
        let configuration_id: u32 = Input::with_theme(&ColorfulTheme::default())
            .with_prompt("Enter configuration id:")
            .interact()?;

        let response: HashMap<u64, Vec<TonEventVoteDataView>> =
            self.get(&format!("ton-to-eth/queued/{}", configuration_id))?;
        let mut output = Pager::new().set_prompt("Queued transactions");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response)?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn get_eth_stats(&self) -> Result<(), Error> {
        self.get_stats::<EthTxStatView>("eth-to-ton/stats")
    }

    pub fn get_ton_stats(&self) -> Result<(), Error> {
        self.get_stats::<TonTxStatView>("ton-to-eth/stats")
    }

    fn get_stats<T>(&self, url: &str) -> Result<(), Error>
    where
        for<'a> T: Deserialize<'a> + Serialize,
    {
        let our_key = self
            .get::<Status>("status")?
            .ton_relay_address
            .ok_or_else(|| anyhow!("Relay is locked or not initialized"))?;
        let theme = ColorfulTheme::default();
        let mut selection = Select::with_theme(&theme);
        selection
            .with_prompt(format!("Select relay key. Our key is: {}", our_key))
            .default(0);
        let response: HashMap<String, Vec<T>> = self.get(url)?;
        let keys: Vec<_> = response.keys().cloned().collect();
        selection.items(&keys);
        let selection = &keys[selection.interact()?];

        let mut output = Pager::new().set_prompt("Stats");
        writeln!(
            output.lines,
            "{}",
            serde_json::to_string_pretty(&response[selection])?.to_colored_json(ColorMode::On)?
        )?;
        minus::page_all(output)?;
        Ok(())
    }

    pub fn add_new_event_configuration(&self) -> Result<(), Error> {
        let theme = ColorfulTheme::default();

        let configuration_id: u32 = Input::with_theme(&theme)
            .with_prompt("Enter configuration id:")
            .interact()?;

        let address: String = Input::with_theme(&theme)
            .with_prompt("Enter contract address:")
            .interact()?;

        let types = [EventConfigurationType::Eth, EventConfigurationType::Ton];
        let contract_type = Select::with_theme(&theme)
            .with_prompt("Select configuration type:")
            .item("ETH event configuration")
            .item("TON event configuration")
            .default(0)
            .interact()?;

        let _ = self.post_raw(
            "event-configurations",
            &NewEventConfiguration {
                configuration_id,
                address,
                configuration_type: types[contract_type],
            },
        )?;

        println!("Success!");
        Ok(())
    }

    pub fn update_bridge_configuration(&self) -> Result<(), Error> {
        let bridge_configuration_view = update_bridge_configuration()?;
        self.post_json("update-bridge-configuration", &bridge_configuration_view)?;
        Ok(())
    }

    pub fn get_event_configurations(&self) -> Result<(), Error> {
        let theme = ColorfulTheme::default();

        let known_configs: Vec<EventConfiguration> = self.get("event-configurations")?;
        if known_configs.is_empty() {
            println!("There are no active configs");
            return Ok(());
        }

        let mut selection = Select::with_theme(&theme);
        selection.with_prompt("Select config to vote").default(0);

        for config in known_configs.iter() {
            selection.item(format!("Config {}", EventConfigurationWrapper(&config)));
        }

        let selected_config = &known_configs[selection.interact()?];

        println!(
            "Config:\n{}\n",
            serde_json::to_string_pretty(&selected_config)?.to_colored_json_auto()?
        );
        Ok(())
    }

    pub fn vote_for_event_configuration(&self) -> Result<(), Error> {
        let theme = ColorfulTheme::default();

        let configuration_id: u32 = Input::with_theme(&theme)
            .with_prompt("Enter configuration id:")
            .interact()?;

        let selected_vote = Select::with_theme(&theme)
            .with_prompt("Select vote")
            .item("Confirm")
            .item("Reject")
            .interact_opt()?
            .ok_or_else(|| anyhow!("You must confirm or reject selection"))?;

        let voting = match selected_vote {
            0 => Voting::Confirm(configuration_id),
            1 => Voting::Reject(configuration_id),
            _ => unreachable!(),
        };

        let _ = self.post_raw("event-configurations/vote", &voting)?;

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

fn update_bridge_configuration() -> Result<BridgeConfigurationView, Error> {
    let nonce: u16 = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Enter nonce")
        .interact_text()?;

    let bridge_update_required_confirmations: u16 = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Enter nonce")
        .interact_text()?;

    let bridge_update_required_rejections: u16 = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Enter nonce")
        .interact_text()?;
    Ok(BridgeConfigurationView {
        nonce,
        bridge_update_required_confirmations,
        bridge_update_required_rejections,
        active: true,
    })
}

fn provide_language() -> Result<String, Error> {
    let langs = ["en", "zh-hans", "zh-hant", "fr", "it", "ja", "ko", "es"];
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Choose bip39 mnemonic language")
        .items(&langs)
        .default(0)
        .interact()?;
    Ok(langs[selection].to_string())
}

fn provide_ton_seed() -> Result<String, Error> {
    let input: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Provide TON seed words. 12 words are needed.")
        .interact_text()?;
    let words: Vec<String> = input.split(' ').map(|x| x.to_string()).collect();
    if words.len() < 12 {
        return Err(anyhow!("{} words for TON seed are provided", words.len()));
    }
    Ok(words.join(" "))
}

fn provide_eth_seed() -> Result<String, Error> {
    let input: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Provide ETH seed words.")
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

fn provide_eth_derivation_path() -> Result<String, Error> {
    let derivation_path = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Provide derivation path for ETH")
        .with_initial_text("m/44'/60'/0'/0/0".to_string())
        .interact()?;

    Ok(derivation_path)
}

fn provide_ton_derivation_path() -> Result<String, Error> {
    let derivation_path = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Provide derivation path for TON")
        .with_initial_text("m/44'/396'/0'/0/0".to_string())
        .interact()?;
    Ok(derivation_path)
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

        f.write_fmt(format_args!(
            "{}: ETH 0x{}...{} -> TON",
            &config.configuration_id,
            &config.ethereum_event_address[0..4],
            &config.ethereum_event_address[eth_addr_len - 4..eth_addr_len]
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
