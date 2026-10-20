//! Week 2: open a 500,000 sat payment channel from Alice to Bob on regtest.
//!
//! Bitcoin Core is driven through its JSON-RPC interface (`bitcoincore-rpc`),
//! and the two Core Lightning nodes are driven through the `clnrest` plugin,
//! authenticated with the runes exported by `test.sh` as `ALICE_RUNE` and
//! `BOB_RUNE`.

use bitcoincore_rpc::bitcoin::{Address, Amount, Network, Txid};
use bitcoincore_rpc::jsonrpc::{self, simple_http::SimpleHttpTransport};
use bitcoincore_rpc::{Client as BitcoinClient, RpcApi};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::error::Error;
use std::fs;
use std::str::FromStr;
use std::thread::sleep;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const BITCOIN_RPC_URL: &str = "http://localhost:18443";
const BITCOIN_RPC_USER: &str = "alice";
const BITCOIN_RPC_PASSWORD: &str = "password";
const MINING_WALLET: &str = "mining_wallet";
/// The client library defaults to 15s, but `createwallet` alone can take
/// longer than that when the Docker volume has slow disk I/O.
const BITCOIN_RPC_TIMEOUT: Duration = Duration::from_secs(120);

/// Bob as seen from inside the Docker network: the container name resolves
/// via Docker DNS and 9735 is his internal P2P port (9736 is only the host mapping).
const BOB_P2P_HOST: &str = "bob";
const BOB_P2P_PORT: u16 = 9735;

const ALICE_FUNDING: Amount = Amount::from_sat(100_000_000); // 1 BTC
const CHANNEL_CAPACITY_SAT: u64 = 500_000;
const CHANNEL_CAPACITY_MSAT: u64 = CHANNEL_CAPACITY_SAT * 1_000;

/// Coinbase outputs can only be spent after 100 confirmations, so mining
/// 101 blocks makes the reward of the first block (50 BTC) spendable.
const COINBASE_MATURITY_BLOCKS: u64 = 101;
/// Conventional "settled" depth. It also covers CLN's default `funding-confirms`
/// of 3 needed before a channel leaves CHANNELD_AWAITING_LOCKIN.
const CONFIRMATION_BLOCKS: u64 = 6;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const POLL_TIMEOUT: Duration = Duration::from_secs(120);

/// The test reads `out.txt` from the repository root; resolve it relative to
/// this crate so the program works regardless of the current directory.
const OUTPUT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../out.txt");

/// A Core Lightning node reached through its clnrest HTTP endpoint.
struct LightningNode {
    name: &'static str,
    base_url: String,
    rune: String,
    http: Client,
}

impl LightningNode {
    /// Build a client for the node at `base_url`, reading its rune from `rune_env_var`.
    fn new(name: &'static str, base_url: &str, rune_env_var: &str) -> Result<Self> {
        let rune = std::env::var(rune_env_var)
            .ok()
            .filter(|rune| !rune.is_empty())
            .ok_or_else(|| format!("environment variable {rune_env_var} is not set or empty"))?;
        Ok(Self {
            name,
            base_url: base_url.to_string(),
            rune,
            http: Client::new(),
        })
    }

    /// Invoke a lightningd RPC method (`POST /v1/<method>`) and return its JSON result.
    fn call(&self, method: &str, params: Value) -> Result<Value> {
        let response = self
            .http
            .post(format!("{}/v1/{}", self.base_url, method))
            .header("Rune", &self.rune)
            .json(&params)
            .send()?;

        let status = response.status();
        let body: Value = response.json()?;
        if !status.is_success() {
            return Err(format!("{} `{}` failed ({}): {}", self.name, method, status, body).into());
        }
        Ok(body)
    }

    fn node_id(&self) -> Result<String> {
        let info = self.call("getinfo", json!({}))?;
        Ok(str_field(&info, "id")?.to_string())
    }

    fn block_height(&self) -> Result<u64> {
        let info = self.call("getinfo", json!({}))?;
        info["blockheight"]
            .as_u64()
            .ok_or_else(|| format!("{}: getinfo has no blockheight", self.name).into())
    }

    /// Peers this node currently has a live connection with.
    fn connected_peers(&self) -> Result<Vec<Value>> {
        let peers = self.call("listpeers", json!({}))?;
        Ok(array_field(&peers, "peers")?
            .iter()
            .filter(|peer| peer["connected"].as_bool() == Some(true))
            .cloned()
            .collect())
    }

    fn is_connected_to(&self, peer_id: &str) -> Result<bool> {
        Ok(self
            .connected_peers()?
            .iter()
            .any(|peer| peer["id"].as_str() == Some(peer_id)))
    }

    /// This node's view of its channel with `peer_id`, if any.
    fn channel_with(&self, peer_id: &str) -> Result<Option<Value>> {
        let channels = self.call("listpeerchannels", json!({ "id": peer_id }))?;
        Ok(array_field(&channels, "channels")?.first().cloned())
    }

    /// Block until lightningd has processed the chain up to `height`.
    fn wait_for_block_height(&self, height: u64) -> Result<()> {
        wait_until(&format!("{} to sync to height {height}", self.name), || {
            Ok(self.block_height()? >= height)
        })
    }
}

/// Poll `condition` until it returns true, failing after `POLL_TIMEOUT`.
fn wait_until(description: &str, mut condition: impl FnMut() -> Result<bool>) -> Result<()> {
    let start = Instant::now();
    while !condition()? {
        if start.elapsed() > POLL_TIMEOUT {
            return Err(format!("timed out waiting for {description}").into());
        }
        sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn str_field<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| format!("missing string field `{key}` in {value}").into())
}

fn array_field<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value[key]
        .as_array()
        .ok_or_else(|| format!("missing array field `{key}` in {value}").into())
}

/// Read a millisatoshi amount. CLN serialises these as integers, but older
/// versions used strings such as "500000000msat", so accept both.
fn msat_field(value: &Value, key: &str) -> Result<u64> {
    match &value[key] {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.trim_end_matches("msat").parse().ok(),
        _ => None,
    }
    .ok_or_else(|| format!("missing msat field `{key}` in {value}").into())
}

/// Return an RPC client bound to `name`, creating or loading the wallet as needed.
fn open_wallet(node: &BitcoinClient, name: &str) -> Result<BitcoinClient> {
    if !node.list_wallets()?.iter().any(|w| w == name) {
        if node.list_wallet_dir()?.iter().any(|w| w == name) {
            node.load_wallet(name)?;
        } else {
            node.create_wallet(name, None, None, None, None)?;
        }
    }
    bitcoin_client(&format!("{BITCOIN_RPC_URL}/wallet/{name}"))
}

/// Build a Bitcoin Core RPC client for `url` with `BITCOIN_RPC_TIMEOUT`.
fn bitcoin_client(url: &str) -> Result<BitcoinClient> {
    let transport = SimpleHttpTransport::builder()
        .url(url)?
        .auth(BITCOIN_RPC_USER, Some(BITCOIN_RPC_PASSWORD))
        .timeout(BITCOIN_RPC_TIMEOUT)
        .build();
    Ok(BitcoinClient::from_jsonrpc(jsonrpc::Client::with_transport(transport)))
}

/// Mine `blocks` blocks to `address` and return the new chain height.
fn mine(node: &BitcoinClient, blocks: u64, address: &Address) -> Result<u64> {
    node.generate_to_address(blocks, address)?;
    Ok(node.get_block_count()?)
}

fn main() -> Result<()> {
    // Bitcoin RPC client (node-level calls; wallet calls use a per-wallet client)
    let bitcoin_rpc = bitcoin_client(BITCOIN_RPC_URL)?;
    let chain_info = bitcoin_rpc.get_blockchain_info()?;
    println!("Bitcoin Core: chain={} blocks={}", chain_info.chain, chain_info.blocks);

    let alice = LightningNode::new("Alice", "http://localhost:3010", "ALICE_RUNE")?;
    let bob = LightningNode::new("Bob", "http://localhost:3011", "BOB_RUNE")?;

    // Get Alice's node ID
    let alice_id = alice.node_id()?;
    println!("Alice node ID: {alice_id}");

    // Get Bob's node ID
    let bob_id = bob.node_id()?;
    println!("Bob node ID: {bob_id}");

    // Connect Alice to Bob as a peer. A node is addressed as id@host:port;
    // the id lets Alice authenticate Bob during the Noise handshake.
    alice.call(
        "connect",
        json!({ "id": bob_id, "host": BOB_P2P_HOST, "port": BOB_P2P_PORT }),
    )?;

    // Verify peer connection from both Alice's and Bob's perspectives
    wait_until("Alice and Bob to see each other as peers", || {
        Ok(alice.is_connected_to(&bob_id)? && bob.is_connected_to(&alice_id)?)
    })?;
    println!("Alice and Bob are connected peers");

    // Create or load a mining wallet
    let mining_wallet = open_wallet(&bitcoin_rpc, MINING_WALLET)?;

    // Generate a new mining address from the mining wallet
    let mining_address = mining_wallet
        .get_new_address(Some("mining"), None)?
        .require_network(Network::Regtest)?;
    println!("Mining address: {mining_address}");

    // How many blocks need to be mined to the mining address? Why?
    // 101: a coinbase output only matures after 100 further blocks, so the
    // 101st block makes the first block's 50 BTC reward spendable. Skip this
    // if the wallet is already funded (e.g. on a re-run against the same chain).
    if mining_wallet.get_balance(None, None)? <= ALICE_FUNDING {
        let height = mine(&bitcoin_rpc, COINBASE_MATURITY_BLOCKS, &mining_address)?;
        println!("Mined {COINBASE_MATURITY_BLOCKS} blocks, height is now {height}");
    }

    // Verify wallet balance
    let mining_balance = mining_wallet.get_balance(None, None)?;
    println!("Mining wallet balance: {mining_balance}");
    if mining_balance <= ALICE_FUNDING {
        return Err(format!("mining wallet balance {mining_balance} cannot fund Alice").into());
    }

    // Create an on-chain address for Alice and send 1 BTC from mining wallet to this address
    let alice_address_json = alice.call("newaddr", json!({}))?;
    let alice_address = Address::from_str(str_field(&alice_address_json, "bech32")?)?
        .require_network(Network::Regtest)?;
    let funding_txid: Txid = mining_wallet.send_to_address(
        &alice_address,
        ALICE_FUNDING,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;
    println!("Sent {ALICE_FUNDING} to Alice at {alice_address} in tx {funding_txid}");

    // Mine blocks to confirm the funding transaction. How many blocks and why?
    // One confirmation is enough for CLN to treat the output as spendable
    // (fundchannel's default minconf is 1); 6 is the conventional depth at which
    // a transaction is considered settled and reorg-safe.
    let height = mine(&bitcoin_rpc, CONFIRMATION_BLOCKS, &mining_address)?;

    // Verify Alice received the funds: lightningd learns about the deposit only
    // after it has scanned the new blocks, so wait for a confirmed output.
    alice.wait_for_block_height(height)?;
    wait_until("Alice's on-chain funds to confirm", || {
        let funds = alice.call("listfunds", json!({}))?;
        Ok(array_field(&funds, "outputs")?.iter().any(|output| {
            output["txid"].as_str() == Some(&funding_txid.to_string())
                && output["status"].as_str() == Some("confirmed")
        }))
    })?;
    println!("Alice received {ALICE_FUNDING} on-chain");

    // Open a payment channel from Alice to Bob with 500,000 satoshis capacity.
    // No `push_msat`, so the whole capacity starts on Alice's side.
    bob.wait_for_block_height(height)?;
    let opened = alice.call(
        "fundchannel",
        json!({ "id": bob_id, "amount": CHANNEL_CAPACITY_SAT }),
    )?;
    let channel_funding_txid = str_field(&opened, "txid")?.to_string();
    println!(
        "Channel {} opened, funding tx {channel_funding_txid}",
        str_field(&opened, "channel_id")?
    );

    // Mine some blocks to confirm the channel opening transaction.
    // CLN needs 3 confirmations (funding-confirms) before channel_ready is
    // exchanged; 6 also makes the channel eligible for gossip announcement.
    let height = mine(&bitcoin_rpc, CONFIRMATION_BLOCKS, &mining_address)?;

    // Wait for nodes to recognize the confirmed channel
    alice.wait_for_block_height(height)?;
    bob.wait_for_block_height(height)?;

    // Verify channel is active on both Alice's side and Bob's side
    let channel_state = |node: &LightningNode, peer_id: &str| -> Result<Option<String>> {
        Ok(node
            .channel_with(peer_id)?
            .and_then(|channel| channel["state"].as_str().map(str::to_string)))
    };
    wait_until("the channel to reach CHANNELD_NORMAL on both sides", || {
        Ok(channel_state(&alice, &bob_id)?.as_deref() == Some("CHANNELD_NORMAL")
            && channel_state(&bob, &alice_id)?.as_deref() == Some("CHANNELD_NORMAL"))
    })?;

    // Get channel details from both Alice's and Bob's perspectives
    let alice_channel = alice
        .channel_with(&bob_id)?
        .ok_or("Alice has no channel with Bob")?;
    let bob_channel = bob
        .channel_with(&alice_id)?
        .ok_or("Bob has no channel with Alice")?;

    let channel_id = str_field(&alice_channel, "channel_id")?.to_string();
    let alice_state = str_field(&alice_channel, "state")?.to_string();
    let bob_state = str_field(&bob_channel, "state")?.to_string();
    let total_msat = msat_field(&alice_channel, "total_msat")?;
    let alice_balance_msat = msat_field(&alice_channel, "to_us_msat")?;
    let bob_balance_msat = msat_field(&bob_channel, "to_us_msat")?;

    // Both sides must agree on which on-chain output funds the channel
    let funding_txid = str_field(&alice_channel, "funding_txid")?.to_string();
    if funding_txid != channel_funding_txid
        || str_field(&bob_channel, "funding_txid")? != funding_txid
    {
        return Err("Alice and Bob disagree on the channel funding transaction".into());
    }
    if total_msat != CHANNEL_CAPACITY_MSAT {
        return Err(format!("unexpected channel capacity {total_msat} msat").into());
    }
    println!(
        "Channel {channel_id}: Alice {alice_state} {alice_balance_msat} msat, \
         Bob {bob_state} {bob_balance_msat} msat"
    );

    // Check if Alice and Bob are peers
    let alice_peer_count = alice.connected_peers()?.len();
    let bob_peer_count = bob.connected_peers()?.len();
    if !alice.is_connected_to(&bob_id)? || !bob.is_connected_to(&alice_id)? {
        return Err("Alice and Bob are no longer connected".into());
    }

    // Write to out.txt: one value per line in the order the autograder parses
    // them (it reads bare values, as in the README's sample output).
    let report = [
        alice_id,
        bob_id,
        alice_peer_count.to_string(),
        bob_peer_count.to_string(),
        channel_id,
        funding_txid,
        alice_state,
        bob_state,
        total_msat.to_string(),
        alice_balance_msat.to_string(),
        bob_balance_msat.to_string(),
    ];
    fs::write(OUTPUT_PATH, report.join("\n") + "\n")?;
    println!("Wrote channel report to out.txt");

    Ok(())
}
