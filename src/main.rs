use rcon::Connection;
use serde::Deserialize;
use serenity::async_trait;
use serenity::model::prelude::*;
use serenity::prelude::*;
use std::fmt::Debug;
use std::str::FromStr;
use tokio::sync::{mpsc, oneshot};
use tokio::net::TcpStream;
use std::sync::Arc;
use rand::Rng;

struct Handler {
    prefix: String,
    connector: Arc<dyn Connector + Send + Sync>,
    command: Option<String>,
    rcon: Arc<RconManager>,
    role: Option<RoleId>,
    channel: Option<ChannelId>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, message: Message) {
        if message.author.bot {
            return;
        }
        let permission = true;
        let permission = permission
            && if let Some(channel) = self.channel {
                message.channel_id == channel
            } else {
                true
            };
        let permission = permission
            && if let Some(guild) = message.guild_id {
                if let Some(expected_role) = self.role {
                    message
                        .author
                        .has_role(&ctx.http, guild, expected_role)
                        .await
                        .unwrap_or(false)
                } else {
                    true
                }
            } else {
                false
            };
        if !permission {
            return;
        };
        if let Some(command) = self
            .command
            .as_ref()
            .and_then(|pat| message.content.strip_prefix(pat))
        {
            let command = command.trim();
            let mut iterator = command.split_whitespace();
            match iterator.next() {
                Some("reconnect") => {
                    print_err(send_long_message(&ctx.http, message.channel_id, "reconnecting ...".to_string()).await);
                    // signal background task to reconnect immediately
                    self.rcon.request_reconnect().await;
                    print_err(send_long_message(&ctx.http, message.channel_id, "reconnect requested; check logs.".to_string()).await);
                }
                _ => {
                    print_err(
                        send_long_message(
                            &ctx.http,
                            message.channel_id,
                            format!("unknown command: {}", command)
                        ).await
                    );
                }
            }
        } else if let Some(command) = message.content.strip_prefix(&self.prefix) {
            let command = command.trim();
            println!("run command: {}", command);

            match self.rcon.run_command(command).await {
                Ok(res) => {
                    println!("response: {}", res);
                    print_err(
                        send_long_message(
                            &ctx.http,
                            message.channel_id,
                            format!("response from server: \n{}", res)
                        ).await
                    );
                }
                Err(err) => {
                    println!("error    : {}", err);
                    print_err(
                        send_long_message(
                            &ctx.http,
                            message.channel_id,
                            format!("error during sending message: \n{}", err)
                        ).await
                    );
                }
            }
        }
    }

    async fn ready(&self, _ctx: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

fn print_err<T, R: Debug>(result: Result<T, R>) -> () {
    if let Some(err) = result.err() {
        println!("error: {:?}", err)
    }
}

/// Split a message into chunks that fit within Discord's 2000 character limit
fn split_message(message: &str, max_length: usize) -> Vec<String> {
    if message.len() <= max_length {
        return vec![message.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current_chunk = String::new();

    for line in message.lines() {
        // If a single line is longer than max_length, split it by characters
        if line.len() > max_length {
            // Flush current chunk if not empty
            if !current_chunk.is_empty() {
                chunks.push(current_chunk.clone());
                current_chunk.clear();
            }

            // Split the long line into character chunks
            let mut chars_remaining = line;
            while !chars_remaining.is_empty() {
                let split_at = chars_remaining.char_indices()
                    .nth(max_length)
                    .map(|(idx, _)| idx)
                    .unwrap_or(chars_remaining.len());
                chunks.push(chars_remaining[..split_at].to_string());
                chars_remaining = &chars_remaining[split_at..];
            }
            continue;
        }

        // Check if adding this line would exceed the limit
        let line_with_newline = if current_chunk.is_empty() {
            line
        } else {
            // Account for the newline character
            if current_chunk.len() + line.len() + 1 > max_length {
                chunks.push(current_chunk.clone());
                current_chunk.clear();
                line
            } else {
                current_chunk.push('\n');
                line
            }
        };

        current_chunk.push_str(line_with_newline);
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    // Ensure we have at least one chunk
    if chunks.is_empty() {
        chunks.push(String::new());
    }

    chunks
}

/// Send a message to Discord with automatic rate limit handling
async fn send_discord_message(
    http: &serenity::http::Http,
    channel_id: ChannelId,
    content: String,
) -> serenity::Result<Message> {
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 5;

    loop {
        match channel_id.say(http, &content).await {
            Ok(msg) => return Ok(msg),
            Err(serenity::Error::Http(http_err)) => {
                // Check if it's a rate limit error (429)
                if let serenity::http::HttpError::UnsuccessfulRequest(response) = http_err.as_ref() {
                    if response.status_code == serenity::http::StatusCode::TOO_MANY_REQUESTS {
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            println!("max retries exceeded for rate limit");
                            return Err(serenity::Error::Http(http_err));
                        }

                        // Use exponential backoff: 1s, 2s, 4s, 8s, 16s
                        let retry_after_ms = 1000u64 * (1 << retry_count.min(4));

                        println!("rate limited (429), retrying after {} ms (attempt {}/{})",
                                 retry_after_ms, retry_count, MAX_RETRIES);
                        tokio::time::sleep(std::time::Duration::from_millis(retry_after_ms)).await;
                        continue;
                    }
                }
                return Err(serenity::Error::Http(http_err));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Send a potentially long message, splitting it if necessary and handling rate limits
async fn send_long_message(
    http: &serenity::http::Http,
    channel_id: ChannelId,
    content: String,
) -> serenity::Result<()> {
    const DISCORD_MAX_LENGTH: usize = 2000;
    let chunks = split_message(&content, DISCORD_MAX_LENGTH);

    for (i, chunk) in chunks.iter().enumerate() {
        if chunks.len() > 1 {
            println!("sending message part {}/{}", i + 1, chunks.len());
        }
        send_discord_message(http, channel_id, chunk.clone()).await?;

        // Add a small delay between chunks to avoid rate limits
        if i < chunks.len() - 1 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // Discord Bot Token を設定
    let options = read_options().await;

    let connector = SimpleConnector {
        server_kind: options.server_kind,
        address: options.rcon.address.clone(),
        pass: options.rcon.pass.clone(),
    };

    let connector_arc: Arc<dyn Connector + Send + Sync> = Arc::new(connector);

    let rcon = Arc::new(RconManager::new(connector_arc.clone(), options.backoff.clone()).await.expect("failed to create manager"));

    let handler = Handler {
        prefix: options.prefix,
        command: options.command,
        role: options.role.map(RoleId),
        channel: options.channel.map(ChannelId),
        // keep connector for manual low-level reconnects if needed
        connector: connector_arc.clone(),
        rcon: rcon.clone(),
    };

    let mut client = Client::builder(&options.token)
        .event_handler(handler)
        .await
        .expect("Err creating client");

    if let Err(why) = client.start().await {
        println!("Client error: {:?}", why);
    } else {
        println!("Client started");
    }
}

async fn read_options() -> Options {
    let expect_env = match read_options_from_env() {
        Ok(options) => return options,
        Err(expect_env) => expect_env,
    };
    let err_msg = match read_options_from_file().await {
        Ok(options) => return options,
        Err(err_msg) => err_msg,
    };
    let mut args = std::env::args();
    args.next(); // skip exec name
    let prefer_env = args.next().map(|x| &x == "--env").unwrap_or(false);
    if prefer_env {
        panic!("required envionemnt variable {} not found", expect_env);
    } else {
        panic!("{}", err_msg);
    }
}

fn read_options_from_env() -> Result<Options, &'static str> {
    use std::env;

    // helper to parse optional u64 / u32 / f64 from env and fallback to default
    let default_backoff = RconBackoffConfig::default();

    let base_delay_ms = env::var("RCON_BASE_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_backoff.base_delay_ms);

    let max_delay_ms = env::var("RCON_MAX_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_backoff.max_delay_ms);

    let max_retries = match env::var("RCON_MAX_RETRIES") {
        Ok(s) => {
            let sl = s.to_lowercase();
            if sl == "none" || sl == "" {
                None
            } else {
                s.parse::<u32>().ok()
            }
        }
        Err(_) => default_backoff.max_retries,
    };

    let jitter_fraction = env::var("RCON_JITTER_FRACTION")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(default_backoff.jitter_fraction);

    Ok(Options {
        token: env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN")?,
        prefix: env::var("DISCORD_PREFIX").map_err(|_| "DISCORD_PREFIX")?,
        command: env::var("DISCORD_COMMAND").ok(),
        role: env::var("DISCORD_ROLE")
            .ok()
            .map(|x| x.parse::<u64>().expect("parsing DISCORD_ROLE")),
        channel: env::var("DISCORD_CHANNEL")
            .ok()
            .map(|x| x.parse::<u64>().expect("parsing DISCORD_CHANNEL")),
        server_kind: env::var("SERVER_KIND")
            .ok()
            .map(|x| x.parse::<ServerKind>().expect("parsing SERVER_KIND"))
            .unwrap_or_else(Default::default),
        rcon: RconOptions {
            address: env::var("RCON_ADDRESS").map_err(|_| "RCON_ADDRESS")?,
            pass: env::var("RCON_PASSWORD")
                .ok()
                .unwrap_or_else(Default::default),
        },
        backoff: RconBackoffConfig {
            base_delay_ms,
            max_delay_ms,
            max_retries,
            jitter_fraction,
        },
    })
}

async fn read_options_from_file() -> Result<Options, &'static str> {
    let config = tokio::fs::read_to_string("config.toml")
        .await
        .map_err(|_| "failed to read config.toml")?;
    Ok(toml::from_str(&config).expect("failed to read config.toml"))
}

#[derive(Deserialize)]
struct Options {
    token: String,
    prefix: String,
    command: Option<String>,
    role: Option<u64>,
    channel: Option<u64>,
    #[serde(default)]
    server_kind: ServerKind,
    rcon: RconOptions,
    #[serde(default)]
    backoff: RconBackoffConfig,
}

#[derive(Deserialize)]
struct RconOptions {
    address: String,
    #[serde(default)]
    pass: String,
}

#[derive(Deserialize, Clone, Copy)]
enum ServerKind {
    #[serde(rename = "normal")]
    Normal,
    #[serde(rename = "minecraft")]
    Minecraft,
    #[serde(rename = "factorio")]
    Factorio,
}

impl Default for ServerKind {
    fn default() -> Self {
        ServerKind::Normal
    }
}

#[derive(Debug)]
struct ParseServerKindErr(());

impl std::fmt::Display for ParseServerKindErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "value is invalid. oneof `normal`, `minecraft`, or `factorio` is required."
        )
    }
}

impl std::error::Error for ParseServerKindErr {}

impl FromStr for ServerKind {
    type Err = ParseServerKindErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "normal" => Ok(ServerKind::Normal),
            "minecraft" => Ok(ServerKind::Minecraft),
            "factorio" => Ok(ServerKind::Factorio),
            _ => Err(ParseServerKindErr(())),
        }
    }
}

struct SimpleConnector {
    server_kind: ServerKind,
    address: String,
    pass: String,
}

#[async_trait]
impl Connector for SimpleConnector {
    async fn connect(&self) -> rcon::Result<Connection<TcpStream>> {
        let mut builder = <Connection<TcpStream>>::builder();
        match self.server_kind {
            ServerKind::Normal => {}
            ServerKind::Minecraft => builder = builder.enable_minecraft_quirks(true),
            ServerKind::Factorio => builder = builder.enable_factorio_quirks(true),
        }
        builder.connect(&self.address, &self.pass).await
    }
}

#[async_trait]
trait Connector {
    async fn connect(&self) -> rcon::Result<Connection<TcpStream>>;
}

// New: backoff config and manager

#[derive(Clone, Deserialize)]
struct RconBackoffConfig {
    base_delay_ms: u64,
    max_delay_ms: u64,
    max_retries: Option<u32>,
    jitter_fraction: f64,
}

impl Default for RconBackoffConfig {
    fn default() -> Self {
        RconBackoffConfig {
            base_delay_ms: 500,
            max_delay_ms: 30_000,
            max_retries: None,
            jitter_fraction: 0.3,
        }
    }
}

struct RconManager {
    request_tx: mpsc::Sender<ManagerRequest>,
}

enum ManagerRequest {
    Command { cmd: String, resp: oneshot::Sender<Result<String, String>> },
    Reconnect,
}

impl RconManager {
    async fn new(connector: Arc<dyn Connector + Send + Sync>, backoff: RconBackoffConfig) -> Result<Self, String> {
        // create channel and spawn manager task that owns Connection
        let (tx, mut rx) = mpsc::channel::<ManagerRequest>(32);

        // attempt initial connect inside the task
        let connector_task = connector.clone();

        // Spawn a dedicated OS thread running a current-thread Tokio runtime so the manager
        // can own non-Send connection types.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build runtime for rcon manager");

            rt.block_on(async move {
                // owned connection inside the task
                let mut conn: Option<Connection<TcpStream>> = match connector_task.connect().await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        println!("initial connect failed: {}", e);
                        None
                    }
                };

                // state for backoff
                let mut backoff_cfg = backoff.clone();

                // helper to perform reconnect with backoff
                async fn do_reconnect(connector: &Arc<dyn Connector + Send + Sync>, backoff: &RconBackoffConfig) -> Option<Connection<TcpStream>> {
                    let mut attempt: u32 = 0;
                    loop {
                        if let Some(max) = backoff.max_retries {
                            if attempt >= max {
                                println!("max retries reached ({}) - giving up", max);
                                return None;
                            }
                        }
                        let pow = 2u64.saturating_pow(attempt);
                        let mut backoff_ms = backoff.base_delay_ms.saturating_mul(pow);
                        if backoff_ms > backoff.max_delay_ms {
                            backoff_ms = backoff.max_delay_ms;
                        }
                        let mut rng = rand::thread_rng();
                        let jitter_range = (backoff_ms as f64 * backoff.jitter_fraction) as i64;
                        let jitter = if jitter_range > 0 { rng.gen_range(-(jitter_range)..=(jitter_range)) as i64 } else { 0 };
                        let sleep_ms = if jitter < 0 { backoff_ms.saturating_sub((-jitter) as u64) } else { backoff_ms.saturating_add(jitter as u64) };
                        println!("reconnect attempt {} - sleeping {} ms", attempt + 1, sleep_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                        match connector.connect().await {
                            Ok(c) => return Some(c),
                            Err(e) => {
                                println!("reconnect failed: {}", e);
                                attempt = attempt.saturating_add(1);
                                continue;
                            }
                        }
                    }
                }

                while let Some(req) = rx.recv().await {
                    match req {
                        ManagerRequest::Command { cmd, resp } => {
                            if let Some(ref mut c) = conn {
                                match c.cmd(&cmd).await {
                                    Ok(res) => {
                                        let _ = resp.send(Ok(res));
                                    }
                                    Err(e) => {
                                        println!("command failed: {}", e);
                                        // drop connection and try to reconnect
                                        conn = None;
                                        // attempt reconnect
                                        conn = do_reconnect(&connector_task, &backoff_cfg).await;
                                        let _ = resp.send(Err(format!("rcon error: {}", e)));
                                    }
                                }
                            } else {
                                // no connection - attempt reconnect then return error
                                conn = do_reconnect(&connector_task, &backoff_cfg).await;
                                let _ = resp.send(Err("not connected to rcon server".into()));
                            }
                        }
                        ManagerRequest::Reconnect => {
                            // trigger reconnect
                            println!("manual reconnect requested");
                            conn = do_reconnect(&connector_task, &backoff_cfg).await;
                        }
                    }
                }

                println!("manager task exiting");
            })
        });

        Ok(RconManager { request_tx: tx })
    }

    async fn request_reconnect(&self) {
        let _ = self.request_tx.send(ManagerRequest::Reconnect).await;
    }

    async fn run_command(&self, cmd: &str) -> Result<String, String> {
        let (tx, rx) = oneshot::channel();
        let req = ManagerRequest::Command { cmd: cmd.to_string(), resp: tx };
        self.request_tx.send(req).await.map_err(|e| format!("manager task stopped: {}", e))?;
        rx.await.map_err(|e| format!("response channel closed: {}", e))?
    }
}
