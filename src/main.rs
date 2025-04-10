use rcon::Connection;
use serde::Deserialize;
use serenity::async_trait;
use serenity::model::prelude::*;
use serenity::prelude::*;
use std::fmt::Debug;
use std::ops::DerefMut;
use std::str::FromStr;
use tokio::sync::Mutex;
use tokio::net::TcpStream;

struct Handler {
    prefix: String,
    connector: Box<dyn Connector + Send + Sync>,
    command: Option<String>,
    connection: Mutex<Connection<TcpStream>>,
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
                    print_err(message.channel_id.say(&ctx.http, "reconnecting ...").await);
                    match self.connector.connect().await {
                        Ok(connection) => {
                            *self.connection.lock().await.deref_mut() = connection;
                            print_err(message.channel_id.say(&ctx.http, "reconnected").await);
                        }
                        Err(err) => {
                            print_err(
                                message
                                    .channel_id
                                    .say(&ctx.http, format!("error connecting to server: {}\nold connection still used.", err))
                                    .await,
                            );
                        }
                    }
                    panic!("manually stop")
                }
                _ => {
                    print_err(
                        message
                            .channel_id
                            .say(&ctx.http, format!("unknown command: {}", command))
                            .await,
                    );
                }
            }
        } else if let Some(command) = message.content.strip_prefix(&self.prefix) {
            let command = command.trim();
            println!("run command: {}", command);

            match { (*self.connection.lock().await).cmd(command).await } {
                Ok(res) => {
                    println!("response: {}", res);
                    print_err(
                        message
                            .channel_id
                            .say(&ctx.http, format!("response from server: \n{}", res))
                            .await,
                    );
                }
                Err(err) => {
                    println!("error    : {}", err);
                    print_err(
                        message
                            .channel_id
                            .say(
                                &ctx.http,
                                format!("error during sending message: \n{}", err),
                            )
                            .await,
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

#[tokio::main]
async fn main() {
    // Discord Bot Token を設定
    let options = read_options().await;

    let connector = SimpleConnector {
        server_kind: options.server_kind,
        address: options.rcon.address.clone(),
        pass: options.rcon.pass.clone(),
    };

    let handler = Handler {
        prefix: options.prefix,
        command: options.command,
        role: options.role.map(RoleId),
        channel: options.channel.map(ChannelId),
        connection: Mutex::new(connector.connect().await.expect("failed to connect")),
        connector: Box::new(connector),
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
