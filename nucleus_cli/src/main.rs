use clap::Parser;
use nucleus::{actor::RemoteActor, net::proto};
use std::io::Write;
use test_cluster::CliActor;
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    select,
    sync::mpsc,
};
use tracing::Instrument as _;
use utils::{
    actor_info_fmt, guess_actor_type, recipient_from_string, try_deserialize_dumped_actor,
};

mod test_cluster;
mod utils;

#[derive(Parser)]
#[command(long_about = None)]
pub(crate) struct Cli {
    // addr
    #[arg(env, long = "addr", help = "Addr", default_value = "127.0.0.1:8080")]
    pub(crate) addr: String,

    // bool whether to start own test cluster
    #[arg(long, short, action, help = "Start own test cluster")]
    pub(crate) test_cluster: bool,

    // command to execute on start, raw string
    #[arg(long, short, help = "Command to execute on start")]
    pub(crate) command: Option<String>,
}

#[tokio::main]
async fn main() {
    let parsed = Cli::parse();

    let single_cmd_mode = parsed.command.is_some();

    let cluster = if parsed.test_cluster {
        Some(test_cluster::start_cluster(!single_cmd_mode).await)
    } else {
        None
    };

    let mut cli = nucleus::client::NucleusClient::connect(parsed.addr, "nucleus-cli".to_owned())
        .await
        .unwrap();

    let mut ctrlc = nucleus::ctrl_c().unwrap();

    // Get tokio's version of stdin, which implements AsyncRead
    let stdin = io::stdin();
    // Create a buffered wrapper, which implements BufRead
    let reader = BufReader::new(stdin);
    // Take a stream of lines from this
    let mut lines = reader.lines();

    loop {
        let input;
        let line;

        if !single_cmd_mode {
            print!("> ");
            std::io::stdout().flush().unwrap();

            select! {
                l = lines.next_line() => {
                    line = l;
                }
                // ctrlc
                _ = ctrlc.recv() => {
                    break;
                }
            };
        } else {
            line = Ok(Some(parsed.command.clone().unwrap()));
        }

        if let Ok(Some(inp)) = line {
            input = inp;
        } else {
            break;
        }

        let input = input.trim();

        if input == "q" {
            break;
        }

        let words = input.split_whitespace().collect::<Vec<_>>();

        let out = match words.as_slice() {
            ["show"] => {
                // debug print cli.handshake()
                Ok(format!("{:#?}", cli.handshake()))
            }
            ["ls"] | ["list"] | ["list", "all"] => {
                let list_all = matches!(words.as_slice(), ["list", "all"]);

                let res = cli
                    .submit_remote_registry_message(proto::debug::ListActors { global: list_all })
                    .await
                    .await;

                match res {
                    Ok(res) => {
                        // return Ok debug message
                        let mut s = res
                            .actors
                            .into_iter()
                            .map(|x| actor_info_fmt(&x))
                            .collect::<Vec<String>>();

                        s.sort();
                        Ok(s.join("\n").to_string())
                    }
                    Err(e) => {
                        // return Err debug message
                        Err(format!("{:?}", e))
                    }
                }
            }
            ["get", name] => {
                let res = cli
                    .submit_remote_message(
                        test_cluster::GetNumber,
                        recipient_from_string(name),
                        Some(CliActor::info().actor_type.to_owned()),
                    )
                    .await
                    .await;

                match res {
                    Ok(res) => Ok(format!("{}", res)),
                    Err(e) => Err(format!("{:?}", e)),
                }
            }
            ["set", name, num] => {
                let num = num.parse().unwrap();
                let res = cli
                    .submit_remote_message(
                        test_cluster::SetNumber { num },
                        recipient_from_string(name),
                        Some(CliActor::info().actor_type.to_owned()),
                    )
                    .await
                    .await;

                match res {
                    Ok(res) => Ok(format!("{}", res)),
                    Err(e) => Err(format!("{:?}", e)),
                }
            }
            ["dbg", name] | ["dbg", "all", name] => {
                let global = matches!(words.as_slice(), ["dbg", "all", _]);

                let mut targets = Vec::new();
                let r = if let Some(pattern) = name.strip_suffix('*') {
                    // list actors, pattern match
                    let list = cli
                        .submit_remote_registry_message(proto::debug::ListActors { global })
                        .await
                        .await
                        .map_err(|e| format!("listing failed - {:?}", e.err));

                    match list {
                        Ok(list) => {
                            for actor in list.actors {
                                if actor.name.starts_with(pattern) {
                                    targets.push(actor);
                                }
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    // discover named actor
                    let res = cli.discover_named_actor(name.to_owned().to_owned()).await;
                    match res {
                        Ok(res) => {
                            if res.actor_id.is_some() {
                                targets.push(proto::debug::ActorInfo {
                                    id: res.actor_id.unwrap(),
                                    name: name.to_owned().to_owned(),
                                    actor_type: res.actor_type.map(|v| v.to_string()),
                                });
                                Ok(())
                            } else {
                                Err("actor not found".to_owned())
                            }
                        }
                        Err(e) => Err(format!("{:?}", e)),
                    }
                };

                if let Err(e) = r {
                    Err(e)
                } else {
                    for actor in targets {
                        println!("---\n{}\n", actor_info_fmt(&actor));
                        let res = cli
                            .submit_remote_message(
                                proto::debug::GetSerialized {},
                                proto::client::client_remote_message::Recipient::ActorId(
                                    actor.id.id(),
                                ),
                                actor.actor_type.or_else(|| guess_actor_type(&actor.name)),
                            )
                            .await
                            .await;

                        match res {
                            Ok(res) => {
                                if let Some(s) =
                                    try_deserialize_dumped_actor(&actor.name, res.as_slice())
                                {
                                    println!("{}", s);
                                } else {
                                    println!("{:?}", res);
                                }
                            }
                            Err(e) => println!("err: {:?}", e),
                        }
                    }

                    Ok("".to_owned())
                }
            }
            ["find", name] => {
                let res = cli.discover_named_actor(name.to_owned().to_owned()).await;

                match res {
                    Ok(res) => {
                        if res.actor_id.is_some() {
                            Ok(format!(
                                "{} ({}) - {} (node {})",
                                name,
                                res.actor_type.unwrap(),
                                res.actor_id.unwrap(),
                                res.actor_id.unwrap().worker_id()
                            ))
                        } else {
                            Err("actor not found".to_owned())
                        }
                    }
                    Err(e) => Err(format!("{:?}", e)),
                }
            }
            ["stop", name] => {
                let res = cli
                    .submit_remote_message(
                        proto::debug::ActorStopMessage {},
                        recipient_from_string(name),
                        guess_actor_type(name),
                    )
                    .await
                    .await;

                match res {
                    Ok(res) => Ok(format!("{:?}", res)),
                    Err(e) => Err(format!("{:?}", e)),
                }
            }
            ["ping"] | ["ping", "all"] => {
                let ping_global = matches!(words.as_slice(), ["ping", "all"]);

                let res = ping_all(&mut cli, ping_global).await;
                if let Err(e) = res {
                    Err(e)
                } else {
                    Ok("".to_owned())
                }
            }
            ["ping", name] => {
                let res = cli
                    .submit_remote_message(
                        proto::debug::PingMsg {},
                        recipient_from_string(name),
                        guess_actor_type(name),
                    )
                    .await
                    .await;

                match res {
                    Ok(res) => Ok(format!("{:?}", res)),
                    Err(e) => Err(format!("{:?}", e)),
                }
            }
            [] => Ok("".to_owned()),

            _ => Ok("unknown command".to_owned()),
        };

        match out {
            Ok(out) => println!("{}", out),
            Err(e) => println!("err: {}", e),
        }

        if single_cmd_mode {
            break;
        }
    }

    if let Some(cluster) = cluster {
        cluster.0.shutdown().await;
        cluster.1.shutdown().await;
    }
}

async fn ping_all(
    cli: &mut nucleus::client::NucleusClient,
    ping_global: bool,
) -> Result<(), String> {
    let list = cli
        .submit_remote_registry_message(proto::debug::ListActors {
            global: ping_global,
        })
        .await
        .await
        .map_err(|e| format!("listing failed - {:?}", e.err))?;

    let count = list.actors.len();

    // ping all actors concurrently, measure time
    let (tx, mut rx) = mpsc::channel(list.actors.len());

    for actor in list.actors {
        let start = std::time::Instant::now();
        let sub = cli
            .submit_remote_message(
                proto::debug::PingMsg {},
                proto::client::client_remote_message::Recipient::ActorId(actor.id.id()),
                actor
                    .actor_type
                    .clone()
                    .or_else(|| guess_actor_type(&actor.name)),
            )
            .await;
        let tx = tx.clone();

        tokio::spawn(
            async move {
                // timeout of 10 seconds

                let res = tokio::time::timeout(std::time::Duration::from_secs(10), sub).await;

                let elapsed = start.elapsed();

                let res = match res {
                    Ok(Ok(_)) => (actor, elapsed, Ok(())),
                    Ok(Err(e)) => (actor, elapsed, Err(format!("{:?}", e.err))),
                    Err(e) => (actor, elapsed, Err(format!("{:?}", e))),
                };

                tx.send(res).await.unwrap();
            }
            .in_current_span(),
        );
    }

    let mut success = vec![];
    let mut errors = vec![];

    for _ in 0..count {
        let (actor, elapsed, res) = rx.recv().await.unwrap();

        match res {
            Ok(_) => {
                success.push((
                    elapsed.as_millis(),
                    format!(
                        "{:3}ms (node {}) - {}\n",
                        elapsed.as_millis(),
                        actor.id.worker_id(),
                        actor.name
                    ),
                ));
            }
            Err(e) => {
                errors.push(format!(
                    "{:3}ms (node {}) - {} - {:?}\n",
                    elapsed.as_millis(),
                    actor.id.worker_id(),
                    actor.name,
                    e
                ));
            }
        }
    }

    // sort success by time
    success.sort();

    let mut res = String::new();
    res.push_str("\n- Ping results:\n");

    for (_, s) in success {
        res.push_str(&s);
    }

    if !errors.is_empty() {
        res.push_str("- Errors:\n");
    }

    for e in errors {
        res.push_str(&e);
    }

    println!("{}", res);

    Ok(())
}
