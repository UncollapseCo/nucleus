use chrono::{DateTime, Local, TimeZone as _, Utc};
use nucleus::{actor::message::Serializable, net::proto};

pub fn recipient_from_string(inp: &str) -> proto::client::client_remote_message::Recipient {
    // try parse to u64
    if let Ok(id) = inp.parse::<u64>() {
        proto::client::client_remote_message::Recipient::ActorId(id)
    } else {
        proto::client::client_remote_message::Recipient::ActorName(inp.to_owned())
    }
}

pub fn guess_actor_type(name: &str) -> Option<String> {
    if name == "System.Registry" {
        Some("RemoteRegistry".to_owned())
    } else if name.starts_with("System.Remote-") {
        Some("System.RemoteNode".to_owned())
    } else if name.starts_with("System.Mover-") {
        Some("System.Mover".to_owned())
    } else {
        None
    }
}

pub fn try_deserialize_dumped_actor(actor_name: &str, data: &[u8]) -> Option<String> {
    if let Some(t) = guess_actor_type(actor_name) {
        if t == "System.RemoteNode" {
            Some(format!(
                "{:#?}",
                proto::debug::SerializedRemoteNode::deserialize(data, None).unwrap()
            ))
        } else if t == "System.Mover" {
            Some(format!(
                "{:#?}",
                proto::debug::SerializedMover::deserialize(data, None).unwrap()
            ))
        } else {
            None
        }
    } else {
        None
    }
}

pub fn actor_info_fmt(actor_info: &proto::debug::ActorInfo) -> String {
    let datetime = Utc
        .timestamp_opt((actor_info.id.timestamp() / 1000) as i64, 0)
        .unwrap();
    let datetime_local: DateTime<Local> = DateTime::from(datetime);

    format!(
        "{} {} (node {}) {}",
        actor_info.id,
        datetime_local.format("%Y-%m-%d %H:%M:%S"),
        actor_info.id.worker_id(),
        actor_info.name,
    )
}
