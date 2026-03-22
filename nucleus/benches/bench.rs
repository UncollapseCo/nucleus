use std::{net::SocketAddr, ops::AddAssign, str::FromStr, sync::RwLock, time::Duration};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use nucleus::SmallBox;
use nucleus::{
    NodeId, NucleusNode,
    actor::{
        Actor, ActorContext, RemoteActor, RemoteActorInfo,
        message::{DeserializationError, Handler, Message, Serializable},
        moving::MovableActor,
        refs::RemoteActorRef,
    },
    async_trait_nobox,
    net::remote::HandlerRegistryBuilder,
    utils::join_all_unordered,
};
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap()
}

// fn rt_single() -> Runtime {
//     tokio::runtime::Builder::new_current_thread()
//         .enable_time()
//         .enable_io()
//         .worker_threads(32)
//         .build()
//         .unwrap()
// }

// fn custom_loop(iters: u64) -> Duration {
//     let rt = rt();

//     rt.block_on(async move {
//         // if stuff takes too long, panic
//         let run_task = async move {
//             let started_at = std::time::Instant::now();

//             for i in 0..iters {
//                 info!("iteration {}", i);
//             }
//             let ends_at = std::time::Instant::now();
//             ends_at - started_at
//         };

//         select! {
//             res = run_task => res,
//             _ = tokio::time::sleep(Duration::from_secs(60)) => {
//                 panic!("test took too long");
//             }
//         }
//     })
// }

struct BenchActor {
    num: u64,
}

impl Actor for BenchActor {
    const MAX_MAILBOX_SIZE: usize = usize::MAX;
}
impl RemoteActor for BenchActor {
    fn info() -> &'static RemoteActorInfo {
        &RemoteActorInfo {
            actor_type: "BenchActor",
        }
    }
}

#[async_trait_nobox]
impl MovableActor for BenchActor {
    fn serialize(&self) -> Bytes {
        self.num.to_be_bytes().to_vec().into()
    }

    fn deserialize(
        data: Bytes,
        _nucleus: &NucleusNode,
        _sub_refs: nucleus::net::SubRefMap,
    ) -> Self {
        BenchActor {
            num: u64::from_be_bytes(data.chunk()[0..8].try_into().unwrap()),
        }
    }

    async fn post_resume(&mut self, _ctx: &mut ActorContext<Self>) {
        BENCH_SEND_CHANNEL
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(())
            .unwrap();
    }
}

struct SetNum {
    num: u64,
}

impl Message for SetNum {
    type Result = u64;
    fn name() -> &'static str {
        "SetNum"
    }
}

impl Serializable for SetNum {
    fn serialize(&self) -> Bytes {
        self.num.to_ne_bytes().to_vec().into()
    }

    fn serialize_into<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        buf.put_slice(&self.num.to_ne_bytes());
    }

    fn deserialize<B>(data: B, nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        Ok(SetNum {
            num: u64::from_ne_bytes(data.chunk()[0..8].try_into().unwrap()),
        })
    }
}

#[async_trait_nobox]
impl Handler<SetNum> for BenchActor {
    async fn handle(
        &mut self,
        msg: SetNum,
        _ctx: &mut ActorContext<Self>,
    ) -> <SetNum as Message>::Result {
        let ret = self.num;
        self.num = msg.num;
        ret
    }
}

struct BenchActorStopMsg;

impl Message for BenchActorStopMsg {
    type Result = ();
    fn name() -> &'static str {
        "BenchActorStopMsg"
    }
}

#[async_trait_nobox]
impl Handler<BenchActorStopMsg> for BenchActor {
    async fn handle(&mut self, _msg: BenchActorStopMsg, ctx: &mut ActorContext<Self>) -> () {
        ctx.stop();
    }
}

struct MoveYourself {
    target: NodeId,
}
impl Message for MoveYourself {
    type Result = ();
    fn name() -> &'static str {
        "MoveYourself"
    }
}
#[async_trait_nobox]
impl Handler<MoveYourself> for BenchActor {
    async fn handle(&mut self, msg: MoveYourself, ctx: &mut ActorContext<Self>) -> () {
        ctx.move_to(msg.target).commit().await.unwrap();
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum MessageSendMode {
    SingleAwait = 0,
    NotifThenAwait = 1,
    NotifForget = 2,
}
impl From<u8> for MessageSendMode {
    fn from(val: u8) -> Self {
        match val {
            0 => MessageSendMode::SingleAwait,
            1 => MessageSendMode::NotifThenAwait,
            2 => MessageSendMode::NotifForget,
            _ => panic!("invalid mode"),
        }
    }
}

fn bench_single_node<const MODE: u8>(iters: u64, msg_count: u64) -> Duration {
    let rt = rt();
    let mode: MessageSendMode = MessageSendMode::from(MODE);

    // println!("iters {}, msg_count {}, mode {:?}", iters, msg_count, mode);

    rt.block_on(async move {
        let node = NucleusNode::new_root(
            0,
            "bench",
            (),
            SocketAddr::from_str("127.0.0.1:8999").unwrap(),
            None,
            None,
            HandlerRegistryBuilder::default(),
            0u64,
            #[cfg(feature = "metrics")]
            None,
        )
        .await
        .unwrap();

        //  sleep 2 secs
        // tokio::time::sleep(Duration::from_millis(250)).await;

        const WARMUP_ITERS: u64 = 0;
        let mut started_at = std::time::Instant::now();

        for i in 0..iters + WARMUP_ITERS {
            let actor = BenchActor { num: 0 };
            let actor_ref = node
                .spawn_actor_local("bench".to_owned(), actor)
                .await
                .unwrap();

            if i == WARMUP_ITERS {
                started_at = std::time::Instant::now();
            }

            match mode {
                MessageSendMode::SingleAwait => {
                    for i in 0..msg_count {
                        // info!("iteration {}", i);
                        let _res = actor_ref.send(SetNum { num: i }).await.unwrap();
                    }
                }
                MessageSendMode::NotifThenAwait => {
                    for i in 0..msg_count - 1 {
                        // info!("iteration {}", i);
                        actor_ref.notify(SetNum { num: i }).unwrap();
                    }
                    let _res = actor_ref.send(SetNum { num: iters }).await.unwrap();
                }
                _ => panic!("invalid mode"),
            }

            actor_ref.send(BenchActorStopMsg).await.unwrap();
        }

        let elapsed = started_at.elapsed();
        node.shutdown().await;
        elapsed
    })
}

fn bench_remote_node<const MODE: u8>(iters: u64, msg_count: u64) -> Duration {
    let rt = rt();
    let mode: MessageSendMode = MessageSendMode::from(MODE);
    let _debugstart = std::time::Instant::now();

    // println!("iters {}, msg_count {}, mode {:?}", iters, msg_count, mode);

    rt.block_on(async move {
        // println!("inside async {:?}", debugstart.elapsed());

        let registry1 = HandlerRegistryBuilder::default()
            .with_actor::<BenchActor>()
            .with_handler::<BenchActor, SetNum>();
        let node1 = NucleusNode::new_root(
            0,
            "bench",
            (),
            SocketAddr::from_str("127.0.0.1:8999").unwrap(),
            None,
            None,
            registry1,
            0u64,
            #[cfg(feature = "metrics")]
            None,
        )
        .await
        .unwrap();

        let registry2 = HandlerRegistryBuilder::default()
            .with_actor::<BenchActor>()
            .with_handler::<BenchActor, SetNum>();
        let node2 = NucleusNode::new_root(
            1,
            "bench",
            (),
            SocketAddr::from_str("127.0.0.1:8998").unwrap(),
            None,
            Some("127.0.0.1:8999".to_owned()),
            registry2,
            0u64,
            #[cfg(feature = "metrics")]
            None,
        )
        .await
        .unwrap();

        //  sleep 2 secs
        tokio::time::sleep(Duration::from_millis(200)).await;

        // println!("set up elapsed {:?}", debugstart.elapsed());

        // println!("iterating {}, {:?}", i, debugstart.elapsed());
        let started_at = std::time::Instant::now();

        for _i in 0..iters {
            let actor = BenchActor { num: 0 };
            let actor_ref = node1.spawn_actor("bench".to_owned(), actor).await.unwrap();

            let remote_actor_ref: RemoteActorRef<BenchActor> = node2
                .get_ref_by_id(actor_ref.actor_id())
                .unwrap()
                .unwrap_remote();

            match mode {
                MessageSendMode::SingleAwait => {
                    for i in 0..msg_count {
                        // info!("iteration {}", i);
                        let _res = remote_actor_ref.send(SetNum { num: i }).await.unwrap();
                    }
                }
                MessageSendMode::NotifThenAwait => {
                    for i in 0..msg_count - 1 {
                        // info!("iteration {}", i);
                        remote_actor_ref.notify(SetNum { num: i }).await.unwrap();
                    }
                    let _res = remote_actor_ref.send(SetNum { num: iters }).await.unwrap();
                }
                MessageSendMode::NotifForget => {
                    for i in 0..msg_count - 1 {
                        // info!("iteration {}", i);
                        // println!("iteration {}", i);remote_actor_ref
                        remote_actor_ref.notify_forget(SetNum { num: i }).unwrap();
                    }
                    let _res = remote_actor_ref.send(SetNum { num: iters }).await.unwrap();
                }
            }

            actor_ref.send(BenchActorStopMsg).await.unwrap();
        }

        let elapsed = started_at.elapsed();

        // println!("shutting down {:?}", debugstart.elapsed());
        node1.shutdown().await;
        node2.shutdown().await;

        // println!("done {:?}", debugstart.elapsed());

        elapsed
    })
}

static BENCH_SEND_CHANNEL: RwLock<Option<tokio::sync::mpsc::UnboundedSender<()>>> =
    RwLock::new(None);

fn bench_moving(iters: u64, actor_count: u64) -> Duration {
    let rt = rt();
    let _debugstart = std::time::Instant::now();

    rt.block_on(async move {
        // println!("inside async {:?}", debugstart.elapsed());
        let mut total_duration = Duration::from_nanos(0);

        let registry1 = HandlerRegistryBuilder::default()
            .with_actor::<BenchActor>()
            .with_handler::<BenchActor, SetNum>();
        let node1 = NucleusNode::new_root(
            0,
            "bench",
            (),
            SocketAddr::from_str("127.0.0.1:8999").unwrap(),
            None,
            None,
            registry1,
            0u64,
            #[cfg(feature = "metrics")]
            None,
        )
        .await
        .unwrap();
        node1.registry().register_movable::<BenchActor>();

        let registry2 = HandlerRegistryBuilder::default()
            .with_actor::<BenchActor>()
            .with_handler::<BenchActor, SetNum>();
        let node2 = NucleusNode::new_root(
            1,
            "bench",
            (),
            SocketAddr::from_str("127.0.0.1:8998").unwrap(),
            None,
            Some("127.0.0.1:8999".to_owned()),
            registry2,
            0u64,
            #[cfg(feature = "metrics")]
            None,
        )
        .await
        .unwrap();
        node2.registry().register_movable::<BenchActor>();

        //  sleep 2 secs
        tokio::time::sleep(Duration::from_millis(500)).await;

        // println!("a");

        // println!("set up elapsed {:?}", debugstart.elapsed());

        for _i in 0..iters {
            let mut actor_refs = vec![];
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            BENCH_SEND_CHANNEL.write().unwrap().replace(tx);
            // println!("b");

            for i in 0..actor_count {
                let actor = BenchActor { num: 0 };
                let actor_ref = node1
                    .spawn_actor(format!("bench-{}", i), actor)
                    .await
                    .unwrap();
                actor_refs.push(actor_ref);
            }
            // println!("c");

            let recv_task = tokio::spawn(async move {
                for _ in 0..actor_count {
                    rx.recv().await.unwrap();
                }
            });

            tokio::time::sleep(Duration::from_millis(50)).await;

            // println!("d");

            // println!("iterating {}, {:?}", i, debugstart.elapsed());
            let started_at = std::time::Instant::now();

            for a in actor_refs.iter() {
                a.notify(MoveYourself { target: 1 }).unwrap();
            }

            // println!("e");

            recv_task.await.unwrap();

            // println!("f");

            let cycle_dur = started_at.elapsed();

            total_duration.add_assign(cycle_dur);

            for i in 0..actor_count {
                node2
                    .get_named_ref::<BenchActor>(format!("bench-{}", i))
                    .await
                    .unwrap()
                    .get_inner_local()
                    .await
                    .unwrap()
                    .send(BenchActorStopMsg)
                    .await
                    .unwrap();
            }

            // println!("g");

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // println!("shutting down {:?}", debugstart.elapsed());
        node1.shutdown().await;
        node2.shutdown().await;

        // println!("done {:?}", debugstart.elapsed());

        total_duration
    })
}

pub fn benchmark(c: &mut Criterion) {
    // println!(
    //     "test duration {}",
    //     bench_remote_node::<1>(2, 100).as_nanos()
    // );

    let mut group = c.benchmark_group("single_node");

    for msg_count in [100000, 300000] {
        group
            .sample_size(25)
            .throughput(criterion::Throughput::Elements(msg_count))
            .warm_up_time(Duration::from_millis(1))
            .bench_with_input(
                BenchmarkId::new("sendwait", msg_count),
                &msg_count,
                |b, mcount| b.iter_custom(|iters| bench_single_node::<0>(iters, *mcount)),
            );
    }

    group.finish();

    let mut single_group_2 = c.benchmark_group("single_node_2");
    for msg_count in [1000000, 3000000] {
        single_group_2
            .sample_size(25)
            .throughput(criterion::Throughput::Elements(msg_count))
            .warm_up_time(Duration::from_millis(1))
            .bench_with_input(
                BenchmarkId::new("notifywait", msg_count),
                &(msg_count),
                |b, mcount| b.iter_custom(|iters| bench_single_node::<1>(iters, *mcount)),
            );
    }

    single_group_2.finish();

    let mut remote_group = c.benchmark_group("remote_node");

    for msg_count in [10000, 40000] {
        remote_group
            .sample_size(25)
            .throughput(criterion::Throughput::Elements(msg_count))
            .warm_up_time(Duration::from_millis(1))
            .bench_with_input(
                BenchmarkId::new("sendwait", msg_count),
                &msg_count,
                |b, mcount| b.iter_custom(|iters| bench_remote_node::<0>(iters, *mcount)),
            )
            .bench_with_input(
                BenchmarkId::new("notifywait", msg_count),
                &msg_count,
                |b, mcount| b.iter_custom(|iters| bench_remote_node::<1>(iters, *mcount)),
            );
    }

    remote_group.finish();

    let mut remote_group_2 = c.benchmark_group("remote_node_2");
    for msg_count in [10000, 40000] {
        let msg_count = msg_count * 30;
        remote_group_2
            .sample_size(25)
            .throughput(criterion::Throughput::Elements(msg_count))
            .warm_up_time(Duration::from_millis(1))
            .bench_with_input(
                BenchmarkId::new("notifyforget", msg_count),
                &(msg_count),
                |b, mcount| b.iter_custom(|iters| bench_remote_node::<2>(iters, *mcount)),
            );
    }

    remote_group_2.finish();
}

pub fn bench_moving_actor(c: &mut Criterion) {
    let mut group = c.benchmark_group("moving");

    for actor_count in [1000, 2500, 10000] {
        group
            .sample_size(10)
            .throughput(criterion::Throughput::Elements(actor_count))
            .warm_up_time(Duration::from_millis(1))
            .bench_with_input(
                BenchmarkId::new("move", actor_count),
                &actor_count,
                |b, actor_count| b.iter_custom(|iters| bench_moving(iters, *actor_count)),
            );
    }

    group.finish();
}

criterion_group!(benches, benchmark, bench_moving_actor);
criterion_main!(benches);
