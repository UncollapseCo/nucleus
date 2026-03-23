# Nucleus

Nucleus is a library running at the core of [Osmium](https://osmium.chat) to enable distributed actor systems. Here is a hello world example.

```rs
struct HelloWorldActor {
    greeting: String,
}

impl Actor for HelloWorldActor {}

struct HelloWorldMessage {
    target: String,
}

impl Message for HelloWorldMessage {
    type Result = String;

    fn name() -> &'static str {
        "HelloWorldMessage"
    }
}

#[async_trait_nobox]
impl Handler<HelloWorldMessage> for HelloWorldActor {
    async fn handle(&mut self, msg: HelloWorldMessage, ctx: &mut ActorContext<Self>) -> String {
        return format!("{}, {}!", self.greeting, msg.target);
    }
}

#[tokio::main]
async fn main() {
    let node = NucleusNode::builder(0, "nucleus-example-cluster")
        .build()
        .await
        .expect("Failed to build node");

    let actor_ref = node
        .spawn_actor_local(
            "Greeter",
            HelloWorldActor {
                greeting: "Hello".to_owned(),
            },
        )
        .await
        .expect("Actor failed to spawn");

    let full_greeting = actor_ref
        .send(HelloWorldMessage {
            target: "World".to_owned(),
        })
        .await
        .expect("Failed to send message");

    println!("{}", full_greeting);
}
```

This is quite a lot of code for a hello world, so lets go over it. First, we define an actor type, with some state - in this case its HelloWorldActor with its greeting field. Then, we implement the Actor trait for it, so that it can then receive and process messages. Next, we define a message type, HelloWorldMessage, which has a result type of String. This means that when an actor receives this message, it will process it and return a String as a result. We then implement the Handler trait for the actor, to say that this actor can process messages of this type. The handler function receives `&mut self` of the actor, the message it received, and the actor context, which is useful for interacting with nucleus. In this case, we just return a formatted string using the greeting and the target from the message.

In the main function, we spawn a node, spawn a local actor, and nucleus node gives us back a reference to it. Reference is internally an Arc so we can clone it all we want. We then send the message to it, wait for the result and print it out.

## Distributed Systems

To make this a distributed system, we need to define a bunch more variables. To make an actor reachable remotely, we need to implement the `RemoteActor` trait for it, like this.

```rs
impl RemoteActor for HelloWorldActor {
    fn info() -> &'static RemoteActorInfo {
        &RemoteActorInfo {
            actor_type: "HelloWorldActor",
        }
    }
}
```

Then, the message itself has to implement `Serializable` trait. Notably, all structs that implement the `prost::Message` trait also implement `Serializable`, so if you use protobuf you can skip this part. If you want to implement it manually, you can do it like this.

```rs
impl Serializable for HelloWorldMessage {
    fn serialize(&self) -> Bytes {
        self.target.as_bytes().to_owned().into();
    }

    fn deserialize<B>(data: B, nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        let string = std::str::from_utf8(data.chunk())
            .map_err(|_| DeserializationError::Other("invalid utf-8".to_owned().into()))?
            .to_owned();

        Ok(HelloWorldMessage { target: string })
    }
}
```

Then - we need to set up dynamic dispatch for the system. We also need to tell the nodes how to discover other nodes running the same cluster. This is done at node build time like this.

```rs
let r = HandlerRegistryBuilder::default()
    .with_handler::<HelloWorldActor, HelloWorldMessage>();

let node1 = NucleusNode::builder(0, "nucleus-example-cluster")
    .with_bind_address(SocketAddr::from_str("127.0.0.1:8080").unwrap())
    .with_public_address("127.0.0.1:8080".to_owned())
    .with_registry(r)
    .build()
    .await
    .unwrap();
```

And then on other node do the same but set `.with_upstream_address("127.0.0.1:8080".to_owned())` so that it automatically connects to the first node. Finally, after all that you can do `node.get_named_ref::<HelloWorldActor>("Greeter").await.unwrap()` to run discovery and get a reference to any remote actor, and then send messages to it just like before. Named refs are cached per node so you two calls to the same named actor will return the same ref and wont trigger node discovery messages.

## Pubsub

Nucleus supports pubsub systems out of the box. Actors can subscribe to other actors on a certain topic, and then then receive messages published to that topic by the actor. This is a single-publisher-multiple-subscriber system, not "anyone can publish to everyone" system. To make use of this, define a marker struct implementing `Topic` trait, then use `ctx.subscribe::<TopicType>(actor_name)` to subscribe to an actor. Then, implement `Handler<PubSubMessage<TopicType>>` for the actor where you receive an `Arc<>` of the published messages. To publish messages, use `ctx.publish::<TopicType>(message)`. Note that `subscribe` returns a pubsub ref, which you need to keep in a local variable, as it unsubscribes when dropped.

## Ergonomics

Implementing Handler trait manually is very repetitive, so nucleus exposes the `#[handler]` macro to do it for you. You can write the same handler like this.

```rs
#[handler]
impl HelloWorldActor {
    async fn handle_hello_world_message(&mut self, msg: HelloWorldMessage, ctx: &mut ActorContext<Self>) -> String {
        return format!("{}, {}!", self.greeting, msg.target);
    }
}
```

This will generate the same code as before, but with much less boilerplate. To reduce boilerplate even more, instead of adding message handlers into the remote registry manually, you can enable the `static_registry` feature, add `define_default_registry!()` to your lib.rs, and then add `.with_static_registry(crate::DEFAULT_REGISTRY)` to the node builder. This adds all handlers defined with the `#[handler]` macro into the registry automatically, so you don't forget to add them manually and get runtime errors because of it.

By default, actors process messages sequentially, but sometimes you want to unblock the actor from processing further messaging, but before responding to the current message. This can be done by implementing the `handle_delayed` function in the Handler trait. In this function you receive in addition to self, message and context also a oneshot sender, which you can move to an async task and send the result on. If implementing the Handler trait manually, replace the `handle` function with a panic, and move your logic into `handle_delayed`. Alternatively, use the handler macro with `#[handler(delayed)]` attribute, and it will generate the correct code for you. Note that you should always send a response on the sender, otherwise the caller can panic. Here is an example of a delayed handler.

```rs
#[handler(delayed)]
impl HelloWorldActor {
    async fn handle_hello_world_message(&mut self, msg: HelloWorldMessage, ctx: &mut ActorContext<Self>, sender: nucleus::Sender<String>) {
        tokio::spawn(async move {
            // do some async work here
            let result = format!("{}, {}!", self.greeting, msg.target);
            sender.send(result).unwrap();
        });
    }
}
```

## Backpressure
With the `kanal` feature enabled, you can specify `const MAX_MAILBOX_SIZE: usize = usize::MAX;` when implementing the Actor trait. Messages also implement the `const RESPECT_BACKPRESSURE: bool = false;` constant. The mailbox size is a soft limit, meaning messages that respect backpressure will fail to send if the mailbox is full. By default, messages respect backpressure but it might be logically incorrect to drop messages.

## Actor moving
In a distributed system, you might want to move actors between nodes for load balancing, when shutting down old nodes, or spawning new ones. You can enable this by implementing the `MovableActor` trait on the actor, running `node.register_movable::<ActorType>()` and then call `ctx.move_to(node_id).commit().await.unwrap()` from inside the actor. Note that LocalActorRef-s and RemoteActorRef-s will be invalidated, and sends will fail. For movable actors you should use NamedActorRef-s, which will automatically update their location and/or rediscover the actor location on move. To move pubsub refs, add `.with_movable_sub_ref::<HelloWorldActor, SomeTopic>()` to remote registry at build time, and then chain `.with_pubsub_ref` after `.move_to()`. 

## Performance
Consider enabling the `smallbox` feature for release builds. This speeds up message processing by a significant margin, but slows down development because it slows down type checking a lot (this includes rust-analyzer). To speed up release builds, consider also disabling the `metrics` feature, as it can add considerable overhead to high-throughput actors.

## Other features
Nucleus comes packaged with a bunch of other features that are too long to mention in readme, such as 
- a client library for connecting to the cluster without running a node
- various discovery methods around named refs
- `Spawner` trait for transparent actor spawning on discovery
- standard metrics and tracing observability setup
- actor debugging tools
- useful hooks for actors lifecycle events, including stop/panic/move/pre-message hooks
- anonymous callback messages and messages sent periodically
- transparent remote-node lifecycle management, discovery and heartbeats
- (in development) custom ring buffer style memory allocator for extra speed
- and more