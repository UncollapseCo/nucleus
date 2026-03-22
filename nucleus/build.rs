use std::io::Result;

fn main() -> Result<()> {
    let mut c = prost_build::Config::new();
    c.bytes(["."]);

    // prost_build::compile_protos(
    //     &[
    //         "proto/core.proto",
    //         "proto/remote.proto",
    //         "proto/pubsub.proto",
    //         "proto/mover.proto",
    //         "proto/client.proto",
    //         "proto/debug.proto",
    //     ],
    //     &["proto/", "./"],
    // )?;

    c.compile_protos(
        &[
            "proto/core.proto",
            "proto/remote.proto",
            "proto/pubsub.proto",
            "proto/mover.proto",
            "proto/client.proto",
            "proto/debug.proto",
        ],
        &["proto/", "./"],
    )?;

    Ok(())
}
