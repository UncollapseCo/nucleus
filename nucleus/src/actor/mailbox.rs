use std::fmt::Display;

use crate::actor::{Actor, message::MessageHandler, refs::ActorRefErr};

pub(crate) struct Mailbox<A: Actor> {
    #[cfg(not(feature = "kanal"))]
    inner: tokio::sync::mpsc::UnboundedReceiver<MessageHandler<A>>,
    #[cfg(feature = "kanal")]
    inner: kanal::AsyncReceiver<MessageHandler<A>>,
}

pub(crate) struct MailboxSender<A: Actor> {
    #[cfg(not(feature = "kanal"))]
    inner: tokio::sync::mpsc::UnboundedSender<MessageHandler<A>>,
    #[cfg(feature = "kanal")]
    inner: kanal::Sender<MessageHandler<A>>,
}

pub(crate) struct MailboxSendError<A: Actor> {
    msg: MessageHandler<A>,
    err: ActorRefErr,
}

impl<A: Actor> Display for MailboxSendError<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to send message to mailbox")
    }
}

impl<A: Actor> MailboxSendError<A> {
    pub(crate) fn new(msg: MessageHandler<A>, err: ActorRefErr) -> Self {
        Self { msg, err }
    }

    pub(crate) fn into_inner(self) -> MessageHandler<A> {
        self.msg
    }

    pub(crate) fn err(&self) -> ActorRefErr {
        self.err
    }
}

#[cfg(feature = "kanal")]
mod kanal_impl {
    use std::{any::type_name, collections::VecDeque};

    use crate::actor::{
        Actor,
        mailbox::{Mailbox, MailboxSendError, MailboxSender},
        message::MessageHandler,
        refs::ActorRefErr,
    };

    impl<A: Actor> Mailbox<A> {
        pub(crate) fn new() -> (MailboxSender<A>, Self) {
            let (tx, rx) = kanal::unbounded_async();
            (
                MailboxSender {
                    inner: tx.to_sync(),
                },
                Mailbox { inner: rx },
            )
        }

        pub(crate) async fn recv(&mut self) -> Option<MessageHandler<A>> {
            self.inner.recv().await.ok()
        }

        // pub(crate) async fn recv_batch(&mut self) -> Vec<MessageHandler<A>> {
        //     self.inner.recv()
        // }

        pub(crate) fn len(&self) -> usize {
            self.inner.len()
        }

        pub(crate) fn close(&mut self) {
            let mut remaining = vec![];

            self.inner
                .as_sync()
                .drain_close(&mut remaining)
                .expect("Unreachable");

            let (tx, rx) = kanal::unbounded_async();
            self.inner = rx;

            let mut remaining: VecDeque<_> = VecDeque::from(remaining);

            // only errors if receivers are dropped, which they aren't
            tx.as_sync().send_many(&mut remaining).expect("Unreachable");
        }
    }

    impl<A: Actor> MailboxSender<A> {
        pub(crate) fn send(&self, msg: MessageHandler<A>) -> Result<(), MailboxSendError<A>> {
            self.inner.try_send(msg).map_err(|e| MailboxSendError {
                msg: e.into_inner(),
                err: ActorRefErr::InvalidRef,
            })?;

            Ok(())
        }

        pub(crate) fn send_limit_len(
            &self,
            msg: MessageHandler<A>,
        ) -> Result<(), MailboxSendError<A>> {
            self.inner
                .try_send_limit_len(msg, A::MAX_MAILBOX_SIZE)
                .map_err(|e| {
                    let err = match &e {
                        kanal::SendTimeoutError::Timeout(_) => {
                            let len = self.inner.len();
                            warn!(
                                "Mailbox is full for {} (len: {}, max: {}), dropping message",
                                type_name::<A>(),
                                len,
                                A::MAX_MAILBOX_SIZE
                            );

                            ActorRefErr::ActorMailboxFull
                        }
                        kanal::SendTimeoutError::Closed(_) => ActorRefErr::InvalidRef,
                    };

                    MailboxSendError {
                        msg: e.into_inner(),
                        err,
                    }
                })?;

            Ok(())
        }

        pub(crate) fn is_closed(&self) -> bool {
            self.inner.is_closed()
        }
    }
}

#[cfg(not(feature = "kanal"))]
mod tokio_mpsc {
    use tokio::sync::mpsc;

    use crate::actor::{
        Actor,
        mailbox::{Mailbox, MailboxSendError, MailboxSender},
        message::MessageHandler,
    };

    impl<A: Actor> Mailbox<A> {
        pub(crate) fn new() -> (MailboxSender<A>, Self) {
            let (tx, rx) = mpsc::unbounded_channel();
            (MailboxSender { inner: tx }, Mailbox { inner: rx })
        }

        pub(crate) async fn recv(&mut self) -> Option<MessageHandler<A>> {
            self.inner.recv().await
        }

        pub(crate) fn len(&self) -> usize {
            self.inner.len()
        }

        pub(crate) fn close(&mut self) {
            self.inner.close();
        }
    }

    impl<A: Actor> MailboxSender<A> {
        #[allow(clippy::result_large_err)]
        // this is only called in one place, so its okay for it to be large
        pub fn send(&self, msg: MessageHandler<A>) -> Result<(), MailboxSendError<A>> {
            self.inner
                .send(msg)
                .map_err(|e| MailboxSendError { msg: e.0 })
        }

        // No backpressure support in tokio mpsc unbounded channel, just alias to send
        pub(crate) fn send_limit_len(
            &self,
            msg: MessageHandler<A>,
        ) -> Result<(), MailboxSendError<A>> {
            self.send(msg)
        }

        pub fn is_closed(&self) -> bool {
            self.inner.is_closed()
        }
    }
}
