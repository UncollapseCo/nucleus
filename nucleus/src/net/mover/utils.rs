use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    NucleusNode,
    pubsub::{PubsubRef, Topic},
};

#[derive(Default)]
pub struct SubRefMap {
    #[expect(clippy::type_complexity)] // 🤓
    sub_refs: HashMap<String, HashMap<Arc<str>, (Box<dyn Any + Send + Sync>, u64)>>,

    outgoing_seqs: HashMap<&'static str, u64>,
}

impl SubRefMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert_ref(
        &mut self,
        topic: String,
        name: Arc<str>,
        value: Box<dyn Any + Send + Sync>,
    ) {
        let entry = self.sub_refs.entry(topic).or_default();
        entry.insert(name, (value, 0));
    }

    pub fn get_ref<T: Topic>(&mut self) -> Option<Vec<PubsubRef<T>>> {
        let key = T::name();

        let v = self.sub_refs.remove(key)?;

        // downcast all to PubsubRef<T>
        Some(
            v.into_iter()
                .map(|x| {
                    let x = x.1.0.downcast::<PubsubRef<T>>().unwrap();
                    *x
                })
                .collect(),
        )
    }

    pub(crate) fn insert_seq(&mut self, topic: &str, name: &str, seq: u64) {
        if let Some(x) = self.sub_refs.get_mut(topic) {
            if let Some(y) = x.get_mut(name) {
                y.1 = seq;
            }
        }
    }

    pub(crate) fn get_seqs(
        &mut self,
        nucleus: &NucleusNode,
    ) -> HashMap<(Arc<str>, &'static str), u64> {
        let mut res = HashMap::new();
        for (topic, v) in self.sub_refs.iter() {
            let topic_name = nucleus.registry().get_topic_handler(topic);
            if topic_name.is_none() {
                error!(
                    "<hazard> No topic handler found for topic: {}, skipping seq",
                    topic
                );
                continue;
            }

            let topic_name = topic_name.unwrap().name();
            for (name, (_, seq)) in v.iter() {
                res.insert((name.clone(), topic_name), *seq);
            }
        }

        res
    }

    pub(crate) fn insert_outgoing_seq(
        &mut self,
        nucleus: &NucleusNode,
        topic: &'static str,
        seq: u64,
    ) {
        if let Some(topic) = nucleus.registry().get_topic_handler(topic) {
            self.outgoing_seqs.insert(topic.name(), seq);
        } else {
            error!(
                "<hazard> No topic handler found for topic: {}, skipping outgoing seq",
                topic
            );
        }
    }

    pub(crate) fn get_outgoing_seqs(&self) -> HashMap<&'static str, u64> {
        self.outgoing_seqs.clone()
    }
}

impl Debug for SubRefMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubRefMap")
            .field("sub_refs", &self.sub_refs)
            .finish()
    }
}
