use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use chrono::{Duration, Utc};
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use sqlx::PgPool;
use tokio::runtime::Handle;
use tracing::{info, warn};

use crate::db;
use crate::window::PartitionState;

pub type SharedStates = Arc<Mutex<HashMap<i32, PartitionState>>>;

/// Custom Kafka consumer context that takes over offset management: instead
/// of trusting Kafka's own consumer-offsets topic, every rebalance loads or
/// drops partition state to/from Postgres, which is the single source of
/// truth for "how far has this partition been durably aggregated".
///
/// Rebalance callbacks run on librdkafka's internal poll thread, not on the
/// Tokio runtime, so the Postgres load in `post_rebalance` is driven via a
/// borrowed `tokio::runtime::Handle::block_on` rather than being spawned.
pub struct RebalanceContext {
    pool: PgPool,
    states: SharedStates,
    rt: Handle,
    consumer: Mutex<Option<Weak<StreamConsumer<RebalanceContext>>>>,
}

impl RebalanceContext {
    pub fn new(pool: PgPool, states: SharedStates, rt: Handle) -> Self {
        RebalanceContext {
            pool,
            states,
            rt,
            consumer: Mutex::new(None),
        }
    }

    /// Must be called once, right after the consumer is constructed, so the
    /// context can call back into it during `post_rebalance` to seek newly
    /// assigned partitions.
    pub fn bind_consumer(&self, consumer: &Arc<StreamConsumer<RebalanceContext>>) {
        *self.consumer.lock().unwrap() = Some(Arc::downgrade(consumer));
    }
}

impl ClientContext for RebalanceContext {}

impl ConsumerContext for RebalanceContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if let Rebalance::Revoke(tpl) = rebalance {
            {
                let mut states = self.states.lock().unwrap();
                for elem in tpl.elements() {
                    info!(
                        partition = elem.partition(),
                        "partition revoked, dropping in-memory window state (already-flushed data is safe in postgres)"
                    );
                    states.remove(&elem.partition());
                }
            }

            // We've overridden the default rebalance handling entirely (by
            // implementing these callbacks at all), so we're also on the
            // hook for actually applying the revoke. `cooperative-sticky`
            // requires the incremental variant, not plain `unassign()`.
            if let Some(consumer) = self.consumer.lock().unwrap().as_ref().and_then(Weak::upgrade) {
                if let Err(e) = consumer.incremental_unassign(tpl) {
                    warn!(error = %e, "failed to incrementally unassign revoked partitions");
                }
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        if let Rebalance::Assign(tpl) = rebalance {
            let Some(consumer) = self
                .consumer
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
            else {
                warn!("post_rebalance fired before consumer was bound; skipping seek");
                return;
            };

            let mut seek_tpl = TopicPartitionList::new();
            for elem in tpl.elements() {
                let partition = elem.partition();
                let topic = elem.topic().to_string();

                // post_rebalance runs on a Tokio worker thread (rdkafka's
                // async consumer drives callbacks from within the runtime),
                // so a plain `block_on` here would panic with "cannot start
                // a runtime from within a runtime". `block_in_place` hands
                // this worker's other queued tasks off to another thread
                // first, which makes blocking here safe.
                let rt = self.rt.clone();
                let pool = self.pool.clone();
                let progress = tokio::task::block_in_place(|| {
                    rt.block_on(db::load_progress(&pool, partition))
                });
                let (offset, watermark, seek_offset) = match progress {
                    Ok(Some(p)) => (p.committed_offset, p.flushed_watermark, Offset::Offset(p.committed_offset)),
                    Ok(None) => (0, Utc::now() - Duration::days(1), Offset::Beginning),
                    Err(e) => {
                        warn!(partition, error = %e, "failed to load partition progress, defaulting to earliest");
                        (0, Utc::now() - Duration::days(1), Offset::Beginning)
                    }
                };

                info!(partition, offset, "partition assigned, seeking to committed offset");
                if let Err(e) = seek_tpl.add_partition_offset(&topic, partition, seek_offset) {
                    warn!(partition, error = %e, "failed to stage seek offset");
                }

                self.states
                    .lock()
                    .unwrap()
                    .insert(partition, PartitionState::resume_from(partition, offset, watermark));
            }

            if let Err(e) = consumer.incremental_assign(&seek_tpl) {
                warn!(error = %e, "failed to assign seeked partitions");
            }
        }
    }
}
