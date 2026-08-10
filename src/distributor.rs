//! Central distributor: pushes changes (`update`) to a list of capnp
//! backends, one at a time.

use std::time::Duration;

use crate::client::{SyncClient, SyncResult};
use crate::error::Error;
use crate::ops::SyncOperations;

/// Per-backend outcome of a distribution pass.
#[derive(Debug)]
pub struct DistributeResult {
    pub backend: String,
    pub result: Result<SyncResult, Error>,
}

/// Distributes operations to backends sequentially.
pub struct Distributor {
    timeout: Duration,
}

impl Distributor {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Push `ops` to a single backend.
    pub async fn distribute_one(
        &self,
        addr: &str,
        ops: &SyncOperations,
    ) -> Result<SyncResult, Error> {
        let client = SyncClient::connect(addr, self.timeout).await?;
        client.update(ops).await
    }

    /// Push `ops` to every backend in turn; one failure does not stop the
    /// others. Returns per-backend results in list order.
    pub async fn distribute(
        &self,
        backends: &[String],
        ops: &SyncOperations,
    ) -> Vec<DistributeResult> {
        if backends.is_empty() {
            log::warn!("distributor: empty backend list, nothing to distribute");
            return Vec::new();
        }
        let mut out = Vec::with_capacity(backends.len());
        for addr in backends {
            let result = self.distribute_one(addr, ops).await;
            match &result {
                Ok(r) => log::info!("distributor: {addr}: update status={}", r.status),
                Err(e) => log::warn!("distributor: {addr}: failed: {e}"),
            }
            out.push(DistributeResult {
                backend: addr.clone(),
                result,
            });
        }
        out
    }
}
