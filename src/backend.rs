//! Backend side of the sync protocol: pluggable storage traits and a ready
//! `SyncService` handler.
//!
//! A backend implements [`ActionPool`] (pending operations) and [`Records`]
//! (its dataset) over its own storage, then calls [`serve`] — `pull`/`ack`/
//! `checkIds`/`dump`/`update` work out of the box. Custom handlers (e.g. the
//! finder) can be served directly with [`crate::server::serve`].

use std::sync::Arc;

use crate::finder_capnp::sync_service;
use crate::ops::{Operation, Window, fill_operations, parse_operations};

// ---------------------------------------------------------------------------
// Storage traits
// ---------------------------------------------------------------------------

/// The backend's pending action pool.
///
/// Contract: operations carry monotonic `seq` numbers (enqueue order);
/// `cursor` is the first unconfirmed seq (inclusive); [`Window::next_cursor`]
/// must be `last returned seq + 1` (or the input `cursor` when empty);
/// `ack(up_to)` deletes exactly `seq < up_to` — ops appended mid-sync get
/// higher seqs and are never deleted by an ack.
pub trait ActionPool: Send + Sync {
    fn pull(
        &self,
        limit: u32,
        cursor: u64,
    ) -> Result<Window, Box<dyn std::error::Error + Send + Sync>>;
    fn ack(&self, up_to: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// The backend's full dataset, used by the weekly full sync and by
/// distributed updates from the center.
pub trait Records: Send + Sync {
    /// Which of the given ids the store does NOT have.
    fn check_ids(&self, ids: &[u64]) -> Result<Vec<u64>, Box<dyn std::error::Error + Send + Sync>>;

    /// Dump up to `limit` records starting at `cursor` (same window contract
    /// as [`ActionPool::pull`], but read-only — no ack).
    fn dump(
        &self,
        limit: u32,
        cursor: u64,
    ) -> Result<Window, Box<dyn std::error::Error + Send + Sync>>;

    /// Apply operations pushed from the center (`update`): upsert `EditTeam`
    /// by id, delete `DelTeam` ids. Must NOT re-enqueue into the pool
    /// (that would create a sync loop).
    fn apply(&self, ops: &[Operation]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SyncHandler {
    pool: Arc<dyn ActionPool>,
    records: Arc<dyn Records>,
}

impl SyncHandler {
    pub fn new(pool: Arc<dyn ActionPool>, records: Arc<dyn Records>) -> Self {
        Self { pool, records }
    }
}

impl sync_service::Server for SyncHandler {
    async fn update(
        self: capnp::capability::Rc<Self>,
        params: sync_service::UpdateParams,
        mut results: sync_service::UpdateResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let so = request.get_so()?;
        let ops = match parse_operations(so) {
            Ok(ops) => ops,
            Err(e) => {
                log::warn!("backend: update parse failed: {e}");
                return Err(capnp::Error::failed(e.to_string()));
            }
        };

        let result = results.get().init_result();
        let mut result = result;
        match self.records.apply(&ops.operations) {
            Ok(()) => result.set_status(0),
            Err(e) => {
                log::warn!("backend: update apply failed: {e}");
                result.set_status(1);
                result.set_meta(e.to_string());
            }
        }
        Ok(())
    }

    async fn pull(
        self: capnp::capability::Rc<Self>,
        params: sync_service::PullParams,
        mut results: sync_service::PullResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let limit = req.get_limit();
        let cursor = req.get_cursor();

        let window = match self.pool.pull(limit, cursor) {
            Ok(w) => w,
            Err(e) => {
                log::warn!("backend: pull failed: {e}");
                return Err(capnp::Error::failed(e.to_string()));
            }
        };
        guard_cursor("pull", cursor, &window)?;

        let mut result = results.get().init_result();
        fill_operations(result.reborrow().init_so(), &window.operations);
        result.set_next_cursor(window.next_cursor);
        Ok(())
    }

    async fn ack(
        self: capnp::capability::Rc<Self>,
        params: sync_service::AckParams,
        mut results: sync_service::AckResults,
    ) -> Result<(), capnp::Error> {
        let up_to = params.get()?.get_up_to();
        if let Err(e) = self.pool.ack(up_to) {
            log::warn!("backend: ack failed: {e}");
            return Err(capnp::Error::failed(e.to_string()));
        }
        results.get().init_result().set_status(0);
        Ok(())
    }

    async fn check_ids(
        self: capnp::capability::Rc<Self>,
        params: sync_service::CheckIdsParams,
        mut results: sync_service::CheckIdsResults,
    ) -> Result<(), capnp::Error> {
        let mut ids = Vec::new();
        for id in params.get()?.get_ids()? {
            ids.push(id);
        }

        let missing = match self.records.check_ids(&ids) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("backend: check_ids failed: {e}");
                return Err(capnp::Error::failed(e.to_string()));
            }
        };

        let mut result = results.get().init_result();
        result.reborrow().init_result().set_status(0);
        let mut list = result.reborrow().init_missing(missing.len() as u32);
        for (i, id) in missing.iter().enumerate() {
            list.set(i as u32, *id);
        }
        Ok(())
    }

    async fn dump(
        self: capnp::capability::Rc<Self>,
        params: sync_service::DumpParams,
        mut results: sync_service::DumpResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let limit = req.get_limit();
        let cursor = req.get_cursor();

        let window = match self.records.dump(limit, cursor) {
            Ok(w) => w,
            Err(e) => {
                log::warn!("backend: dump failed: {e}");
                return Err(capnp::Error::failed(e.to_string()));
            }
        };
        guard_cursor("dump", cursor, &window)?;

        let mut result = results.get().init_result();
        fill_operations(result.reborrow().init_so(), &window.operations);
        result.set_next_cursor(window.next_cursor);
        Ok(())
    }
}

fn guard_cursor(op: &str, cursor: u64, window: &Window) -> Result<(), capnp::Error> {
    if !window.operations.is_empty() && window.next_cursor <= cursor {
        log::warn!(
            "backend: {op}: non-advancing cursor {cursor} -> {}",
            window.next_cursor
        );
        return Err(capnp::Error::failed(format!(
            "{op}: non-advancing cursor {cursor} -> {}",
            window.next_cursor
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Serve loop
// ---------------------------------------------------------------------------

/// Bind a capnp `SyncService` server on `addr` with the backend handler
/// (pool + records) and serve forever.
pub async fn serve(
    addr: &str,
    pool: Arc<dyn ActionPool>,
    records: Arc<dyn Records>,
) -> Result<(), std::io::Error> {
    crate::server::serve(addr, SyncHandler::new(pool, records)).await
}
