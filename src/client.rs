//! Client for any `SyncService` endpoint: the full method set used by the
//! worker, the finder and the central distributor.

use std::time::Duration;

use crate::error::Error;
use crate::finder_capnp::IdsTable;
use crate::ops::{SyncOperations, Window, fill_operations, parse_operations};
use crate::transport::{Peer, call_with_timeout};

/// RPC result from a `SyncService` method.
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub status: u8,
    pub meta: String,
}

pub struct SyncClient {
    peer: Peer,
    timeout: Duration,
}

impl SyncClient {
    /// Connect to a capnp `SyncService` endpoint. The connection is closed
    /// when the client is dropped.
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, Error> {
        log::debug!("client: connecting to {addr}");
        let peer = crate::transport::connect(addr, timeout).await?;
        log::debug!("client: connected to {addr}");
        Ok(Self { peer, timeout })
    }

    /// Push operations (upserts/deletes) to the peer.
    pub async fn update(&self, ops: &SyncOperations) -> Result<SyncResult, Error> {
        let mut req = self.peer.client().update_request();
        fill_operations(req.get().init_so(), &ops.operations);

        let resp = call_with_timeout("update", self.timeout, req.send().promise).await?;
        let result = resp.get()?.get_result()?;
        let status = result.get_status();
        let meta = result
            .get_meta()
            .map(|m| m.to_str().unwrap_or("").to_owned())
            .unwrap_or_default();

        log::debug!("client: update status={status} meta={meta}");
        Ok(SyncResult { status, meta })
    }

    /// Pull up to `limit` pending operations starting at `cursor`
    /// (backend action pool).
    pub async fn pull(&self, limit: u32, cursor: u64) -> Result<Window, Error> {
        let mut req = self.peer.client().pull_request();
        {
            let mut p = req.get().init_req();
            p.set_limit(limit);
            p.set_cursor(cursor);
        }

        let resp = call_with_timeout("pull", self.timeout, req.send().promise).await?;
        let result = resp.get()?.get_result()?;
        let next_cursor = result.get_next_cursor();
        let parsed = parse_operations(result.get_so()?)?;

        if !parsed.operations.is_empty() && next_cursor <= cursor {
            return Err(Error::Protocol(format!(
                "pull: non-advancing cursor {cursor} -> {next_cursor}"
            )));
        }

        log::debug!(
            "client: pull(cursor={cursor}, limit={limit}) -> next={next_cursor}, {} ops",
            parsed.operations.len()
        );
        Ok(Window {
            operations: parsed.operations,
            next_cursor,
        })
    }

    /// Confirm that everything up to `up_to` was synced; the backend deletes
    /// those operations from its pool.
    pub async fn ack(&self, up_to: u64) -> Result<SyncResult, Error> {
        let mut req = self.peer.client().ack_request();
        req.get().set_up_to(up_to);

        let resp = call_with_timeout("ack", self.timeout, req.send().promise).await?;
        let result = resp.get()?.get_result()?;
        let status = result.get_status();
        let meta = result
            .get_meta()
            .map(|m| m.to_str().unwrap_or("").to_owned())
            .unwrap_or_default();

        log::debug!("client: ack(up_to={up_to}) status={status} meta={meta}");
        Ok(SyncResult { status, meta })
    }

    /// Ask the peer which of `ids` it does NOT have (weekly reconcile).
    pub async fn check_ids(&self, ids: &[u64]) -> Result<Vec<u64>, Error> {
        let mut req = self.peer.client().check_ids_request();
        {
            let mut list = req.get().init_ids(ids.len() as u32);
            for (i, id) in ids.iter().enumerate() {
                list.set(i as u32, *id);
            }
        }

        let resp = call_with_timeout("check_ids", self.timeout, req.send().promise).await?;
        let result = resp.get()?.get_result()?;

        let status = result.get_result()?.get_status();
        if status != 0 {
            let meta = result
                .get_result()?
                .get_meta()
                .map(|m| m.to_str().unwrap_or("").to_owned())
                .unwrap_or_default();
            return Err(Error::Rejected {
                op: "check_ids",
                status,
                meta,
            });
        }

        let mut missing = Vec::new();
        for id in result.get_missing()? {
            missing.push(id);
        }
        log::debug!(
            "client: check_ids({} ids) -> {} missing",
            ids.len(),
            missing.len()
        );
        Ok(missing)
    }

    /// Dump all records in windows (read-only; no ack).
    pub async fn dump(&self, limit: u32, cursor: u64) -> Result<Window, Error> {
        let mut req = self.peer.client().dump_request();
        {
            let mut p = req.get().init_req();
            p.set_limit(limit);
            p.set_cursor(cursor);
        }

        let resp = call_with_timeout("dump", self.timeout, req.send().promise).await?;
        let result = resp.get()?.get_result()?;
        let next_cursor = result.get_next_cursor();
        let parsed = parse_operations(result.get_so()?)?;

        if !parsed.operations.is_empty() && next_cursor <= cursor {
            return Err(Error::Protocol(format!(
                "dump: non-advancing cursor {cursor} -> {next_cursor}"
            )));
        }

        log::debug!(
            "client: dump(cursor={cursor}, limit={limit}) -> next={next_cursor}, {} ops",
            parsed.operations.len()
        );
        Ok(Window {
            operations: parsed.operations,
            next_cursor,
        })
    }

    /// Page through the peer's index for `table` (0-based page index);
    /// empty vec when exhausted.
    pub async fn get_ids(&self, table: IdsTable, page: u64) -> Result<Vec<u64>, Error> {
        let mut req = self.peer.client().get_ids_request();
        {
            let mut p = req.get().init_ip();
            p.set_table(table);
            p.set_page(page);
        }

        let resp = call_with_timeout("get_ids", self.timeout, req.send().promise).await?;
        let result = resp.get()?.get_result()?;

        let status = result.get_result()?.get_status();
        if status != 0 {
            let meta = result
                .get_result()?
                .get_meta()
                .map(|m| m.to_str().unwrap_or("").to_owned())
                .unwrap_or_default();
            return Err(Error::Rejected {
                op: "get_ids",
                status,
                meta,
            });
        }

        let mut ids = Vec::new();
        for id in result.get_id_list()? {
            ids.push(id);
        }
        log::debug!(
            "client: get_ids(table={table:?}, page={page}) -> {} ids",
            ids.len()
        );
        Ok(ids)
    }
}
