//! Generic capnp `SyncService` server transport: bind, accept, and serve
//! any [`sync_service::Server`] implementation (backends via
//! [`crate::backend`], the finder via its own handler, …).
//!
//! Runs on the current ntex/compio runtime; each connection is handled in a
//! spawned task. Transient accept errors are logged and retried with a small
//! backoff instead of killing the server.

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use compio_io::compat::AsyncStream;
use futures::AsyncReadExt;

use crate::finder_capnp::sync_service;

const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// Bind a capnp `SyncService` server on `addr` and serve forever.
pub async fn serve<S>(addr: &str, handler: S) -> Result<(), std::io::Error>
where
    S: sync_service::Server + Clone + 'static,
{
    let listener = compio_net::TcpListener::bind(addr).await?;
    log::info!("capnp sync server listening on {addr}");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                log::warn!("capnp: accept error on {addr}: {e}; retrying");
                ntex::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        stream.set_nodelay(true)?;
        log::debug!("capnp: new connection from {peer}");

        let (reader, writer) = AsyncStream::new(stream).split();

        let network = Box::new(twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        ));

        let server: sync_service::Client = capnp_rpc::new_client(handler.clone());
        let rpc_system = RpcSystem::new(network, Some(server.client));

        ntex::rt::spawn(async move {
            match rpc_system.await {
                Ok(()) => log::debug!("capnp: disconnected {peer}"),
                Err(e) => log::warn!("capnp: rpc error ({peer}): {e}"),
            }
        });
    }
}
