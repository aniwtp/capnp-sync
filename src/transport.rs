//! Cap'n Proto transport: client connection (`Peer`) with close-on-drop,
//! plus a timeout wrapper shared by all RPC calls.

use std::time::Duration;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use compio_io::compat::AsyncStream;
use futures::AsyncReadExt;

use crate::error::Error;
use crate::finder_capnp::sync_service;

/// A live capnp connection: the bootstrapped client plus the RPC task that
/// drives it. Dropping the peer cancels the task and closes the socket, so
/// connections do not accumulate.
pub(crate) struct Peer {
    client: sync_service::Client,
    rpc_task: Option<ntex::rt::JoinHandle<()>>,
}

impl Peer {
    pub(crate) fn client(&self) -> &sync_service::Client {
        &self.client
    }

    fn close(&mut self) {
        if let Some(task) = self.rpc_task.take() {
            task.cancel();
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.close();
    }
}

/// Connect to a capnp `SyncService` endpoint and bootstrap the remote
/// interface. Must be called on the ntex/compio runtime.
pub(crate) async fn connect(addr: &str, timeout: Duration) -> Result<Peer, Error> {
    let stream = ntex::time::timeout(timeout, compio_net::TcpStream::connect(addr))
        .await
        .map_err(|_| Error::Timeout("connect"))?
        .map_err(|source| Error::Connect {
            addr: addr.to_owned(),
            source,
        })?;

    stream.set_nodelay(true).map_err(|source| Error::Connect {
        addr: addr.to_owned(),
        source,
    })?;

    let (reader, writer) = AsyncStream::new(stream).split();

    let network = Box::new(twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));

    let mut rpc_system = RpcSystem::new(network, None);
    let client: sync_service::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    let rpc_task = ntex::rt::spawn(async move {
        if let Err(e) = rpc_system.await {
            log::warn!("capnp: rpc system ended with error: {e}");
        }
    });

    Ok(Peer {
        client,
        rpc_task: Some(rpc_task),
    })
}

/// Wrap an RPC call in a timeout.
pub(crate) async fn call_with_timeout<T>(
    op: &'static str,
    timeout: Duration,
    fut: impl std::future::Future<Output = Result<T, capnp::Error>>,
) -> Result<T, Error> {
    let res = ntex::time::timeout(timeout, fut)
        .await
        .map_err(|_| Error::Timeout(op))?;
    res.map_err(Error::Rpc)
}
