//! Error types for the sync toolkit.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connect {addr}: {source}")]
    Connect {
        addr: String,
        source: std::io::Error,
    },

    #[error("timeout: {0}")]
    Timeout(&'static str),

    #[error("rpc: {0}")]
    Rpc(#[from] capnp::Error),

    #[error("not in schema: {0}")]
    NotInSchema(#[from] capnp::NotInSchema),

    #[error("utf8: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("{op} rejected: status={status} meta={meta}")]
    Rejected {
        op: &'static str,
        status: u8,
        meta: String,
    },

    #[error("protocol: {0}")]
    Protocol(String),
}
