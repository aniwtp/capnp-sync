//! capnp-sync — общий крейт синхронизации.
//!
//! Две половины одного протокола (`capnp/finder.capnp`, единая схема для
//! финдера, воркера и бекендов):
//!
//! - [`backend`] — серверная часть для бекендов: трейты [`ActionPool`] и
//!   [`Records`] + готовый `SyncService`-хендлер и TCP-serve. Бекенд
//!   реализует два трейта поверх своего хранилища и получает
//!   `pull`/`ack`/`checkIds`/`dump`/`update` целиком.
//! - [`distributor`] — клиент для централизованной раздачи изменений
//!   (`update`) по списку capnp-бекендов. По одному адресу — [`SyncClient`].

pub mod backend;
pub mod client;
pub mod distributor;
pub mod error;
pub mod finder_capnp;
pub mod ops;
pub mod server;
mod transport;

pub use error::Error;
pub use ops::{Operation, SyncOperations, Team, Window};
pub use capnp;
