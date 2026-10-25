//! Страницы глав по capnp (`capnp/pages.capnp`): chapter проверяет права на тайтл
//! и проксирует страницы в uploader, uploader хранит.
//!
//! - сервер: uploader реализует [`PageBackend`] (блокирующие методы — S3/redb) и
//!   запускает [`serve`]; каждый вызов уезжает в `ntex::rt::spawn_blocking`;
//! - клиент: [`PagesClient`] — соединение на запрос, закрывается при drop.

use std::sync::Arc;
use std::time::Duration;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use compio_io::compat::AsyncStream;
use futures::AsyncReadExt;

use crate::error::Error;
use crate::pages_capnp::page_store;
use crate::transport::call_with_timeout;

/// Страница для заливки.
#[derive(Debug, Clone)]
pub struct PageUpload {
    pub data: Vec<u8>,
    pub user_id: u64,
    pub team_id: u64,
    pub title_id: u32,
    /// Часть страницы вебтуна (свои пределы габаритов), иначе страница манги.
    pub web: bool,
    /// Готовая миниатюра (пусто — миниатюры нет, uploader строит сам).
    pub mini: Vec<u8>,
}

/// Залитая страница: ключ объекта, плейсхолдер и габариты.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPage {
    pub token: String,
    pub hash: Vec<u8>,
    pub width: u32,
    pub height: u32,
}

/// Отказ бекенда страниц.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PageError {
    /// Плохие данные запроса: не изображение, битая шапка, габариты (HTTP 400).
    #[error("invalid: {0}")]
    Invalid(String),
    /// Токены неизвестны, не страницы или чужой команды (HTTP 400/403).
    #[error("bad tokens: {0:?}")]
    BadTokens(Vec<String>),
    /// Сбой хранилища/БД (HTTP 500).
    #[error("failed: {0}")]
    Failed(String),
}

/// Хранилище страниц (uploader). Методы блокирующие — сервер зовёт их из пула потоков.
pub trait PageBackend: Send + Sync + 'static {
    fn upload(&self, page: PageUpload) -> Result<StoredPage, PageError>;
    /// `pending` → `done`; все или ничего.
    fn claim(&self, tokens: &[String], team_id: u64) -> Result<(), PageError>;
    /// Пометить `released`; неизвестные токены пропускаются.
    fn release(&self, tokens: &[String], team_id: u64) -> Result<(), PageError>;
}

#[derive(Clone)]
struct Handler(Arc<dyn PageBackend>);

fn read_tokens(list: capnp::text_list::Reader<'_>) -> Result<Vec<String>, capnp::Error> {
    let mut out = Vec::with_capacity(list.len() as usize);
    for t in list {
        out.push(
            t?.to_str()
                .map_err(|e| capnp::Error::failed(e.to_string()))?
                .to_owned(),
        );
    }
    Ok(out)
}

async fn blocking<R: Send + 'static>(
    f: impl FnOnce() -> Result<R, PageError> + Send + 'static,
) -> Result<R, PageError> {
    match ntex::rt::spawn_blocking(f).await {
        Ok(r) => r,
        Err(e) => Err(PageError::Failed(format!("blocking task: {e:?}"))),
    }
}

fn fill_result(mut r: crate::pages_capnp::pages_result::Builder<'_>, res: &Result<(), PageError>) {
    match res {
        Ok(()) => r.set_status(0),
        Err(PageError::BadTokens(bad)) => {
            r.set_status(1);
            r.set_error("bad tokens");
            let mut list = r.init_bad(bad.len() as u32);
            for (i, t) in bad.iter().enumerate() {
                list.set(i as u32, t.as_str());
            }
        }
        Err(PageError::Invalid(e)) => {
            r.set_status(1);
            r.set_error(e.as_str());
        }
        Err(PageError::Failed(e)) => {
            r.set_status(2);
            r.set_error(e.as_str());
        }
    }
}

impl page_store::Server for Handler {
    async fn upload(
        self: capnp::capability::Rc<Self>,
        params: page_store::UploadParams,
        mut results: page_store::UploadResults,
    ) -> Result<(), capnp::Error> {
        let p = params.get()?.get_page()?;
        let page = PageUpload {
            data: p.get_data()?.to_vec(),
            user_id: p.get_user_id(),
            team_id: p.get_team_id(),
            title_id: p.get_title_id(),
            web: p.get_web(),
            mini: p.get_mini()?.to_vec(),
        };
        let backend = self.0.clone();
        let res = blocking(move || backend.upload(page)).await;
        let mut r = results.get().init_result();
        match res {
            Ok(stored) => {
                r.set_status(0);
                r.set_token(stored.token.as_str());
                r.set_hash(&stored.hash);
                r.set_width(stored.width);
                r.set_height(stored.height);
            }
            Err(PageError::Failed(e)) => {
                log::warn!("pages: upload failed: {e}");
                r.set_status(2);
                r.set_error(e.as_str());
            }
            Err(e) => {
                r.set_status(1);
                r.set_error(e.to_string().as_str());
            }
        }
        Ok(())
    }

    async fn claim(
        self: capnp::capability::Rc<Self>,
        params: page_store::ClaimParams,
        mut results: page_store::ClaimResults,
    ) -> Result<(), capnp::Error> {
        let p = params.get()?;
        let tokens = read_tokens(p.get_tokens()?)?;
        let team_id = p.get_team_id();
        let backend = self.0.clone();
        let res = blocking(move || backend.claim(&tokens, team_id)).await;
        fill_result(results.get().init_result(), &res);
        Ok(())
    }

    async fn release(
        self: capnp::capability::Rc<Self>,
        params: page_store::ReleaseParams,
        mut results: page_store::ReleaseResults,
    ) -> Result<(), capnp::Error> {
        let p = params.get()?;
        let tokens = read_tokens(p.get_tokens()?)?;
        let team_id = p.get_team_id();
        let backend = self.0.clone();
        let res = blocking(move || backend.release(&tokens, team_id)).await;
        fill_result(results.get().init_result(), &res);
        Ok(())
    }
}

/// Лимит чтения сообщения: страница до 32 MiB плюс запас (по умолчанию capnp — 64 MiB).
fn reader_options() -> capnp::message::ReaderOptions {
    let mut o = capnp::message::ReaderOptions::new();
    o.traversal_limit_in_words(Some(48 * 1024 * 1024 / 8));
    o
}

/// Поднять `PageStore` на `addr` и обслуживать вечно (ntex/compio-рантайм).
pub async fn serve(addr: &str, backend: Arc<dyn PageBackend>) -> Result<(), std::io::Error> {
    let listener = compio_net::TcpListener::bind(addr).await?;
    log::info!("capnp pages server listening on {addr}");
    let handler = Handler(backend);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                log::warn!("capnp pages: accept error on {addr}: {e}; retrying");
                ntex::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        stream.set_nodelay(true)?;
        let (reader, writer) = AsyncStream::new(stream).split();
        let network = Box::new(twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Server,
            reader_options(),
        ));
        let server: page_store::Client = capnp_rpc::new_client(handler.clone());
        let rpc_system = RpcSystem::new(network, Some(server.client));
        ntex::rt::spawn(async move {
            match rpc_system.await {
                Ok(()) => log::debug!("capnp pages: disconnected {peer}"),
                Err(e) => log::warn!("capnp pages: rpc error ({peer}): {e}"),
            }
        });
    }
}

/// Клиент `PageStore`. Соединение закрывается при drop.
pub struct PagesClient {
    client: page_store::Client,
    rpc_task: Option<ntex::rt::JoinHandle<()>>,
    timeout: Duration,
}

impl Drop for PagesClient {
    fn drop(&mut self) {
        if let Some(task) = self.rpc_task.take() {
            task.cancel();
        }
    }
}

fn pages_result(
    r: crate::pages_capnp::pages_result::Reader<'_>,
) -> Result<Result<(), PageError>, Error> {
    let error = r
        .get_error()
        .map(|m| m.to_str().unwrap_or("").to_owned())
        .unwrap_or_default();
    Ok(match r.get_status() {
        0 => Ok(()),
        1 => {
            let bad = read_tokens(r.get_bad()?)?;
            if bad.is_empty() {
                Err(PageError::Invalid(error))
            } else {
                Err(PageError::BadTokens(bad))
            }
        }
        _ => Err(PageError::Failed(error)),
    })
}

impl PagesClient {
    /// Подключиться к `PageStore` (на ntex/compio-рантайме).
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, Error> {
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
            reader_options(),
        ));
        let mut rpc_system = RpcSystem::new(network, None);
        let client: page_store::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
        let rpc_task = ntex::rt::spawn(async move {
            if let Err(e) = rpc_system.await {
                log::warn!("capnp pages: rpc system ended with error: {e}");
            }
        });
        Ok(Self {
            client,
            rpc_task: Some(rpc_task),
            timeout,
        })
    }

    /// Залить страницу. Внешняя ошибка — связь/протокол, внутренняя — ответ бекенда.
    pub async fn upload(&self, page: &PageUpload) -> Result<Result<StoredPage, PageError>, Error> {
        let mut req = self.client.upload_request();
        {
            let mut p = req.get().init_page();
            p.set_data(&page.data);
            p.set_user_id(page.user_id);
            p.set_team_id(page.team_id);
            p.set_title_id(page.title_id);
            p.set_web(page.web);
            p.set_mini(&page.mini);
        }
        let resp = call_with_timeout("upload", self.timeout, req.send().promise).await?;
        let r = resp.get()?.get_result()?;
        let error = r
            .get_error()
            .map(|m| m.to_str().unwrap_or("").to_owned())
            .unwrap_or_default();
        Ok(match r.get_status() {
            0 => Ok(StoredPage {
                token: r.get_token()?.to_str()?.to_owned(),
                hash: r.get_hash()?.to_vec(),
                width: r.get_width(),
                height: r.get_height(),
            }),
            1 => Err(PageError::Invalid(error)),
            _ => Err(PageError::Failed(error)),
        })
    }

    /// Залить пачку страниц одним соединением: запросы идут одновременно (uploader разбирает их в пуле потоков),
    /// результаты — в порядке `pages`. Сбой одной не мешает остальным.
    pub async fn upload_many(
        &self,
        pages: &[PageUpload],
    ) -> Vec<Result<Result<StoredPage, PageError>, Error>> {
        futures::future::join_all(pages.iter().map(|p| self.upload(p))).await
    }

    /// Закрепить страницы за главой (`pending` → `done`).
    pub async fn claim(
        &self,
        tokens: &[String],
        team_id: u64,
    ) -> Result<Result<(), PageError>, Error> {
        let mut req = self.client.claim_request();
        {
            let mut p = req.get();
            p.set_team_id(team_id);
            let mut list = p.init_tokens(tokens.len() as u32);
            for (i, t) in tokens.iter().enumerate() {
                list.set(i as u32, t.as_str());
            }
        }
        let resp = call_with_timeout("claim", self.timeout, req.send().promise).await?;
        pages_result(resp.get()?.get_result()?)
    }

    /// Освободить страницы (удалит GC uploader).
    pub async fn release(
        &self,
        tokens: &[String],
        team_id: u64,
    ) -> Result<Result<(), PageError>, Error> {
        let mut req = self.client.release_request();
        {
            let mut p = req.get();
            p.set_team_id(team_id);
            let mut list = p.init_tokens(tokens.len() as u32);
            for (i, t) in tokens.iter().enumerate() {
                list.set(i as u32, t.as_str());
            }
        }
        let resp = call_with_timeout("release", self.timeout, req.send().promise).await?;
        pages_result(resp.get()?.get_result()?)
    }
}
