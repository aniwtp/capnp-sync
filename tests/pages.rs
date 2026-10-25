//! PageStore: сервер с бекендом в памяти ↔ клиент (upload/claim/release, отказы).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use capnp_sync::pages::{PageBackend, PageError, PageUpload, PagesClient, StoredPage, serve};

const TIMEOUT: Duration = Duration::from_secs(5);

/// Запись страницы в памяти.
#[derive(Clone)]
struct Entry {
    team: u64,
    done: bool,
    released: bool,
    mini: Vec<u8>,
}

/// token → запись
#[derive(Default)]
struct Mem(Mutex<HashMap<String, Entry>>);

impl PageBackend for Mem {
    fn upload(&self, page: PageUpload) -> Result<StoredPage, PageError> {
        if page.data.first() != Some(&0xFF) {
            return Err(PageError::Invalid("not an image".into()));
        }
        let mut m = self.0.lock().unwrap();
        let token = format!("tok{:09}", m.len());
        m.insert(
            token.clone(),
            Entry {
                team: page.team_id,
                done: false,
                released: false,
                mini: page.mini,
            },
        );
        Ok(StoredPage {
            token,
            hash: vec![7; 16],
            width: page.data.len() as u32,
            height: page.title_id,
        })
    }

    fn claim(&self, tokens: &[String], team_id: u64) -> Result<(), PageError> {
        let mut m = self.0.lock().unwrap();
        let bad: Vec<String> = tokens
            .iter()
            .filter(|t| m.get(*t).is_none_or(|e| e.team != team_id || e.released))
            .cloned()
            .collect();
        if !bad.is_empty() {
            return Err(PageError::BadTokens(bad));
        }
        for t in tokens {
            m.get_mut(t).unwrap().done = true;
        }
        Ok(())
    }

    fn release(&self, tokens: &[String], team_id: u64) -> Result<(), PageError> {
        let mut m = self.0.lock().unwrap();
        for t in tokens {
            if let Some(e) = m.get_mut(t).filter(|e| e.team == team_id) {
                e.released = true;
            }
        }
        Ok(())
    }
}

fn run(fut: impl std::future::Future<Output = ()> + 'static) {
    ntex::rt::System::build()
        .build(ntex::rt::DefaultRuntime)
        .block_on(fut);
}

#[test]
fn upload_claim_release_roundtrip() {
    run(async {
        let probe = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("127.0.0.1:{}", probe.local_addr().unwrap().port());
        drop(probe);
        let mem = Arc::new(Mem::default());
        let backend: Arc<dyn PageBackend> = mem.clone();
        let serve_addr = addr.clone();
        ntex::rt::spawn(async move {
            let _ = serve(&serve_addr, backend).await;
        });
        ntex::time::sleep(Duration::from_millis(50)).await;

        let client = PagesClient::connect(&addr, TIMEOUT).await.unwrap();
        let page = |byte: u8| PageUpload {
            data: vec![byte; 3 * 1024 * 1024],
            user_id: 1,
            team_id: 5,
            title_id: 9,
            web: byte == 0xFF,
            mini: if byte == 0xFF {
                vec![1, 2, 3]
            } else {
                Vec::new()
            },
        };

        // большая страница доходит целиком, миниатюра — тоже
        let a = client.upload(&page(0xFF)).await.unwrap().unwrap();
        assert_eq!((a.width, a.height, a.hash.len()), (3 * 1024 * 1024, 9, 16));
        assert_eq!(mem.0.lock().unwrap()[&a.token].mini, vec![1, 2, 3]);
        let b = client.upload(&page(0xFF)).await.unwrap().unwrap();
        assert!(matches!(
            client.upload(&page(0)).await.unwrap(),
            Err(PageError::Invalid(_))
        ));

        // пачка: порядок результатов совпадает с порядком запроса, плохая страница не ломает остальные
        let batch = client.upload_many(&[page(0xFF), page(0), page(0xFF)]).await;
        assert_eq!(batch.len(), 3);
        assert!(batch[0].as_ref().unwrap().is_ok());
        assert!(matches!(
            batch[1].as_ref().unwrap(),
            Err(PageError::Invalid(_))
        ));
        assert!(batch[2].as_ref().unwrap().is_ok());
        assert_ne!(
            batch[0].as_ref().unwrap().as_ref().unwrap().token,
            batch[2].as_ref().unwrap().as_ref().unwrap().token
        );

        // чужая команда и неизвестный токен — отказ без изменений
        let err = client
            .claim(&[a.token.clone(), "nope".into()], 5)
            .await
            .unwrap();
        assert_eq!(err, Err(PageError::BadTokens(vec!["nope".into()])));
        assert_eq!(
            client
                .claim(std::slice::from_ref(&a.token), 6)
                .await
                .unwrap(),
            Err(PageError::BadTokens(vec![a.token.clone()]))
        );
        assert!(!mem.0.lock().unwrap()[&a.token].done);

        assert_eq!(
            client
                .claim(std::slice::from_ref(&a.token), 5)
                .await
                .unwrap(),
            Ok(())
        );
        assert_eq!(
            client
                .release(&[b.token.clone(), "nope".into()], 5)
                .await
                .unwrap(),
            Ok(())
        );
        let m = mem.0.lock().unwrap();
        assert_eq!((m[&a.token].done, m[&b.token].released), (true, true));
    });
}
