//! Integration tests: backend server (in-memory pool/records) ↔ client,
//! plus the central distributor.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use capnp_sync::Error;
use capnp_sync::backend::{ActionPool, Records, serve};
use capnp_sync::client::SyncClient;
use capnp_sync::distributor::Distributor;
use capnp_sync::finder_capnp::sync_service;
use capnp_sync::ops::{Operation, SyncOperations, Team, Window};

const TIMEOUT: Duration = Duration::from_secs(5);

struct MemPool {
    ops: Mutex<Vec<(u64, Operation)>>,
}

impl ActionPool for MemPool {
    fn pull(
        &self,
        limit: u32,
        cursor: u64,
    ) -> Result<Window, Box<dyn std::error::Error + Send + Sync>> {
        let ops = self.ops.lock().unwrap();
        let mut out = Vec::new();
        let mut next = cursor;
        for (seq, op) in ops.iter() {
            if *seq >= cursor && out.len() < limit as usize {
                out.push(op.clone());
                next = *seq + 1;
            }
        }
        Ok(Window {
            operations: out,
            next_cursor: next,
        })
    }

    fn ack(&self, up_to: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.ops.lock().unwrap().retain(|(seq, _)| *seq >= up_to);
        Ok(())
    }
}

struct MemRecords {
    records: Mutex<Vec<(u64, Operation)>>,
    fail_apply: bool,
}

impl Records for MemRecords {
    fn check_ids(&self, ids: &[u64]) -> Result<Vec<u64>, Box<dyn std::error::Error + Send + Sync>> {
        let records = self.records.lock().unwrap();
        Ok(ids
            .iter()
            .filter(|id| {
                !records
                    .iter()
                    .any(|(_, op)| matches!(op, Operation::EditTeam(t) if t.id == **id))
            })
            .copied()
            .collect())
    }

    fn dump(
        &self,
        limit: u32,
        cursor: u64,
    ) -> Result<Window, Box<dyn std::error::Error + Send + Sync>> {
        let records = self.records.lock().unwrap();
        let mut out = Vec::new();
        let mut next = cursor;
        for (seq, op) in records.iter() {
            if *seq >= cursor && out.len() < limit as usize {
                out.push(op.clone());
                next = *seq + 1;
            }
        }
        Ok(Window {
            operations: out,
            next_cursor: next,
        })
    }

    fn apply(&self, ops: &[Operation]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_apply {
            return Err("apply failed on purpose".into());
        }
        let mut records = self.records.lock().unwrap();
        for op in ops {
            match op {
                Operation::EditTeam(team) => {
                    records
                        .retain(|(_, o)| !matches!(o, Operation::EditTeam(t) if t.id == team.id));
                    let seq = records.last().map(|(s, _)| s + 1).unwrap_or(0);
                    records.push((seq, Operation::EditTeam(team.clone())));
                }
                Operation::DelTeam(id) => {
                    records.retain(|(_, o)| !matches!(o, Operation::EditTeam(t) if t.id == *id));
                }
            }
        }
        Ok(())
    }
}

fn edit(id: u64, name: &str) -> Operation {
    Operation::EditTeam(Team {
        id,
        name: name.to_owned(),
        alt_names: vec![],
        avatar: vec![],
        banner: vec![],
    })
}

fn del(id: u64) -> Operation {
    Operation::DelTeam(id)
}

async fn serve_test(pool: Arc<dyn ActionPool>, records: Arc<dyn Records>) -> String {
    let probe = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", probe.local_addr().unwrap().port());
    drop(probe);

    let serve_addr = addr.clone();
    ntex::rt::spawn(async move {
        let _ = serve(&serve_addr, pool, records).await;
    });
    addr
}

#[test]
fn server_serve_with_custom_handler() {
    // the finder's pattern: a custom handler that is not pool/records based.
    run(async {
        let probe = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("127.0.0.1:{}", probe.local_addr().unwrap().port());
        drop(probe);

        let serve_addr = addr.clone();
        ntex::rt::spawn(async move {
            let _ = capnp_sync::server::serve(&serve_addr, OnlyUpdate).await;
        });

        let client = SyncClient::connect(&addr, TIMEOUT).await.unwrap();

        let r = client
            .update(&SyncOperations {
                operations: vec![edit(3, "C")],
            })
            .await
            .unwrap();
        assert_eq!(r.status, 0);

        // unimplemented methods on a custom handler surface as RPC errors
        let err = client.pull(10, 0).await.unwrap_err();
        assert!(matches!(err, Error::Rpc(_)), "got {err:?}");
    });
}

#[derive(Clone)]
struct OnlyUpdate;

impl sync_service::Server for OnlyUpdate {
    async fn update(
        self: capnp::capability::Rc<Self>,
        _params: sync_service::UpdateParams,
        mut results: sync_service::UpdateResults,
    ) -> Result<(), capnp::Error> {
        results.get().init_result().set_status(0);
        Ok(())
    }
}

fn run(fut: impl std::future::Future<Output = ()> + 'static) {
    ntex::rt::System::build()
        .build(ntex::rt::DefaultRuntime)
        .block_on(fut);
}

#[test]
fn pool_pull_ack_roundtrip() {
    run(async {
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![(0, edit(1, "A")), (1, edit(2, "B")), (2, del(3))]),
        });
        let records: Arc<dyn Records> = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: false,
        });
        let addr = serve_test(pool, records).await;

        let client = SyncClient::connect(&addr, TIMEOUT).await.unwrap();

        // window 1: ops 0..1 (limit 2)
        let w1 = client.pull(2, 0).await.unwrap();
        assert_eq!(w1.operations.len(), 2);
        assert_eq!(w1.next_cursor, 2);
        assert!(matches!(w1.operations[0], Operation::EditTeam(ref t) if t.id == 1));
        assert!(matches!(w1.operations[1], Operation::EditTeam(ref t) if t.id == 2));

        // ack up to 2 → first two ops gone
        let r = client.ack(2).await.unwrap();
        assert_eq!(r.status, 0);

        let w2 = client.pull(10, 2).await.unwrap();
        assert_eq!(w2.operations.len(), 1);
        assert_eq!(w2.next_cursor, 3);
        assert!(matches!(w2.operations[0], Operation::DelTeam(3)));

        let r = client.ack(3).await.unwrap();
        assert_eq!(r.status, 0);

        let w3 = client.pull(10, 3).await.unwrap();
        assert!(w3.operations.is_empty());
        assert_eq!(w3.next_cursor, 3);
    });
}

#[test]
fn records_check_ids_and_dump() {
    run(async {
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![]),
        });
        let records: Arc<dyn Records> = Arc::new(MemRecords {
            records: Mutex::new(vec![
                (0, edit(1, "A")),
                (1, edit(2, "B")),
                (2, edit(3, "C")),
            ]),
            fail_apply: false,
        });
        let addr = serve_test(pool, records).await;
        let client = SyncClient::connect(&addr, TIMEOUT).await.unwrap();

        let missing = client.check_ids(&[1, 3, 99]).await.unwrap();
        assert_eq!(missing, vec![99]);

        let w = client.dump(2, 0).await.unwrap();
        assert_eq!(w.operations.len(), 2);
        assert_eq!(w.next_cursor, 2);
        let w2 = client.dump(10, 2).await.unwrap();
        assert_eq!(w2.operations.len(), 1);
        assert_eq!(w2.next_cursor, 3);
    });
}

#[test]
fn update_applies_ops_to_records() {
    run(async {
        let records = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: false,
        });
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![]),
        });
        let addr = serve_test(pool, records.clone()).await;

        let client = SyncClient::connect(&addr, TIMEOUT).await.unwrap();

        let r = client
            .update(&SyncOperations {
                operations: vec![edit(7, "Seven")],
            })
            .await
            .unwrap();
        assert_eq!(r.status, 0);

        let store = records.records.lock().unwrap();
        assert_eq!(store.len(), 1);
        assert!(matches!(&store[0].1, Operation::EditTeam(t) if t.id == 7 && t.name == "Seven"));
    });
}

#[test]
fn update_failure_reports_status() {
    run(async {
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![]),
        });
        let records: Arc<dyn Records> = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: true,
        });
        let addr = serve_test(pool, records).await;

        let client = SyncClient::connect(&addr, TIMEOUT).await.unwrap();

        let r = client
            .update(&SyncOperations {
                operations: vec![edit(1, "A")],
            })
            .await
            .unwrap();
        assert_eq!(r.status, 1);
        assert!(r.meta.contains("apply failed"));
    });
}

#[test]
fn distributor_pushes_to_all_backends() {
    run(async {
        let records1 = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: false,
        });
        let records2 = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: false,
        });
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![]),
        });
        let addr1 = serve_test(pool.clone(), records1.clone()).await;
        let addr2 = serve_test(pool.clone(), records2.clone()).await;

        let distributor = Distributor::new(TIMEOUT);
        let results = distributor
            .distribute(
                &[addr1, addr2],
                &SyncOperations {
                    operations: vec![edit(5, "Five")],
                },
            )
            .await;

        assert_eq!(results.len(), 2);
        for res in &results {
            assert!(
                res.result.is_ok(),
                "backend {} failed: {:?}",
                res.backend,
                res.result
            );
        }
        assert!(
            matches!(&records1.records.lock().unwrap()[0].1, Operation::EditTeam(t) if t.id == 5)
        );
        assert!(
            matches!(&records2.records.lock().unwrap()[0].1, Operation::EditTeam(t) if t.id == 5)
        );
    });
}

#[test]
fn distributor_survives_dead_backend() {
    run(async {
        let records = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: false,
        });
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![]),
        });
        let addr = serve_test(pool, records.clone()).await;

        let distributor = Distributor::new(TIMEOUT);
        let results = distributor
            .distribute(
                &[addr.clone(), "127.0.0.1:1".to_owned()],
                &SyncOperations {
                    operations: vec![edit(9, "Nine")],
                },
            )
            .await;

        assert_eq!(results.len(), 2);
        assert!(results[0].result.is_ok());
        assert!(matches!(&results[1].result, Err(Error::Connect { .. })));
        // the healthy backend still got the change
        assert!(
            matches!(&records.records.lock().unwrap()[0].1, Operation::EditTeam(t) if t.id == 9)
        );
    });
}

#[test]
fn connections_closed_after_use() {
    run(async {
        let pool: Arc<dyn ActionPool> = Arc::new(MemPool {
            ops: Mutex::new(vec![]),
        });
        let records: Arc<dyn Records> = Arc::new(MemRecords {
            records: Mutex::new(vec![]),
            fail_apply: false,
        });
        let addr = serve_test(pool, records).await;

        // a few connect/update cycles; each peer must close its socket on drop
        let client = SyncClient::connect(&addr, TIMEOUT).await.unwrap();
        let r = client
            .update(&SyncOperations {
                operations: vec![edit(1, "A")],
            })
            .await
            .unwrap();
        assert_eq!(r.status, 0);
        drop(client);
        ntex::time::sleep(Duration::from_millis(200)).await;

        let client2 = SyncClient::connect(&addr, TIMEOUT).await.unwrap();
        let r = client2
            .update(&SyncOperations {
                operations: vec![edit(2, "B")],
            })
            .await
            .unwrap();
        assert_eq!(r.status, 0);
    });
}
