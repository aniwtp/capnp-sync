# capnp-sync

Общий крейт синхронизации для бекендов и центральной раздачи изменений.
Один протокол (`capnp/finder.capnp`) для всех участников: финдер, воркер,
бекенды.

## Состав

| Модуль | Назначение |
|--------|-----------|
| `backend` | Серверная часть для бекендов: трейты `ActionPool`/`Records` + готовый `SyncService`-хендлер и `serve()` |
| `server` | Общий серверный транспорт: `server::serve(addr, handler)` для любого кастомного `SyncService`-хендлера (финдер и т.п.) |
| `client::SyncClient` | Полный клиент: `update`/`pull`/`ack`/`check_ids`/`dump`/`get_ids` |
| `distributor` | Централизованная раздача изменений (`update`) по списку бекендов |
| `ops` | Домен (`Team`, `Operation`, `SyncOperations`, `Window`) + конверсии capnp |
| `finder_capnp` | Сгенерированные биндинги из `capnp/finder.capnp` — единая схема для всех |

Транспорт: compio TCP + two-party capnp-rpc на ntex/compio рантайме
(как остальные сервисы). Все вызовы ограничены таймаутом; соединения
закрываются при дропе клиента. `capnp` ре-экспортируется (`capnp_sync::capnp`).

## Бекенд: реализовать два трейта и слушать

```rust
use std::sync::Arc;
use capnp_sync::backend::{ActionPool, Records, serve};
use capnp_sync::ops::{Operation, Window};

struct MyPool { /* ваше хранилище пула действий */ }
impl ActionPool for MyPool {
    fn pull(&self, limit: u32, cursor: u64) -> Result<Window, Box<dyn std::error::Error + Send + Sync>> { /* ... */ }
    fn ack(&self, up_to: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { /* ... */ }
}

struct MyRecords { /* ваше хранилище записей */ }
impl Records for MyRecords {
    fn check_ids(&self, ids: &[u64]) -> Result<Vec<u64>, Box<dyn std::error::Error + Send + Sync>> { /* ... */ }
    fn dump(&self, limit: u32, cursor: u64) -> Result<Window, Box<dyn std::error::Error + Send + Sync>> { /* ... */ }
    fn apply(&self, ops: &[Operation]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { /* ... */ }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    let pool: Arc<dyn ActionPool> = Arc::new(MyPool::new());
    let records: Arc<dyn Records> = Arc::new(MyRecords::new());
    ntex::rt::spawn(async move { serve("0.0.0.0:9091", pool, records).await });
    // ... остальной сервис
    Ok(())
}
```

`pull`/`ack`/`checkIds`/`dump`/`update` работают сразу. `update` применяет
присланные с центра операции через `Records::apply` (апсерт `EditTeam` по
id, удаление `DelTeam`) и отвечает status 0, либо status 1 + meta при
ошибке. `getIds` на бекенде не реализован (это метод финдера) — вернёт
unimplemented.

### Контракт окон (pull/dump)

Операции несут монотонный `seq` (порядок добавления). `cursor` — первый
неподтверждённый seq (включительно); `next_cursor` = последний отданный
seq + 1 (или входной `cursor`, если пусто). `ack(upTo)` удаляет
`seq < upTo`. Сервер отклоняет непродвигающийся курсор (`Protocol`).

## Центральная раздача изменений

```rust
let distributor = Distributor::new(Duration::from_secs(30));
let results = distributor.distribute(
    &["10.0.0.2:9091".to_owned(), "10.0.0.3:9091".to_owned()],
    &SyncOperations { operations: vec![Operation::EditTeam(team)] },
).await;
// results: Vec<DistributeResult> — по одному на бекенд, один сбой не
// останавливает остальных
```

## Клиент (для воркера/финдера)

```rust
let client = SyncClient::connect("127.0.0.1:9090", Duration::from_secs(30)).await?;
let w = client.pull(100, 0).await?;        // окно пула
client.ack(w.next_cursor).await?;          // подтвердить
let missing = client.check_ids(&ids).await?; // кого нет на беке
```

## Тесты

```sh
cargo test    # 8 интеграционных тестов: roundtrip, окна, отказы,
              # distributor (включая мёртвый бекенд), кастомный хендлер,
              # закрытие соединений
```

## Интеграция

- **Воркер** (`finder_worker`) — оркестрация поверх `SyncClient`
  (drain + weekly full sync), своя логика в `logic/sync.rs`.
- **Финдер** (`finder`) — сервер: `capnp_sync::server::serve` +
  собственный `SyncHandler` (`update`/`getIds` поверх tantivy).
- **Бекенды** — `backend::serve` + два трейта (см. выше).

Схема `capnp/finder.capnp` — единственный источник истины в этом крейте;
воркер и финдер больше не держат свои копии.

## Замечания

- Крейт требует ntex/compio рантайм (spawn + time) — вызывать `serve` и
  `SyncClient` из кода на `#[ntex::main]`.
- `ntex` запинен точно (`=3.10.1`): свежий `ntex-server 3.11.0` не
  собирается; Cargo.lock скопирован с проверенной сборки воркера.
