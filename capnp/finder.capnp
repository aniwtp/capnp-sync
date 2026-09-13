@0xf10049e114f808c7;

struct Team {
  id @0 :UInt64;
  name @1 :Text;
  altNames @2 :List(Text);
  avatar @3 :Data;
  banner @4 :Data;
}

struct Credit {
  id @0 :UInt64;
  name @1 :Text;
  altNames @2 :List(Text);
  avatar @3 :Data;
  banner @4 :Data;
}

struct Title {
  id @0 :UInt64;
  name @1 :Text;
  altNames @2 :List(Text);
  teams @3 :List(UInt64);
  avatar @4 :Data;
  banner @5 :Data;
  typeComics @6 :UInt8;
  statusRelease @7 :UInt8;
  statusTranslate @8 :UInt8;
}

struct User {
  id @0 :UInt64;
  name @1 :Text;
}

struct Operation {
  union {
    editTeam @0 :Team;
    delTeam @1 :UInt64;
    editCredit @2 :Credit;
    delCredit @3 :UInt64;
    editTitle @4 :Title;
    delTitle @5 :UInt64;
    editUser @6 :User;
    delUser @7 :UInt64;
  }
}

struct SyncOperations {
  operations @0 :List(Operation);
}

struct Result {
  status @0 :UInt8;
  meta @1 :Text;
}

enum IdsTable {
  team @0;
  credits @1;
  titles @2;
  users @3;
}

struct IdsPayload {
  table @0 :IdsTable;
  page @1 :UInt64;
}

struct IdsResult {
  result @0 :Result;
  idList @1 :List(UInt64);
}

# Backend-side pull window. Pool ops carry monotonic seq numbers (enqueue
# order). `cursor` is the first unconfirmed seq (inclusive); `limit` bounds
# one response. `nextCursor` = last returned seq + 1, to pass back as
# `cursor` and as `upTo` for `ack` (which deletes seq < upTo).
struct PullRequest {
  limit @0 :UInt32;
  cursor @1 :UInt64;
}

struct PullResult {
  so @0 :SyncOperations;
  nextCursor @1 :UInt64;
}

# Backend-side existence check: which of the given ids the backend does NOT
# have (used by the weekly full sync to prune stale ids from the finder).
struct CheckIdsResult {
  result @0 :Result;
  missing @1 :List(UInt64);
}

interface SyncService {
  update @0 (so :SyncOperations) -> (result :Result);
  getIds @1 (ip :IdsPayload) -> (result :IdsResult);
  pull @2 (req :PullRequest) -> (result :PullResult);
  ack @3 (upTo :UInt64) -> (result :Result);
  checkIds @4 (ids :List(UInt64)) -> (result :CheckIdsResult);
  dump @5 (req :PullRequest) -> (result :PullResult);
}
