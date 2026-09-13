//! Domain model for synced operations plus capnp conversions.
//!
//! Content-type agnostic: new content types extend [`Operation`] with new
//! variants (and the capnp schema with matching union arms).

use crate::error::Error;
use crate::finder_capnp::operation::Which;
use crate::finder_capnp::sync_operations;

#[derive(Debug, Clone)]
pub struct Team {
    pub id: u64,
    pub name: String,
    pub alt_names: Vec<String>,
    pub avatar: Vec<u8>,
    pub banner: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Credit {
    pub id: u64,
    pub name: String,
    pub alt_names: Vec<String>,
    pub avatar: Vec<u8>,
    pub banner: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Title {
    pub id: u64,
    pub name: String,
    pub alt_names: Vec<String>,
    pub teams: Vec<u64>,
    pub avatar: Vec<u8>,
    pub banner: Vec<u8>,
    pub type_comics: u8,
    pub status_release: u8,
    pub status_translate: u8,
}

#[derive(Debug, Clone)]
pub struct User {
    pub id: u64,
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum Operation {
    EditTeam(Team),
    DelTeam(u64),
    EditCredit(Credit),
    DelCredit(u64),
    EditTitle(Title),
    DelTitle(u64),
    EditUser(User),
    DelUser(u64),
}

#[derive(Debug, Clone, Default)]
pub struct SyncOperations {
    pub operations: Vec<Operation>,
}

impl SyncOperations {
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

/// One bounded window over a pool (`pull`) or a records store (`dump`).
///
/// `next_cursor` follows the shared contract: it is the cursor to pass back
/// on the next call (and as `up_to` to [`crate::client::SyncClient::ack`]).
#[derive(Debug, Clone)]
pub struct Window {
    pub operations: Vec<Operation>,
    pub next_cursor: u64,
}

/// Fill a `SyncOperations` builder from a slice of operations.
pub fn fill_operations(builder: sync_operations::Builder<'_>, ops: &[Operation]) {
    let mut list = builder.init_operations(ops.len() as u32);
    for (i, op) in ops.iter().enumerate() {
        let mut item = list.reborrow().get(i as u32);
        match op {
            Operation::EditTeam(team) => {
                let mut t = item.init_edit_team();
                t.set_id(team.id);
                t.set_name(team.name.as_str());
                let mut alt = t.reborrow().init_alt_names(team.alt_names.len() as u32);
                for (j, name) in team.alt_names.iter().enumerate() {
                    alt.set(j as u32, name.as_str());
                }
                t.set_avatar(&team.avatar);
                t.set_banner(&team.banner);
            }
            Operation::DelTeam(id) => {
                item.set_del_team(*id);
            }
            Operation::EditCredit(credit) => {
                let mut c = item.init_edit_credit();
                c.set_id(credit.id);
                c.set_name(credit.name.as_str());
                let mut alt = c.reborrow().init_alt_names(credit.alt_names.len() as u32);
                for (j, name) in credit.alt_names.iter().enumerate() {
                    alt.set(j as u32, name.as_str());
                }
                c.set_avatar(&credit.avatar);
            }
            Operation::DelCredit(id) => {
                item.set_del_credit(*id);
            }
            Operation::EditTitle(title) => {
                let mut t = item.init_edit_title();
                t.set_id(title.id);
                t.set_name(title.name.as_str());
                let mut alt = t.reborrow().init_alt_names(title.alt_names.len() as u32);
                for (j, name) in title.alt_names.iter().enumerate() {
                    alt.set(j as u32, name.as_str());
                }
                let mut teams = t.reborrow().init_teams(title.teams.len() as u32);
                for (j, team) in title.teams.iter().enumerate() {
                    teams.set(j as u32, *team);
                }
                t.set_avatar(&title.avatar);
                t.set_banner(&title.banner);
                t.set_type_comics(title.type_comics);
                t.set_status_release(title.status_release);
                t.set_status_translate(title.status_translate);
            }
            Operation::DelTitle(id) => {
                item.set_del_title(*id);
            }
            Operation::EditUser(user) => {
                let mut u = item.init_edit_user();
                u.set_id(user.id);
                u.set_name(user.name.as_str());
            }
            Operation::DelUser(id) => {
                item.set_del_user(*id);
            }
        }
    }
}

/// Parse a `SyncOperations` reader into the domain model.
pub fn parse_operations(so: sync_operations::Reader<'_>) -> Result<SyncOperations, Error> {
    let mut operations = Vec::new();
    for operation in so.get_operations()? {
        match operation.which()? {
            Which::EditTeam(v) => {
                let v = v?;
                let mut alt_names = Vec::new();
                for name in v.get_alt_names()? {
                    alt_names.push(name?.to_str()?.to_owned());
                }
                operations.push(Operation::EditTeam(Team {
                    id: v.get_id(),
                    name: v.get_name()?.to_str()?.to_owned(),
                    alt_names,
                    avatar: v.get_avatar()?.to_vec(),
                    banner: v.get_banner()?.to_vec(),
                }));
            }
            Which::DelTeam(id) => {
                operations.push(Operation::DelTeam(id));
            }
            Which::EditCredit(v) => {
                let v = v?;
                let mut alt_names = Vec::new();
                for name in v.get_alt_names()? {
                    alt_names.push(name?.to_str()?.to_owned());
                }
                operations.push(Operation::EditCredit(Credit {
                    id: v.get_id(),
                    name: v.get_name()?.to_str()?.to_owned(),
                    alt_names,
                    avatar: v.get_avatar()?.to_vec(),
                    banner: v.get_banner()?.to_vec(),
                }));
            }
            Which::DelCredit(id) => {
                operations.push(Operation::DelCredit(id));
            }
            Which::EditTitle(v) => {
                let v = v?;
                let mut alt_names = Vec::new();
                for name in v.get_alt_names()? {
                    alt_names.push(name?.to_str()?.to_owned());
                }
                let mut teams = Vec::new();
                for team in v.get_teams()? {
                    teams.push(team);
                }
                operations.push(Operation::EditTitle(Title {
                    id: v.get_id(),
                    name: v.get_name()?.to_str()?.to_owned(),
                    alt_names,
                    teams,
                    avatar: v.get_avatar()?.to_vec(),
                    banner: v.get_banner()?.to_vec(),
                    type_comics: v.get_type_comics(),
                    status_release: v.get_status_release(),
                    status_translate: v.get_status_translate(),
                }));
            }
            Which::DelTitle(id) => {
                operations.push(Operation::DelTitle(id));
            }
            Which::EditUser(v) => {
                let v = v?;
                operations.push(Operation::EditUser(User {
                    id: v.get_id(),
                    name: v.get_name()?.to_str()?.to_owned(),
                }));
            }
            Which::DelUser(id) => {
                operations.push(Operation::DelUser(id));
            }
        }
    }
    Ok(SyncOperations { operations })
}
