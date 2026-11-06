//! authorization: the middleware that decides every request, relations derived from
//! events, `Explain`, and `visibility` for lists. The middleware is a listener of the group
//! domain; `group()` builds it.

pub mod levels;
pub mod middleware;
pub mod policy;
pub mod relations;

pub use middleware::{Explain, Explained, use_authorization_store};
pub use relations::{Target, ids, parent, relation};
