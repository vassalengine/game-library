use axum::extract::FromRef;
use std::sync::Arc;

use crate::{
    core::CoreArc,
    jwt::DecodingKey,
    model::User
};

#[derive(Clone, FromRef)]
pub struct AppState {
    pub key: Arc<DecodingKey>,
    pub core: CoreArc,
    pub admins: Arc<Vec<User>>
}
