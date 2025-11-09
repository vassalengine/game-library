use axum::extract::FromRef;
use std::sync::Arc;

use crate::{
    core::CoreArc,
    jwt::DecodingKey,
    model::User
};

#[derive(Default)]
pub struct DiscourseUpdateConfig {
    pub secret: Vec<u8>
}

#[derive(Clone, FromRef)]
pub struct AppState {
    pub key: Arc<DecodingKey>,
    pub core: CoreArc,
    pub admins: Arc<Vec<User>>,
    pub discourse_update_config: Arc<DiscourseUpdateConfig>
}
