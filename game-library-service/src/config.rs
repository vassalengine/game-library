use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub db_path: String,
    pub jwt_key: String,
    pub api_base_path: String,
    pub listen_ip: String,
    pub listen_port: u16,
    pub max_file_size: usize,
    pub max_image_size: usize,
    pub read_only: bool,
    pub bucket_name: String,
    pub bucket_region: String,
    pub bucket_endpoint: String,
    pub bucket_access_key: String,
    pub bucket_secret_key: String,
    pub bucket_base_url: String,
    pub bucket_base_dir: String,
    pub upload_dir: String,
    pub upload_timeout: u64,
    pub log_headers: bool,
    pub admins: Vec<String>,
    // See: discourse webhooks
    pub discourse_update_secret: String
}
