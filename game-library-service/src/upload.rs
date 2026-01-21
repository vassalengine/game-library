use axum::body::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use futures::Stream;
use regex::Regex;
use s3::{
    bucket::Bucket,
    error::S3Error,
    creds::{
        Credentials,
        error::CredentialsError
    },
    region::Region
};
use sha2::{
    Digest,
    Sha256
};
use std::{
    future::Future,
    io,
    sync::LazyLock
};
use thiserror::Error;
use tokio::io::{
    AsyncRead,
    AsyncWrite
};
use tokio_util::io::{
    InspectReader,
    StreamReader
};
use tracing::info;

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("Bucket error: {0}")]
    S3Error(#[from] S3Error),
}

impl PartialEq for UploadError {
    fn eq(&self, _other: &Self) -> bool {
        // S3Error is not PartialEq, so we must exclude it
        false
    }
}

#[derive(Debug, Eq, PartialEq, Error)]
#[error("Invalid filename")]
pub struct InvalidFilename;

pub fn safe_filename(path: &str) -> Result<&str, InvalidFilename> {
    // characters to reject
    static BAD_CHAR: LazyLock<Regex> = LazyLock::new(||
        Regex::new(r#"[\x00-\x1F\x7F-\x9F/?<>\\/:*|"']"#)
            .expect("bad regex")
    );

    // reserved names on Windows
    static WIN_RESERVED: LazyLock<Regex> = LazyLock::new(||
        Regex::new(r#"^(?i:CON|PRN|AUX|NUL|(COM|LPT)[1-9])($|\.)"#)
            .expect("bad regex")
    );

    if path.is_empty() ||           // empty
        path.len() > 255 ||         // overlong
        path != path.trim() ||      // leading, trailing whitespace
        path.ends_with('.') ||      // trailing periods (Windows)
        path.contains("..") ||      // parent directory
        BAD_CHAR.is_match(path) ||  // control, reserved characters
        WIN_RESERVED.is_match(path) // reserved filenames (Windows)
    {
        Err(InvalidFilename)
    }
    else {
        Ok(path)
    }
}

pub async fn stream_to_writer<S, W>(
    stream: S,
    writer: W
) -> Result<(String, u64), io::Error>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send,
    W: AsyncWrite
{
    info!("{}", line!());

    // make hashing reader
    let mut hasher = Sha256::new();
    let reader = InspectReader::new(
        StreamReader::new(stream),
        |buf| hasher.update(buf)
    );

    info!("{}", line!());

    // read stream
    futures::pin_mut!(reader);
    futures::pin_mut!(writer);
    info!("{}", line!());
    let size = tokio::io::copy(&mut reader, &mut writer).await?;
    let sha256 = format!("{:x}", hasher.finalize());

    info!("{}", line!());
    Ok((sha256, size))
}

pub trait Uploader {
    fn upload<R>(
        &self,
        filename: &str,
        reader: R
    ) -> impl Future<Output = Result<String, UploadError>> + Send
    where
        R: AsyncRead + Unpin + Send;

    fn upload_with_content_type<R>(
        &self,
        filename: &str,
        reader: R,
        content_type: &str
    ) -> impl Future<Output = Result<String, UploadError>> + Send
    where
        R: AsyncRead + Unpin + Send;
}

pub struct LocalUploader {
    pub uploads_directory: String
}

impl Uploader for LocalUploader {
    async fn upload<R>(
        &self,
        filename: &str,
        _reader: R
    ) -> Result<String, UploadError>
    where
        R: AsyncRead + Unpin + Send
    {
        Ok(
            format!(
                "http://localhost:3000/{0}/{filename}",
                self.uploads_directory
            )
        )
    }

    async fn upload_with_content_type<R>(
        &self,
        filename: &str,
        reader: R,
        _content_type: &str
    ) -> Result<String, UploadError>
    where
        R: AsyncRead + Unpin + Send
    {
        self.upload(filename, reader).await
    }
}

#[derive(Debug, Error)]
pub enum BucketUploaderError {
    #[error("Bucket error: {0}")]
    S3Error(#[from] S3Error),
    #[error("Credentials error: {0}")]
    CredentialsError(#[from] CredentialsError)
}

pub struct BucketUploader {
    bucket: Bucket,
    base_url: String,
    base_dir: String
}

impl BucketUploader {
    pub fn new(
        name: &str,
        region: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        base_url: &str,
        base_dir: &str
    ) -> Result<BucketUploader, BucketUploaderError> {
        let bucket = Bucket::new(
            name,
            Region::Custom {
                region: region.into(),
                endpoint: endpoint.into()
            },
            Credentials::new(
                Some(access_key),
                Some(secret_key),
                None,
                None,
                None
            )?
        )?
        .with_path_style()
        .with_extra_headers(
            HeaderMap::from_iter([(
                HeaderName::from_static("x-amz-acl"),
                HeaderValue::from_static("public-read")
            )])
        )?;

        Ok(
            BucketUploader {
                bucket,
                base_url: base_url.into(),
                base_dir: base_dir.into()
            }
        )
    }
}

impl Uploader for BucketUploader {
    async fn upload<R>(
        &self,
        filename: &str,
        mut reader: R
    ) -> Result<String, UploadError>
    where
        R: AsyncRead + Unpin + Send
    {
        let path = format!("{0}/{filename}", self.base_dir);
// TODO: check return code?
        self.bucket.put_object_stream(&mut reader, &path).await?;
        Ok(format!("{0}/{path}", self.base_url))
    }

    async fn upload_with_content_type<R>(
        &self,
        filename: &str,
        mut reader: R,
        content_type: &str
    ) -> Result<String, UploadError>
    where
        R: AsyncRead + Unpin + Send
    {
        let path = format!("{0}/{filename}", self.base_dir);
// TODO: check return code?
        self.bucket.put_object_stream_with_content_type(
            &mut reader,
            &path,
            content_type
        ).await?;
        Ok(format!("{0}/{path}", self.base_url))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[track_caller]
    fn assert_invalid_filename(filename: &str) {
        assert_eq!(safe_filename(filename).unwrap_err(), InvalidFilename);
    }

    #[test]
    fn safe_filename_empty() {
        assert_invalid_filename("");
    }

    #[test]
    fn safe_filename_too_long() {
        assert_invalid_filename(&"x".repeat(256));
    }

    #[test]
    fn safe_filename_dots_1() {
        assert_invalid_filename("..");
    }

    #[test]
    fn safe_filename_dots_2() {
        assert_invalid_filename("../bad");
    }

    #[test]
    fn safe_filename_multiple_components() {
        assert_invalid_filename("one/two/three");
    }

    #[test]
    fn safe_filename_root() {
        assert_invalid_filename("/");
    }

    #[test]
    fn safe_filename_leading_whitespace() {
        assert_invalid_filename(" bad");
    }

    #[test]
    fn safe_filename_trailing_whitespace() {
        assert_invalid_filename("bad ");
    }

    #[test]
    fn safe_filename_control_chars() {
        assert_invalid_filename("bad\tbad");
    }

    #[test]
    fn safe_filename_reserved_chars() {
        assert_invalid_filename("bad?");
    }

    #[test]
    fn safe_filename_aux() {
        assert_invalid_filename("AUX");
    }

    #[test]
    fn safe_filename_lpt5() {
        assert_invalid_filename("LPT5");
    }

    #[test]
    fn safe_filename_com3() {
        assert_invalid_filename("CoM3");
    }

    #[test]
    fn safe_filename_nul_foo() {
        assert_invalid_filename("NUL.foo");
    }

    #[test]
    fn safe_filename_c_colon() {
        assert_invalid_filename("C:\\Program Files");
    }

    #[test]
    fn safe_filename_ok() {
        assert_eq!(
            safe_filename("filename.vmod").unwrap(),
            "filename.vmod"
        );
    }
}
