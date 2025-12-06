use mime::Mime;
use std::sync::LazyLock;
use thiserror::Error;

static IMAGE_AVIF: LazyLock<Mime> = LazyLock::new(||
    "image/avif".parse().expect("bad MIME type")
);

static IMAGE_WEBP: LazyLock<Mime> = LazyLock::new(||
    "image/webp".parse().expect("bad MIME type")
);

static APPLICATION_ZIP: LazyLock<Mime> = LazyLock::new(||
    "application/zip".parse().expect("bad MIME type")
);

#[derive(Debug, Error, PartialEq, Eq)]
#[error("Bad MIME type")]
pub struct BadMimeType;

fn check_avif(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::image::is_avif(buf) {
        true => Ok(IMAGE_AVIF.clone()),
        false => Err(BadMimeType)
    }
}

fn check_gif(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::image::is_gif(buf) {
        true => Ok(mime::IMAGE_GIF),
        false => Err(BadMimeType)
    }
}

fn check_jpeg(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::image::is_jpeg(buf) {
        true => Ok(mime::IMAGE_JPEG),
        false => Err(BadMimeType)
    }
}

fn check_png(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::image::is_png(buf) {
        true => Ok(mime::IMAGE_PNG),
        false => Err(BadMimeType)
    }
}

fn check_webp(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::image::is_webp(buf) {
        true => Ok(IMAGE_WEBP.clone()),
        false => Err(BadMimeType)
    }
}

fn check_pdf(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::archive::is_pdf(buf) {
        true => Ok(mime::APPLICATION_PDF),
        false => Err(BadMimeType)
    }
}

fn check_ours(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::archive::is_zip(buf) {
        true => Ok(mime::APPLICATION_OCTET_STREAM),
        false => Err(BadMimeType)
    }
}

fn check_zip(buf: &[u8]) -> Result<Mime, BadMimeType>
{
    match infer::archive::is_zip(buf) {
        true => Ok(APPLICATION_ZIP.clone()),
        false => Err(BadMimeType)
    }
}

fn check_other(
    ext: &str,
    buf: &[u8]
) -> Result<Option<Mime>, BadMimeType>
{
    match infer::get(buf) {
        None => Ok(None),
        Some(inf) if inf.extension() == ext => Ok(Some(
            inf.mime_type().parse::<Mime>()
                .map_err(|_| BadMimeType)?
        )),
        _ => Err(BadMimeType)
    }
}

pub fn infer_file_type(
    ext: Option<&str>,
    buf: &[u8]
) -> Result<Option<Mime>, BadMimeType>
{
    match ext {
        Some("svg") => Ok(Some(mime::IMAGE_SVG)),
        Some("txt") => Ok(Some(mime::TEXT_PLAIN)),
        Some("avif") => check_avif(buf).map(Some),
        Some("gif") => check_gif(buf).map(Some),
        Some("jpg") => check_jpeg(buf).map(Some),
        Some("png") => check_png(buf).map(Some),
        Some("webp") => check_webp(buf).map(Some),
        Some("pdf") => check_pdf(buf).map(Some),
        Some("vlog") |
        Some("vmdx") |
        Some("vmod") |
        Some("vsav") => check_ours(buf).map(Some),
        Some("zip") => check_zip(buf).map(Some),
        Some(ext) => check_other(ext, buf),
        None => Ok(None)
    }
}

pub fn infer_image_type(
    ext: Option<&str>,
    buf: &[u8]
) -> Result<Mime, BadMimeType>
{
    match ext {
        Some("svg") => Ok(mime::IMAGE_SVG),
        Some("avif") => check_avif(buf),
        Some("gif") => check_gif(buf),
        Some("jpg") => check_jpeg(buf),
        Some("png") => check_png(buf),
        Some("webp") => check_webp(buf),
        _ => Err(BadMimeType)
    }
}

pub fn supported_image_type(mime: &Mime) -> bool {
    mime == &mime::IMAGE_PNG ||
    mime == &mime::IMAGE_JPEG ||
    mime == &mime::IMAGE_GIF ||
    mime == &mime::IMAGE_SVG ||
    *mime == *IMAGE_AVIF ||
    *mime == *IMAGE_WEBP
}

#[cfg(test)]
mod test {
    use super::*;

    use itertools::iproduct;

    macro_rules! file_as_bytes {
        ($i:ident, $f:literal) => {
            const $i: [u8; include_bytes!($f).len()] = *include_bytes!($f);
        }
    }

    file_as_bytes!(AVIF, "../test/a.avif");
    file_as_bytes!(GIF, "../test/a.gif");
    file_as_bytes!(JPEG, "../test/a.jpg");
    file_as_bytes!(PDF, "../test/a.pdf");
    file_as_bytes!(PNG, "../test/a.png");
    file_as_bytes!(WEBP, "../test/a.webp");
    file_as_bytes!(ZIP, "../test/a.zip");

    #[test]
    fn check_types() {
        type Test<'a> = (
            &'a [u8],
            fn(buf: &[u8]) -> Result<Mime, BadMimeType>,
            Mime
        );

        let tests: [Test; 7] = [
            (&AVIF, check_avif, IMAGE_AVIF.clone()),
            (&GIF, check_gif, mime::IMAGE_GIF),
            (&JPEG, check_jpeg, mime::IMAGE_JPEG),
            (&PDF, check_pdf, mime::APPLICATION_PDF),
            (&PNG, check_png, mime::IMAGE_PNG),
            (&WEBP, check_webp, IMAGE_WEBP.clone()),
            (&ZIP, check_zip, APPLICATION_ZIP.clone())
        ];

        for ((lbuf, ch, mtype), (rbuf, _, _)) in iproduct!(&tests, &tests) {
            if lbuf == rbuf {
                // on the diagonal, should be a match
                assert_eq!(ch(rbuf), Ok(mtype.clone()));
            }
            else {
                // off the diagonal, should not be a match
                assert_eq!(ch(rbuf), Err(BadMimeType));
            }
        }
    }

    #[test]
    fn check_ours_ok() {
        assert_eq!(
            check_ours(&ZIP),
            Ok(mime::APPLICATION_OCTET_STREAM)
        );
    }

    #[test]
    fn check_ours_err() {
        assert_eq!(check_ours(&[]), Err(BadMimeType));
    }

    #[test]
    fn check_other_ok_recognized() {
        assert_eq!(check_other("gif", &GIF), Ok(Some(mime::IMAGE_GIF)));
    }

    #[test]
    fn check_other_ok_unrecognized() {
        assert_eq!(check_other("xyz", &[]), Ok(None));
    }

    #[test]
    fn check_other_err() {
        assert_eq!(check_other("notgif", &GIF), Err(BadMimeType));
    }

    #[test]
    fn infer_image_types() {
        let tests: [(&[u8], Option<&str>, Mime); 5] = [
            (&AVIF, Some("avif"), IMAGE_AVIF.clone()),
            (&GIF, Some("gif"),  mime::IMAGE_GIF),
            (&JPEG, Some("jpg"), mime::IMAGE_JPEG),
            (&PNG, Some("png"), mime::IMAGE_PNG),
            (&WEBP, Some("webp"), IMAGE_WEBP.clone())
        ];

        for ((lbuf, ext, mtype), (rbuf, _, _)) in iproduct!(&tests, &tests) {
            if lbuf == rbuf {
                // on the diagonal, should be a match
                assert_eq!(infer_image_type(*ext, rbuf), Ok(mtype.clone()));
            }
            else {
                // off the diagonal, should not be a match
                assert_eq!(infer_image_type(*ext, rbuf), Err(BadMimeType));
            }
        }

        assert_eq!(infer_image_type(Some("svg"), &[]), Ok(mime::IMAGE_SVG));
    }

    #[test]
    fn supported_image_types_yes() {
        let mtypes = [
            &*IMAGE_AVIF,
            &mime::IMAGE_GIF,
            &mime::IMAGE_JPEG,
            &mime::IMAGE_PNG,
            &mime::IMAGE_SVG,
            &*IMAGE_WEBP
        ];

        for m in mtypes {
            assert!(supported_image_type(m));
        }
    }

    #[test]
    fn supported_image_types_no() {
        assert!(!supported_image_type(&mime::TEXT_PLAIN));
    }

    #[test]
    fn infer_file_types() {
        let tests: [(&[u8], &str, Mime); 7] = [
            (&AVIF, "avif", IMAGE_AVIF.clone()),
            (&GIF,  "gif",  mime::IMAGE_GIF),
            (&JPEG, "jpg", mime::IMAGE_JPEG),
            (&PDF,  "pdf", mime::APPLICATION_PDF),
            (&PNG,  "png", mime::IMAGE_PNG),
            (&WEBP, "webp", IMAGE_WEBP.clone()),
            (&ZIP, "zip", APPLICATION_ZIP.clone())
        ];

        for ((lbuf, ext, mtype), (rbuf, _, _)) in iproduct!(&tests, &tests) {
            let r = infer_file_type(Some(ext), rbuf);
            if lbuf == rbuf {
                // on the diagonal, should be a match
                assert_eq!(r, Ok(Some(mtype.clone())));
            }
            else {
                // off the diagonal, should not be a match
                assert_eq!(r, Err(BadMimeType));
            }
        }

        assert_eq!(infer_file_type(Some("svg"), &[]), Ok(Some(mime::IMAGE_SVG)));
        assert_eq!(infer_file_type(Some("txt"), &[]), Ok(Some(mime::TEXT_PLAIN)));
    }

    #[test]
    fn infer_v_file_types() {
        let tests = [ "vlog", "vmdx", "vmod", "vsav" ];

        for ext in tests {
            assert_eq!(
                infer_file_type(Some(ext), &ZIP),
                Ok(Some(mime::APPLICATION_OCTET_STREAM))
            );

            assert_eq!(
                infer_file_type(Some(ext), &[]),
                 Err(BadMimeType)
            );
        }
    }
}
