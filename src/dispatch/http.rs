//! The one HTTP port of the dispatcher, and its `curl` adapter.
//!
//! `curl` is the adapter on purpose: it honours the host's trust store
//! (`SSL_CERT_FILE`, the internal entrance's CA), adds no TLS stack and no
//! non-permissive licence to the crate, and takes the request from its
//! standard input (`--config -`), so a token never appears in an argument
//! list.

use std::fmt;
use std::io::{self, Write};
use std::process::{Command, Stdio};

/// One request. Header values may be secret: `Debug` hides them.
#[derive(Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// `GET` or `POST`.
    pub method: String,
    /// The absolute URL.
    pub url: String,
    /// Header name and value pairs.
    pub headers: Vec<(String, String)>,
    /// The body, when there is one.
    pub body: Option<String>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field(
                "headers",
                &self.headers.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// One answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// The status code.
    pub status: u16,
    /// The body.
    pub body: String,
}

/// Send a request.
pub trait Http: Send + Sync {
    /// Perform the request; a non-2xx status is an answer, not an error.
    ///
    /// # Errors
    /// Transport failures only.
    fn send(&self, request: &HttpRequest) -> io::Result<HttpResponse>;
}

/// The `curl` adapter.
#[derive(Debug, Clone)]
pub struct CurlHttp {
    /// The curl binary.
    pub curl: String,
    /// The whole-request time limit in seconds.
    pub timeout_s: u32,
}

impl Default for CurlHttp {
    fn default() -> Self {
        Self {
            curl: "curl".to_owned(),
            timeout_s: 15,
        }
    }
}

/// A value in curl's config-file syntax.
fn quoted(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The curl configuration for a request (the secrets travel in it).
fn curl_config(request: &HttpRequest) -> String {
    let mut cfg = format!(
        "url = {}\nrequest = {}\n",
        quoted(&request.url),
        quoted(&request.method)
    );
    for (name, value) in &request.headers {
        cfg.push_str(&format!(
            "header = {}\n",
            quoted(&format!("{name}: {value}"))
        ));
    }
    if let Some(body) = &request.body {
        cfg.push_str(&format!("data-raw = {}\n", quoted(body)));
    }
    cfg
}

impl Http for CurlHttp {
    fn send(&self, request: &HttpRequest) -> io::Result<HttpResponse> {
        // curl's config syntax turns `\n` into a real newline, so a line break in
        // a header would inject another header: refuse it outright.
        let bad = |text: &str| text.contains(['\r', '\n']);
        if request
            .headers
            .iter()
            .any(|(name, value)| bad(name) || bad(value))
            || bad(&request.url)
            || bad(&request.method)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a line break in a request line or header is refused",
            ));
        }
        let mut child = Command::new(&self.curl)
            .args([
                "--silent",
                "--show-error",
                "--max-time",
                &self.timeout_s.to_string(),
            ])
            .args(["--write-out", "\n%{http_code}", "--config", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(curl_config(request).as_bytes())?;
        }
        let out = child.wait_with_output()?;
        if !out.status.success() {
            let why = String::from_utf8_lossy(&out.stderr);
            return Err(io::Error::other(format!("curl failed: {}", why.trim())));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let (body, code) = text
            .rsplit_once('\n')
            .ok_or_else(|| io::Error::other("curl gave no status line"))?;
        let status = code
            .trim()
            .parse::<u16>()
            .map_err(|_| io::Error::other("curl gave an unreadable status"))?;
        Ok(HttpResponse {
            status,
            body: body.to_owned(),
        })
    }
}
