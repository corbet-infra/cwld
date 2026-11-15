//! The Forgejo port: which scopes are served, which jobs wait, and one
//! ephemeral runner registration per job.
//!
//! Forgejo offers no push for "a job is waiting" (its webhooks report finished
//! runs, and the runner protocol's `FetchTask` long poll needs a registered
//! runner, which would defeat one-job isolation). Listing the waiting jobs is
//! therefore the one polling exception; see [`super::flow`] for its cadence.

use std::fmt;
use std::sync::Arc;

use serde_json::{Value, json};

use super::http::{Http, HttpRequest, HttpResponse};
use super::secret::Secret;

/// Why a forge call failed.
#[derive(Debug, thiserror::Error)]
pub enum ForgeError {
    /// The transport failed.
    #[error("transport: {0}")]
    Transport(String),
    /// The forge answered with a non-2xx status.
    #[error("{method} {path} answered {status}")]
    Status {
        /// The method.
        method: String,
        /// The path, without its query.
        path: String,
        /// The status.
        status: u16,
    },
    /// The answer was not what the API documents.
    #[error("unexpected answer: {0}")]
    Shape(String),
}

/// Whose runners a registration belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScopeKind {
    /// The token owner's own namespace.
    User,
    /// An organisation the token owner owns.
    Org,
}

/// One runner scope: a registration is valid for exactly one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scope {
    /// User or organisation.
    pub kind: ScopeKind,
    /// Its name.
    pub name: String,
}

impl Scope {
    fn path(&self, tail: &str) -> String {
        match self.kind {
            ScopeKind::Org => format!("/orgs/{}/actions/runners/{tail}", self.name),
            ScopeKind::User => format!("/user/actions/runners/{tail}"),
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// A job that waits for a runner and asks for exactly one label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingJob {
    /// The forge's job id (older first).
    pub id: u64,
    /// The handle `one-job --handle` takes.
    pub handle: String,
    /// The one label the job asks for.
    pub label: String,
}

/// One single-use runner registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// The runner's uuid.
    pub uuid: String,
    /// Its single-use token; never logged, never in an argument list.
    pub token: Secret,
}

/// What the dispatcher needs from Forgejo.
pub trait Forge: Send + Sync {
    /// The scopes served: the token owner's namespace and every organisation
    /// the owner owns, derived at each refresh and never configured.
    ///
    /// # Errors
    /// The forge could not be asked.
    fn scopes(&self) -> Result<Vec<Scope>, ForgeError>;

    /// The waiting jobs of a scope that ask for exactly one of `labels`.
    ///
    /// # Errors
    /// The forge could not be asked.
    fn waiting_jobs(&self, scope: &Scope, labels: &[String])
    -> Result<Vec<WaitingJob>, ForgeError>;

    /// One EPHEMERAL registration for the scope under the runner `name`.
    ///
    /// # Errors
    /// The forge refused or could not be asked.
    fn register_ephemeral(&self, scope: &Scope, name: &str) -> Result<Registration, ForgeError>;
}

/// A name that is safe in a path, a unit name and a log line.
#[must_use]
pub fn is_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// A job handle (hex and dashes).
#[must_use]
pub fn is_handle(value: &str) -> bool {
    (8..=64).contains(&value.len()) && value.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

/// The forge over HTTP.
pub struct ForgeClient {
    http: Arc<dyn Http>,
    api: String,
    runner_url: String,
    token: Secret,
    version: String,
    include_user: bool,
}

impl ForgeClient {
    /// `api` is the REST base (`https://forge/api/v1`), `runner_url` the
    /// forge's root (`https://forge/`), `token` a credential with read scopes
    /// only, `version` what the runner protocol's `Register` reports.
    #[must_use]
    pub fn new(
        http: Arc<dyn Http>,
        api: &str,
        runner_url: &str,
        token: Secret,
        version: &str,
        include_user: bool,
    ) -> Self {
        Self {
            http,
            api: api.trim_end_matches('/').to_owned(),
            runner_url: runner_url.trim_end_matches('/').to_owned(),
            token,
            version: version.to_owned(),
            include_user,
        }
    }

    fn check(method: &str, path: &str, response: HttpResponse) -> Result<String, ForgeError> {
        if response.status / 100 == 2 {
            Ok(response.body)
        } else {
            Err(ForgeError::Status {
                method: method.to_owned(),
                path: path.split('?').next().unwrap_or_default().to_owned(),
                status: response.status,
            })
        }
    }

    fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, ForgeError> {
        let mut url = format!("{}{path}", self.api);
        if !query.is_empty() {
            let pairs: Vec<String> = query
                .iter()
                .map(|(k, v)| format!("{k}={}", encode(v)))
                .collect();
            url.push('?');
            url.push_str(&pairs.join("&"));
        }
        let request = HttpRequest {
            method: "GET".to_owned(),
            url,
            headers: vec![
                (
                    "Authorization".to_owned(),
                    format!("token {}", self.token.expose()),
                ),
                ("Accept".to_owned(), "application/json".to_owned()),
                // the forge refuses a default client agent with 403
                ("User-Agent".to_owned(), "cwld-ci/1".to_owned()),
            ],
            body: None,
        };
        let response = self
            .http
            .send(&request)
            .map_err(|e| ForgeError::Transport(e.to_string()))?;
        let body = Self::check("GET", path, response)?;
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).map_err(|e| ForgeError::Shape(e.to_string()))
    }

    fn paged(&self, path: &str) -> Result<Vec<Value>, ForgeError> {
        const LIMIT: usize = 50;
        let mut all = Vec::new();
        for page in 1.. {
            let part = self.get(
                path,
                &[("limit", LIMIT.to_string()), ("page", page.to_string())],
            )?;
            let items = part.as_array().cloned().unwrap_or_default();
            let n = items.len();
            all.extend(items);
            if n < LIMIT {
                break;
            }
        }
        Ok(all)
    }
}

fn encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b',') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl Forge for ForgeClient {
    fn scopes(&self) -> Result<Vec<Scope>, ForgeError> {
        let me = self.get("/user", &[])?;
        let login = me["login"]
            .as_str()
            .ok_or_else(|| ForgeError::Shape("/user has no login".to_owned()))?
            .to_owned();
        let mut scopes = Vec::new();
        if self.include_user && is_name(&login) {
            scopes.push(Scope {
                kind: ScopeKind::User,
                name: login.clone(),
            });
        }
        for org in self.paged("/user/orgs")? {
            let name = org["username"].as_str().unwrap_or_default();
            if !is_name(name) {
                continue;
            }
            let perm = self.get(&format!("/users/{login}/orgs/{name}/permissions"), &[])?;
            if perm["is_owner"] == Value::Bool(true) {
                scopes.push(Scope {
                    kind: ScopeKind::Org,
                    name: name.to_owned(),
                });
            }
        }
        Ok(scopes)
    }

    fn waiting_jobs(
        &self,
        scope: &Scope,
        labels: &[String],
    ) -> Result<Vec<WaitingJob>, ForgeError> {
        let answer = self.get(&scope.path("jobs"), &[("labels", labels.join(","))])?;
        let mut jobs = Vec::new();
        for j in answer.as_array().cloned().unwrap_or_default() {
            let runs_on: Vec<&str> = j["runs_on"]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let handle = j["handle"].as_str().unwrap_or_default();
            // a job that asks for two labels is nobody's; a held (blocked) run is not `waiting`
            if j["status"] == "waiting"
                && runs_on.len() == 1
                && labels.iter().any(|l| l == runs_on[0])
                && is_handle(handle)
            {
                jobs.push(WaitingJob {
                    id: j["id"].as_u64().unwrap_or_default(),
                    handle: handle.to_owned(),
                    label: runs_on[0].to_owned(),
                });
            }
        }
        Ok(jobs)
    }

    fn register_ephemeral(&self, scope: &Scope, name: &str) -> Result<Registration, ForgeError> {
        // 1. a registration token (a GET: the credential has read scopes only)
        let reg = self.get(&scope.path("registration-token"), &[])?;
        let reg_token = Secret::new(
            reg["token"]
                .as_str()
                .ok_or_else(|| ForgeError::Shape("no registration token".to_owned()))?,
        );
        // 2. the runner protocol's Register, ephemeral; the token travels in the body
        let path = "/api/actions/runner.v1.RunnerService/Register";
        let body = json!({
            "name": name,
            "token": reg_token.expose(),
            "version": self.version,
            "ephemeral": true,
        });
        let request = HttpRequest {
            method: "POST".to_owned(),
            url: format!("{}{path}", self.runner_url),
            headers: vec![
                ("Content-Type".to_owned(), "application/json".to_owned()),
                ("Connect-Protocol-Version".to_owned(), "1".to_owned()),
                ("User-Agent".to_owned(), "cwld-ci/1".to_owned()),
            ],
            body: Some(body.to_string()),
        };
        let response = self
            .http
            .send(&request)
            .map_err(|e| ForgeError::Transport(e.to_string()))?;
        let answer = Self::check("POST", path, response)?;
        let value: Value =
            serde_json::from_str(&answer).map_err(|e| ForgeError::Shape(e.to_string()))?;
        let runner = &value["runner"];
        match (runner["uuid"].as_str(), runner["token"].as_str()) {
            (Some(uuid), Some(token)) => Ok(Registration {
                uuid: uuid.to_owned(),
                token: Secret::new(token),
            }),
            _ => Err(ForgeError::Shape("Register answered no runner".to_owned())),
        }
    }
}
