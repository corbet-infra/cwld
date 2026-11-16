//! The instance reconcile against a fake host.

use std::cell::RefCell;
use std::collections::BTreeMap;

use cdrv::{InstanceId, InstanceSpec, InstanceState};
use cwld::reconcile::{Domains, Outcome, reconcile};

struct Fake {
    xml: RefCell<String>,
    states: BTreeMap<String, InstanceState>,
    defines: RefCell<Vec<String>>,
    refuse: bool,
}

impl Domains for Fake {
    fn render(&self, spec: &InstanceSpec) -> Result<String, String> {
        if self.refuse {
            return Err("4Kn always".to_owned());
        }
        Ok(format!("{}:{}", spec.id, self.xml.borrow()))
    }
    fn state(&self, id: &InstanceId) -> Result<Option<InstanceState>, String> {
        Ok(self.states.get(id.as_str()).cloned())
    }
    fn define(&self, spec: &InstanceSpec) -> Result<(), String> {
        self.defines.borrow_mut().push(spec.id.to_string());
        Ok(())
    }
}

fn spec(name: &str) -> InstanceSpec {
    serde_json::from_value(serde_json::json!({
        "id": name, "kind": "vm", "guest_os": "macos",
        "memory": {"min_mib": 1024, "max_mib": 1024, "balloon": false},
        "cpu": {"quota_cores": 1.0}, "class": "desktop",
        "devices": [], "disks": [], "vcpus": 2, "nic": null, "console": false, "identity": null
    }))
    .expect("a spec")
}

fn fake(state: Option<InstanceState>) -> Fake {
    let mut states = BTreeMap::new();
    if let Some(s) = state {
        states.insert("mac".to_owned(), s);
    }
    Fake {
        xml: RefCell::new("v1".to_owned()),
        states,
        defines: RefCell::new(Vec::new()),
        refuse: false,
    }
}

fn run(f: &Fake, dir: &std::path::Path) -> Outcome {
    reconcile(f, &[spec("mac")], dir, true).remove(0).1
}

#[test]
fn an_undefined_domain_is_defined_and_then_left_alone() {
    let dir = tempdir("undef");
    let mut f = fake(None);
    assert_eq!(run(&f, &dir), Outcome::Defined);
    f.states.insert("mac".to_owned(), InstanceState::Stopped);
    assert_eq!(run(&f, &dir), Outcome::Unchanged);
    assert_eq!(f.defines.borrow().len(), 1);
}

#[test]
fn a_shut_off_domain_with_a_changed_render_is_redefined() {
    let dir = tempdir("redef");
    let f = fake(Some(InstanceState::Stopped));
    assert_eq!(
        run(&f, &dir),
        Outcome::Redefined,
        "no record yet: define what the declaration says"
    );
    assert_eq!(run(&f, &dir), Outcome::Unchanged);
    *f.xml.borrow_mut() = "v2".to_owned();
    assert_eq!(run(&f, &dir), Outcome::Redefined);
    assert_eq!(run(&f, &dir), Outcome::Unchanged);
    assert_eq!(f.defines.borrow().len(), 2);
}

#[test]
fn a_live_domain_is_never_touched() {
    let dir = tempdir("live");
    for state in [
        InstanceState::Running,
        InstanceState::Starting,
        InstanceState::Stopping,
        InstanceState::Paused,
        InstanceState::Failed("x".to_owned()),
    ] {
        let f = fake(Some(state));
        assert_eq!(run(&f, &dir), Outcome::Pending);
        assert!(f.defines.borrow().is_empty());
    }
}

#[test]
fn a_dry_run_reports_and_changes_nothing() {
    let dir = tempdir("dry");
    let f = fake(Some(InstanceState::Stopped));
    let out = reconcile(&f, &[spec("mac")], &dir, false).remove(0).1;
    assert_eq!(out, Outcome::Redefined);
    assert!(f.defines.borrow().is_empty());
    assert_eq!(
        run(&f, &dir),
        Outcome::Redefined,
        "nothing was recorded by the dry run"
    );
}

#[test]
fn a_refused_spec_changes_nothing() {
    let dir = tempdir("refuse");
    let mut f = fake(Some(InstanceState::Stopped));
    f.refuse = true;
    assert!(matches!(run(&f, &dir), Outcome::Failed(_)));
    assert!(f.defines.borrow().is_empty());
}

fn tempdir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("cwld-reconcile-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a temp dir");
    dir
}
