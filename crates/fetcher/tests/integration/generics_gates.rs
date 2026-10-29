// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/generics_gates.rs
// Purpose:   Run the generics acceptance gates as part of the test stage
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The generics acceptance gates in `scripts/generics_gates.py`.
//!
//! CI runs `hyperi-ci check` and has no hook for a repo-supplied command, so
//! the gates run from the test stage: a provider mechanism written outside its
//! framework site fails the run instead of waiting for someone to type `make`.

use std::process::Command;

/// Every gate holds: provider-shaped mechanisms sit at their one framework
/// site, the crate dependency direction is intact and each hook is inside its
/// line budget.
#[test]
fn the_generics_gates_hold() {
    let root = dfe_fetcher::deployment::repo_root();
    let out = Command::new("python3")
        .arg(root.join("scripts").join("generics_gates.py"))
        .current_dir(&root)
        .output()
        .expect("python3 is needed to run the generics gates");
    let report = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "generics gates exited {:?}:\n{report}\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(report.contains("every gate holds"), "{report}");
}
