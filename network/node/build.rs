//! Stamp build-time env vars the binary can use for a boot marker.
//!
//! Exposes two env vars to the source:
//! - `TAPE_BUILD_SHA` — short git sha, or `"unknown"` if git is unavailable
//! - `TAPE_BUILD_SUFFIX` — `"-dirty"` if the working tree has uncommitted
//!   changes at build time, otherwise empty

use std::process::Command;

fn main() {
    // Watch the HEAD reflog, not just HEAD: a pull moves the ref without
    // touching the symref. The dirty flag only refreshes on rebuild.
    println!("cargo:rerun-if-changed=build.rs");
    if let Some(git_dir) = run_git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        println!("cargo:rerun-if-changed={git_dir}/logs/HEAD");
    }

    let sha = run_git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = match Command::new("git")
        .args(["diff-index", "--quiet", "HEAD", "--"])
        .status()
    {
        Ok(status) => !status.success(),
        Err(_) => false,
    };
    let suffix = if dirty { "-dirty" } else { "" };

    println!("cargo:rustc-env=TAPE_BUILD_SHA={sha}");
    println!("cargo:rustc-env=TAPE_BUILD_SUFFIX={suffix}");
}

fn run_git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
