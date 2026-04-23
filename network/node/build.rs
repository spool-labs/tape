//! Stamp build-time env vars the binary can use for a boot marker.
//!
//! Exposes two env vars to the source:
//! - `TAPE_BUILD_SHA` — short git sha, or `"unknown"` if git is unavailable
//! - `TAPE_BUILD_SUFFIX` — `"-dirty"` if the working tree has uncommitted
//!   changes at build time, otherwise empty

use std::process::Command;

fn main() {
    // Trigger rebuild when HEAD moves. For dirty-flag accuracy we'd want to
    // watch every tracked file, which is impractical — operators that care
    // about the dirty flag should `cargo clean` before stamping.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=build.rs");

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
