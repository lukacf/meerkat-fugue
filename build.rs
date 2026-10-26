use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(["-c", "core.fsmonitor=false"])
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|s| s.trim().to_owned())
}

fn main() {
    for path in ["src", "Cargo.toml", "Cargo.lock", "build.rs"] {
        println!("cargo:rerun-if-changed={path}");
    }
    for name in ["HEAD", "index", "refs", "packed-refs"] {
        if let Some(path) = git(&["rev-parse", "--git-path", name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let commit = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let tree = git(&["rev-parse", "HEAD^{tree}"]).unwrap_or_else(|| "unknown".to_owned());
    let dirty = git(&["status", "--porcelain", "--untracked-files=normal"])
        .is_none_or(|status| !status.is_empty());
    println!("cargo:rustc-env=FUGUE_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=FUGUE_BUILD_TREE={tree}");
    println!("cargo:rustc-env=FUGUE_BUILD_DIRTY={dirty}");
}
