use sha2::{Digest, Sha256};
use std::{fs, path::Path, process::Command};

fn source_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.is_dir() {
            source_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn main() {
    // Build-time source identity, independent of capture time and checkout path.
    let mut files = vec!["Cargo.toml".into(), "Cargo.lock".into(), "build.rs".into()];
    source_files(Path::new("src"), &mut files);
    files.sort();
    let mut hash = Sha256::new();
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        let bytes = fs::read(&path).expect("read build source");
        hash.update(path.to_string_lossy().as_bytes());
        hash.update([0]);
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    // Watching Git metadata also refreshes the identity after a commit without
    // changing source. Neither the metadata bytes nor timestamps enter the hash.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
    println!("cargo:rerun-if-changed=.git/refs");
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git revision");
    assert!(
        revision.status.success(),
        "source revision must be available"
    );
    let status = Command::new("git")
        .args([
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "src",
            "Cargo.toml",
            "Cargo.lock",
            "build.rs",
        ])
        .output()
        .expect("git status");
    assert!(status.status.success(), "source status must be available");
    println!(
        "cargo:rustc-env=QUORUM_REVISION={}",
        String::from_utf8(revision.stdout).unwrap().trim()
    );
    println!("cargo:rustc-env=QUORUM_DIRTY={}", !status.stdout.is_empty());
    println!("cargo:rustc-env=QUORUM_SOURCE_SHA256={:x}", hash.finalize());
}
