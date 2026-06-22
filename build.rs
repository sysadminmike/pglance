use std::{env, fs, path::Path, process::Command};

#[derive(Default)]
struct Package {
    name: String,
    version: String,
    source: String,
}

fn parse_value(line: &str, key: &str) -> Option<String> {
    let prefix = format!("{} = \"", key);
    line.strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .map(ToString::to_string)
}

fn read_lock_packages() -> Vec<Package> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let lock_path = Path::new(&manifest_dir).join("Cargo.lock");
    let Ok(contents) = fs::read_to_string(lock_path) else {
        return Vec::new();
    };

    let mut packages = Vec::new();
    let mut current: Option<Package> = None;

    for line in contents.lines().map(str::trim) {
        if line == "[[package]]" {
            if let Some(package) = current.take() {
                packages.push(package);
            }
            current = Some(Package::default());
            continue;
        }

        let Some(package) = current.as_mut() else {
            continue;
        };

        if let Some(value) = parse_value(line, "name") {
            package.name = value;
        } else if let Some(value) = parse_value(line, "version") {
            package.version = value;
        } else if let Some(value) = parse_value(line, "source") {
            package.source = value;
        }
    }

    if let Some(package) = current.take() {
        packages.push(package);
    }

    packages
}

fn find_package<'a>(packages: &'a [Package], name: &str) -> Option<&'a Package> {
    packages
        .iter()
        .find(|package| {
            package.name == name && package.source.contains("github.com/lance-format/lance")
        })
        .or_else(|| packages.iter().find(|package| package.name == name))
}

fn source_revision(source: &str) -> String {
    source
        .rsplit_once('#')
        .map(|(_, revision)| revision.to_string())
        .unwrap_or_default()
}

fn emit_package(prefix: &str, package: Option<&Package>) {
    let version = package
        .map(|package| package.version.as_str())
        .unwrap_or("");
    let source = package.map(|package| package.source.as_str()).unwrap_or("");
    let revision = package
        .map(|package| source_revision(&package.source))
        .unwrap_or_default();

    println!("cargo:rustc-env={prefix}_VERSION={version}");
    println!("cargo:rustc-env={prefix}_SOURCE={source}");
    println!("cargo:rustc-env={prefix}_REVISION={revision}");
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=.git/HEAD");

    let packages = read_lock_packages();
    emit_package("PGLANCE_DEP_LANCE", find_package(&packages, "lance"));
    emit_package(
        "PGLANCE_DEP_LANCE_INDEX",
        find_package(&packages, "lance-index"),
    );
    emit_package(
        "PGLANCE_DEP_LANCE_NAMESPACE",
        find_package(&packages, "lance-namespace"),
    );
    emit_package(
        "PGLANCE_DEP_LANCE_NAMESPACE_IMPLS",
        find_package(&packages, "lance-namespace-impls"),
    );

    println!(
        "cargo:rustc-env=PGLANCE_BUILD_PROFILE={}",
        env::var("PROFILE").unwrap_or_default()
    );
    println!(
        "cargo:rustc-env=PGLANCE_GIT_REVISION={}",
        command_output("git", &["rev-parse", "--short", "HEAD"])
    );
    println!(
        "cargo:rustc-env=PGLANCE_RUSTC_VERSION={}",
        command_output("rustc", &["--version"])
    );
}
