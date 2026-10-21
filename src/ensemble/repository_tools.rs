//! Read-only, bounded checkout tools for specialists without shell access.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::{AgentToolDispatcher, ToolCallView, ToolDef, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::json;

const MAX_READ_BYTES: usize = 64 * 1024;
const MAX_DIRECTORY_ENTRIES: usize = 200;
const MAX_DIRECTORY_SCAN: usize = 10_000;

pub struct RepositoryTools {
    root: PathBuf,
    excluded_state_root: Option<PathBuf>,
    tools: Arc<[Arc<ToolDef>]>,
}

impl RepositoryTools {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            excluded_state_root: None,
            tools: vec![
                Arc::new(ToolDef::new(
                    "repo_read_file",
                    "Read UTF-8 source or documentation inside your own repository, including .rkat/skills. Bounded to 64 KiB per call; use next_offset to continue. Conventional credential files, configured runtime state and paths outside the checkout are unavailable.",
                    json!({
                        "type": "object",
                        "properties": {
                            "path": {"type": "string", "description": "Repository-relative file path."},
                            "offset": {"type": "integer", "minimum": 0, "description": "Byte offset, normally next_offset from the previous result."},
                            "limit": {"type": "integer", "minimum": 1, "maximum": MAX_READ_BYTES}
                        },
                        "required": ["path"],
                        "additionalProperties": false
                    }),
                )),
                Arc::new(ToolDef::new(
                    "repo_list_files",
                    "List immediate files and directories inside your own repository in sorted pages of up to 200 entries, including an explicit .rkat/skills path. Use next_offset to continue; offsets count only visible entries. Omits symlink entries, conventional credential files and configured runtime state. Scans at most 10,000 directory entries. Restart at offset 0 if the directory changes between calls.",
                    json!({
                        "type": "object",
                        "properties": {
                            "path": {"type": "string", "description": "Repository-relative directory; defaults to the root."},
                            "offset": {"type": "integer", "minimum": 0, "description": "Visible-entry offset, normally next_offset from the previous result; defaults to 0."}
                        },
                        "additionalProperties": false
                    }),
                )),
            ].into(),
        }
    }

    pub fn with_excluded_state_root(mut self, state_root: PathBuf) -> Self {
        self.excluded_state_root = Some(state_root);
        self
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFile {
    path: PathBuf,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_read_limit")]
    limit: usize,
}

fn default_read_limit() -> usize {
    MAX_READ_BYTES
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFiles {
    #[serde(default)]
    path: PathBuf,
    #[serde(default)]
    offset: usize,
}

#[derive(Serialize, Deserialize)]
struct FileContent {
    path: PathBuf,
    offset: u64,
    next_offset: Option<u64>,
    total_bytes: u64,
    content: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EntryKind {
    File,
    Directory,
}

#[derive(Serialize, Deserialize)]
struct DirectoryEntry {
    name: String,
    kind: EntryKind,
}

#[derive(Serialize, Deserialize)]
struct DirectoryContent {
    path: PathBuf,
    offset: usize,
    next_offset: Option<usize>,
    entries: Vec<DirectoryEntry>,
}

enum Operation {
    Read(ReadFile),
    List(ListFiles),
}

#[async_trait]
impl AgentToolDispatcher for RepositoryTools {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        Arc::clone(&self.tools)
    }

    fn tool_catalog_capabilities(&self) -> meerkat_core::ToolCatalogCapabilities {
        meerkat_core::ToolCatalogCapabilities {
            exact_catalog: true,
            may_require_catalog_control_plane: false,
        }
    }

    async fn dispatch(&self, call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        let parse_error = |error: serde_json::Error| ToolError::InvalidArguments {
            name: call.name.to_string(),
            reason: error.to_string(),
        };
        let operation = match call.name {
            "repo_read_file" => {
                Operation::Read(serde_json::from_str(call.args.get()).map_err(parse_error)?)
            }
            "repo_list_files" => {
                Operation::List(serde_json::from_str(call.args.get()).map_err(parse_error)?)
            }
            _ => {
                return Err(ToolError::NotFound {
                    name: call.name.to_string(),
                });
            }
        };
        let root = self.root.clone();
        let excluded_state_root = self.excluded_state_root.clone();
        let content = tokio::task::spawn_blocking(move || match operation {
            Operation::Read(input) => {
                serde_json::to_string(&read_file(&root, input, excluded_state_root.as_deref())?)
                    .map_err(|error| error.to_string())
            }
            Operation::List(input) => {
                serde_json::to_string(&list_files(&root, input, excluded_state_root.as_deref())?)
                    .map_err(|error| error.to_string())
            }
        })
        .await
        .map_err(|error| ToolError::ExecutionFailed {
            message: format!("repository read task failed: {error}"),
        })?
        .map_err(|message| ToolError::ExecutionFailed { message })?;
        Ok(ToolResult::new(call.id.to_string(), content, false).into())
    }
}

fn resolve(
    root: &Path,
    relative: &Path,
    excluded_state_root: Option<&Path>,
) -> Result<PathBuf, String> {
    if relative.is_absolute()
        || relative.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err("use a repository-relative path without '..'".to_string());
    }
    if protected_path(relative) {
        return Err(
            "credentials and private runtime state are not exposed by repository tools".into(),
        );
    }
    let root = root
        .canonicalize()
        .map_err(|error| format!("resolve repository: {error}"))?;
    let path = root
        .join(relative)
        .canonicalize()
        .map_err(|error| format!("resolve repository path: {error}"))?;
    let canonical_relative = path
        .strip_prefix(&root)
        .map_err(|_| "path resolves outside the repository".to_string())?;
    if protected_path(canonical_relative) {
        return Err("path resolves to protected credentials or runtime state".into());
    }
    if let Some(excluded) = excluded_state_root {
        let excluded = excluded
            .canonicalize()
            .map_err(|error| format!("resolve excluded runtime state directory: {error}"))?;
        if path.starts_with(excluded) {
            return Err("configured runtime state is not exposed by repository tools".into());
        }
    }
    Ok(path)
}

fn protected_path(path: &Path) -> bool {
    let mut components = path.components().filter_map(|part| match part {
        Component::Normal(name) => Some(name.to_string_lossy().to_ascii_lowercase()),
        _ => None,
    });
    while let Some(name) = components.next() {
        if name == ".rkat" {
            if components.next().as_deref() != Some("skills") {
                return true;
            }
            continue;
        }
        if matches!(
            name.as_str(),
            ".git"
                | ".fugue"
                | ".ssh"
                | ".aws"
                | ".gnupg"
                | ".npmrc"
                | ".pypirc"
                | ".netrc"
                | "id_rsa"
                | "id_ed25519"
        ) || name == ".env"
            || name.starts_with(".env.")
            || name.ends_with(".pem")
            || name.ends_with(".key")
            || name.ends_with(".p12")
            || name.ends_with(".pfx")
            || name.ends_with(".keystore")
        {
            return true;
        }
    }
    false
}

fn read_file(
    root: &Path,
    input: ReadFile,
    excluded_state_root: Option<&Path>,
) -> Result<FileContent, String> {
    if input.limit == 0 || input.limit > MAX_READ_BYTES {
        return Err(format!(
            "limit must be between 1 and {MAX_READ_BYTES} bytes"
        ));
    }
    let path = resolve(root, &input.path, excluded_state_root)?;
    let metadata = fs::metadata(&path).map_err(|error| format!("inspect file: {error}"))?;
    if !metadata.is_file() {
        return Err("requested path is not an ordinary file".into());
    }
    if input.offset > metadata.len() {
        return Err("offset exceeds the file length".into());
    }
    let mut file = fs::File::open(&path).map_err(|error| format!("open file: {error}"))?;
    file.seek(SeekFrom::Start(input.offset))
        .map_err(|error| format!("seek file: {error}"))?;
    let mut bytes = Vec::new();
    file.take(input.limit as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read file: {error}"))?;
    // A page may end within a UTF-8 character. Leave that character for the next
    // page; byte offsets returned by this tool always stay on a text boundary.
    let length = match std::str::from_utf8(&bytes) {
        Ok(_) => bytes.len(),
        Err(error) if error.error_len().is_none() && error.valid_up_to() > 0 => error.valid_up_to(),
        Err(_) => return Err("file is not UTF-8 text, or offset/limit splits a character".into()),
    };
    bytes.truncate(length);
    if bytes.contains(&0) {
        return Err("binary files are not exposed by repository text tools".into());
    }
    let content = String::from_utf8(bytes).map_err(|error| format!("decode text: {error}"))?;
    let end = input.offset + length as u64;
    Ok(FileContent {
        path: input.path,
        offset: input.offset,
        next_offset: (end < metadata.len()).then_some(end),
        total_bytes: metadata.len(),
        content,
    })
}

fn list_files(
    root: &Path,
    input: ListFiles,
    excluded_state_root: Option<&Path>,
) -> Result<DirectoryContent, String> {
    let path = resolve(root, &input.path, excluded_state_root)?;
    let excluded_state_root = excluded_state_root
        .map(|excluded| {
            excluded
                .canonicalize()
                .map_err(|error| format!("resolve excluded runtime state directory: {error}"))
        })
        .transpose()?;
    let directory = fs::read_dir(path).map_err(|error| format!("list directory: {error}"))?;
    let mut entries = Vec::new();
    for (index, entry) in directory.enumerate() {
        if index >= MAX_DIRECTORY_SCAN {
            return Err(format!(
                "directory exceeds the scan limit of {MAX_DIRECTORY_SCAN} entries; choose a narrower path"
            ));
        }
        let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "directory contains a filename that is not UTF-8".to_string())?;
        if protected_path(&input.path.join(&name))
            || excluded_state_root
                .as_ref()
                .is_some_and(|excluded| entry.path().starts_with(excluded))
        {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|error| format!("inspect directory entry: {error}"))?;
        let kind = if file_type.is_file() {
            EntryKind::File
        } else if file_type.is_dir() {
            EntryKind::Directory
        } else {
            continue;
        };
        entries.push(DirectoryEntry { name, kind });
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    if input.offset > entries.len() {
        return Err("offset exceeds the visible directory length; restart at offset 0".into());
    }
    let end = input
        .offset
        .saturating_add(MAX_DIRECTORY_ENTRIES)
        .min(entries.len());
    let next_offset = (end < entries.len()).then_some(end);
    let entries = entries
        .into_iter()
        .skip(input.offset)
        .take(MAX_DIRECTORY_ENTRIES)
        .collect();
    Ok(DirectoryContent {
        path: input.path,
        offset: input.offset,
        next_offset,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_file(root: &Path, input: ReadFile) -> Result<FileContent, String> {
        super::read_file(root, input, None)
    }

    fn list_files(root: &Path, input: ListFiles) -> Result<DirectoryContent, String> {
        super::list_files(root, input, None)
    }

    #[test]
    fn skills_are_readable_but_native_state_is_not() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join(".rkat/skills/review")).unwrap();
        fs::create_dir_all(root.path().join(".rkat/auth")).unwrap();
        fs::create_dir_all(root.path().join("custom-state")).unwrap();
        fs::write(
            root.path().join(".rkat/skills/review/SKILL.md"),
            "Review source and tests",
        )
        .unwrap();
        fs::write(root.path().join(".rkat/auth/config.json"), "private").unwrap();
        fs::write(root.path().join("custom-state/config.json"), "private").unwrap();
        assert_eq!(
            read_file(root.path(), read(".rkat/skills/review/SKILL.md", 0, 64))
                .unwrap()
                .content,
            "Review source and tests"
        );
        assert_eq!(
            list_files(
                root.path(),
                ListFiles {
                    path: ".rkat/skills".into(),
                    offset: 0,
                }
            )
            .unwrap()
            .entries[0]
                .name,
            "review"
        );
        assert!(read_file(root.path(), read(".rkat/auth/config.json", 0, 64)).is_err());
        let excluded = root.path().join("custom-state");
        assert!(
            super::read_file(
                root.path(),
                read("custom-state/config.json", 0, 64),
                Some(&excluded)
            )
            .is_err()
        );
        let listing = super::list_files(
            root.path(),
            ListFiles {
                path: PathBuf::new(),
                offset: 0,
            },
            Some(&excluded),
        )
        .unwrap();
        assert!(
            !listing
                .entries
                .iter()
                .any(|entry| entry.name == "custom-state")
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&excluded, root.path().join("alias")).unwrap();
            assert!(
                super::read_file(
                    root.path(),
                    read("alias/config.json", 0, 64),
                    Some(&excluded)
                )
                .is_err()
            );
        }
    }

    fn read(path: &str, offset: u64, limit: usize) -> ReadFile {
        ReadFile {
            path: PathBuf::from(path),
            offset,
            limit,
        }
    }

    #[test]
    fn utf8_source_pages_reassemble_without_losing_characters() {
        let root = tempfile::tempdir().unwrap();
        let text = format!("{}🌳last", "a".repeat(MAX_READ_BYTES - 1));
        fs::write(root.path().join("source.rs"), &text).unwrap();
        let first = read_file(root.path(), read("source.rs", 0, MAX_READ_BYTES)).unwrap();
        assert_eq!(first.content.len(), MAX_READ_BYTES - 1);
        let second = read_file(
            root.path(),
            read("source.rs", first.next_offset.unwrap(), MAX_READ_BYTES),
        )
        .unwrap();
        assert_eq!(format!("{}{}", first.content, second.content), text);
        assert!(second.next_offset.is_none());
    }

    #[test]
    fn private_state_traversal_binary_and_oversized_requests_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(".env"), "secret").unwrap();
        fs::write(root.path().join("binary"), [0, 1, 2]).unwrap();
        for path in [".env", "../other", "/etc/passwd", ".git/config"] {
            assert!(read_file(root.path(), read(path, 0, 20)).is_err());
        }
        assert!(read_file(root.path(), read("binary", 0, 20)).is_err());
        assert!(read_file(root.path(), read("binary", 0, MAX_READ_BYTES + 1)).is_err());
        let listing = list_files(
            root.path(),
            ListFiles {
                path: PathBuf::new(),
                offset: 0,
            },
        )
        .unwrap();
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].name, "binary");
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_cannot_escape_or_alias_protected_state() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("private"), "secret").unwrap();
        fs::write(root.path().join(".env"), "secret").unwrap();
        symlink(outside.path().join("private"), root.path().join("escape")).unwrap();
        symlink(root.path().join(".env"), root.path().join("alias")).unwrap();
        assert!(read_file(root.path(), read("escape", 0, 20)).is_err());
        assert!(read_file(root.path(), read("alias", 0, 20)).is_err());
        assert!(
            list_files(
                root.path(),
                ListFiles {
                    path: PathBuf::new(),
                    offset: 0,
                }
            )
            .unwrap()
            .entries
            .is_empty()
        );
    }

    #[test]
    fn directory_listing_is_sorted_and_bounded() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("z"), "").unwrap();
        fs::write(root.path().join("a"), "").unwrap();
        let listing = list_files(
            root.path(),
            ListFiles {
                path: PathBuf::new(),
                offset: 0,
            },
        )
        .unwrap();
        assert_eq!(
            listing
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "z"]
        );
        assert!(listing.next_offset.is_none());
    }

    #[test]
    fn directory_pages_reassemble_and_offsets_exclude_protected_entries() {
        let root = tempfile::tempdir().unwrap();
        let expected: Vec<_> = (0..201)
            .map(|index| format!("source-{index:03}.rs"))
            .collect();
        for name in &expected {
            fs::write(root.path().join(name), "").unwrap();
        }
        fs::write(root.path().join(".env"), "secret").unwrap();
        fs::write(root.path().join("source-100.key"), "secret").unwrap();
        let first = list_files(root.path(), serde_json::from_str("{}").unwrap()).unwrap();
        assert_eq!(first.offset, 0);
        assert_eq!(first.entries.len(), MAX_DIRECTORY_ENTRIES);
        assert_eq!(first.next_offset, Some(200));
        let second = list_files(
            root.path(),
            ListFiles {
                path: PathBuf::new(),
                offset: first.next_offset.unwrap(),
            },
        )
        .unwrap();
        assert_eq!(second.offset, 200);
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.next_offset, None);
        let names: Vec<_> = first
            .entries
            .into_iter()
            .chain(second.entries)
            .map(|entry| entry.name)
            .collect();
        assert_eq!(names, expected);
        assert!(
            list_files(
                root.path(),
                ListFiles {
                    path: PathBuf::new(),
                    offset: 202,
                }
            )
            .is_err()
        );
        assert!(serde_json::from_str::<ListFiles>(r#"{"offset":-1}"#).is_err());
        assert!(serde_json::from_str::<ListFiles>(r#"{"unknown":0}"#).is_err());
    }
}
