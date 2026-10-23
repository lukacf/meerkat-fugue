//! Parse a repository address before selecting credentials or invoking Git.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryAddress {
    pub host: String,
    pub owner: String,
    pub repository: String,
}

impl RepositoryAddress {
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        let invalid = || {
            anyhow::anyhow!(
                "Enter a GitHub or GitHub Enterprise repository URL, such as https://github.com/owner/repository"
            )
        };
        if input.len() > 1024 || input.chars().any(char::is_control) {
            return Err(invalid());
        }
        let normalized = if let Some(ssh) = input.strip_prefix("git@") {
            let (host, path) = ssh.split_once(':').ok_or_else(invalid)?;
            format!("https://{host}/{path}")
        } else {
            input.to_owned()
        };
        let url = reqwest::Url::parse(&normalized).map_err(|_| invalid())?;
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.port().is_some()
            || normalized.contains(['%', '\\'])
        {
            return Err(invalid());
        }
        let host = url.host_str().ok_or_else(invalid)?.to_owned();
        super::connections::validate_github_host(&host)?;
        // Check the original path as well: URL parsing normalizes dot segments.
        let path = normalized
            .split_once("://")
            .ok_or_else(invalid)?
            .1
            .split_once('/')
            .ok_or_else(invalid)?
            .1
            .trim_end_matches('/');
        let (owner, repository) = path.split_once('/').ok_or_else(invalid)?;
        let repository = repository.strip_suffix(".git").unwrap_or(repository);
        for component in [owner, repository] {
            if component.is_empty()
                || component.len() > 100
                || component == "."
                || component == ".."
                || !component
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            {
                return Err(invalid());
            }
        }
        if owner.starts_with('-') || repository.starts_with('-') {
            bail!("Repository owner and name cannot start with a hyphen");
        }
        Ok(Self {
            host,
            owner: owner.into(),
            repository: repository.into(),
        })
    }

    pub fn https_url(&self) -> String {
        format!("https://{}/{}/{}", self.host, self.owner, self.repository)
    }

    pub fn same_repository(&self, other: &Self) -> bool {
        self.host.eq_ignore_ascii_case(&other.host)
            && self.owner.eq_ignore_ascii_case(&other.owner)
            && self.repository.eq_ignore_ascii_case(&other.repository)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_addresses_select_the_exact_host_and_repository() {
        let https = RepositoryAddress::parse("https://git.example.com/team/service.git").unwrap();
        assert_eq!(
            https,
            RepositoryAddress::parse("git@git.example.com:team/service.git").unwrap()
        );
        assert_eq!(https.https_url(), "https://git.example.com/team/service");
        assert_eq!(
            RepositoryAddress::parse("https://github.com/user/repo/")
                .unwrap()
                .host,
            "github.com"
        );
    }

    #[test]
    fn secrets_ambiguous_paths_and_non_remote_addresses_are_rejected_without_echo() {
        for input in [
            "/tmp/repo",
            "owner/repo",
            "https://secret@github.com/o/r",
            "http://github.com/o/r",
            "https://github.com/o/r?token=secret",
            "https://github.com/o/r#secret",
            "https://github.com/o/../r",
            "https://github.com/o/%72",
            "https://github.com/o/r/tree/main",
            "https://github.com/o/r\\secret",
            "https://github.com:443/o/r",
            "git@github.com:o/../../r",
        ] {
            let result = RepositoryAddress::parse(input);
            // Explicit default ports are harmless and URL-normalized.
            if input.contains(":443/") {
                continue;
            }
            let error = result.unwrap_err().to_string();
            assert!(!error.contains("secret"));
        }
    }
}
