use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::tools::IdempotencyKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ImsIssueId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ImsIssueKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ImsStageName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ImsCommentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ImsRepoScope(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImsRepoIssueQuery {
    pub repo: ImsRepoScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImsIssueRef {
    pub key: ImsIssueKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImsIssueSnapshot {
    pub id: ImsIssueId,
    pub key: ImsIssueKey,
    pub title: String,
    pub description: Option<String>,
    pub stage: ImsStageName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImsCommentSnapshot {
    pub id: ImsCommentId,
    pub body_mark: String,
    #[serde(default)]
    pub attachments: Vec<ImsCommentAttachmentSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImsCommentAttachmentSnapshot {
    pub title: Option<String>,
    pub url: String,
    pub path: Option<String>,
    pub content_type: Option<String>,
    pub byte_len: u64,
}

#[async_trait]
pub trait IssueManagementSystem {
    async fn list_repo_issues(&self, query: &ImsRepoIssueQuery) -> Result<Vec<ImsIssueSnapshot>>;
    async fn issue(&self, issue: &ImsIssueRef) -> Result<ImsIssueSnapshot>;
    async fn comments(&self, issue: &ImsIssueRef) -> Result<Vec<ImsCommentSnapshot>>;
    async fn move_issue(
        &self,
        issue: &ImsIssueRef,
        stage: &ImsStageName,
        idempotency_key: IdempotencyKey,
    ) -> Result<ImsIssueSnapshot>;
    async fn add_comment(&self, issue: &ImsIssueRef, body: &str) -> Result<ImsCommentSnapshot>;
    async fn add_comment_with_attachments(
        &self,
        issue: &ImsIssueRef,
        body: &str,
        attachments: Vec<ImsCommentAttachmentSnapshot>,
    ) -> Result<ImsCommentSnapshot>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImsStageClassification {
    Parked,
    Intake,
    ProjectedActive,
    ControlActive,
    AttentionOrTerminal,
    UnexpectedNonterminal,
}

impl ImsStageClassification {
    pub fn should_report_first_seen_without_runtime(self) -> bool {
        matches!(
            self,
            Self::Intake
                | Self::ControlActive
                | Self::ProjectedActive
                | Self::UnexpectedNonterminal
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImsStageObservation {
    Unchanged,
    FirstSeen {
        identifier: String,
        current: String,
    },
    Changed {
        identifier: String,
        previous: String,
        current: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImsCommentObservation<CommentId> {
    FirstSeen,
    Unchanged,
    Changed {
        changes: Vec<ImsCommentChange<CommentId>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImsCommentChange<CommentId> {
    Added {
        comment_id: CommentId,
        body_markdown: String,
    },
    Changed {
        comment_id: CommentId,
        body_markdown: String,
    },
    Deleted {
        comment_id: CommentId,
    },
}
