#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedReviewFindings {
    pub section: String,
    pub summary: String,
}

pub fn extract_unresolved_review_findings(body: &str) -> Option<UnresolvedReviewFindings> {
    let lines = body.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| is_review_findings_heading(line.trim()))?;
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find_map(|(index, line)| is_markdown_section_heading(line.trim()).then_some(index))
        .unwrap_or(lines.len());
    let section = lines[start..end].join("\n");
    if !section.lines().any(is_unresolved_task_line) {
        return None;
    }
    let summary = section
        .lines()
        .find(|line| is_unresolved_task_line(line))
        .map(unchecked_task_summary)
        .filter(|summary| !summary.is_empty())
        .unwrap_or("unresolved review finding")
        .to_string();
    Some(UnresolvedReviewFindings { section, summary })
}

pub fn extract_unresolved_operator_review_note(body: &str) -> Option<String> {
    if let Some(findings) = extract_unresolved_review_findings(body) {
        return Some(findings.summary);
    }

    // The managed Workpad describes progress and policy. Only its explicit
    // unresolved findings section above is actionable; incidental keywords
    // must not turn a status update into a new operator rework request.
    if body
        .trim_start()
        .lines()
        .next()
        .is_some_and(|line| normalize_heading(line.trim()).eq_ignore_ascii_case("## Fugue Workpad"))
    {
        return None;
    }

    let lower = body.to_ascii_lowercase();
    let is_operator_review = lower.contains("operator review")
        || lower.contains("requesting changes")
        || lower.contains("changes requested")
        || lower.contains("blocking rework")
        || lower.contains("blocking issue");
    if !is_operator_review {
        return None;
    }
    if lower.contains("no blocking findings") || lower.contains("no focused rework") {
        return None;
    }

    body.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.trim_start_matches(['-', '*', ' ']).trim().to_string())
        .filter(|summary| !summary.is_empty())
        .or_else(|| Some("unresolved operator review finding".to_string()))
}

pub fn merge_preserved_review_findings(rendered_body: &str, existing_body: Option<&str>) -> String {
    let Some(existing_findings) = existing_body.and_then(extract_unresolved_review_findings) else {
        return rendered_body.to_string();
    };
    if extract_unresolved_review_findings(rendered_body).is_some() {
        return rendered_body.to_string();
    }

    let mut section = existing_findings.section;
    if !section.ends_with('\n') {
        section.push('\n');
    }

    let marker = "\n### Pull request or Handoff\n";
    if let Some(index) = rendered_body.find(marker) {
        let mut merged = String::with_capacity(rendered_body.len() + section.len() + 1);
        merged.push_str(&rendered_body[..index]);
        if !merged.ends_with("\n\n") {
            if !merged.ends_with('\n') {
                merged.push('\n');
            }
            merged.push('\n');
        }
        merged.push_str(&section);
        merged.push_str(&rendered_body[index..]);
        return merged;
    }

    let mut merged = rendered_body.to_string();
    if !merged.ends_with("\n\n") {
        if !merged.ends_with('\n') {
            merged.push('\n');
        }
        merged.push('\n');
    }
    merged.push_str(&section);
    merged
}

fn is_review_findings_heading(line: &str) -> bool {
    matches!(
        normalize_heading(line).as_str(),
        "### review findings" | "### unresolved review findings" | "### rework findings"
    )
}

fn is_markdown_section_heading(line: &str) -> bool {
    line.starts_with("## ") || line.starts_with("### ")
}

fn normalize_heading(line: &str) -> String {
    line.trim_end_matches('#')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn is_unresolved_task_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("- [ ]") || trimmed.starts_with("* [ ]")
}

fn unchecked_task_summary(line: &str) -> &str {
    let trimmed = line.trim_start();
    trimmed
        .strip_prefix("- [ ]")
        .or_else(|| trimmed.strip_prefix("* [ ]"))
        .unwrap_or(trimmed)
        .trim()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unresolved_review_findings_extract_full_section_until_next_heading() {
        let body = "### Plan\n- [x] done\n\n### Review findings\n- [ ] Source: fix behavior.\n- [ ] Tests: add coverage.\n- [ ] Docs: preserve note.\n\n### Validation\n- [ ] cargo test\n";

        let findings = extract_unresolved_review_findings(body).unwrap();

        assert!(findings.section.contains("Source: fix behavior"));
        assert!(findings.section.contains("Tests: add coverage"));
        assert!(findings.section.contains("Docs: preserve note"));
        assert!(!findings.section.contains("### Validation"));
        assert_eq!(findings.summary, "Source: fix behavior.");
    }

    #[test]
    fn completed_review_findings_are_not_preserved_as_unresolved() {
        let body = "### Review findings\n- [x] Source: addressed.\n";

        assert!(extract_unresolved_review_findings(body).is_none());
    }

    #[test]
    fn merge_preserves_unresolved_review_findings_once_before_handoff() {
        let rendered = "### Plan\n- [x] launched\n\n### Pull request or Handoff\n- [ ] pending\n";
        let existing = "### Review findings\n- [ ] Source: fix behavior.\n";

        let merged = merge_preserved_review_findings(rendered, Some(existing));
        let merged_again = merge_preserved_review_findings(&merged, Some(existing));

        assert!(merged.contains("### Review findings\n- [ ] Source: fix behavior."));
        assert_eq!(merged.matches("### Review findings").count(), 1);
        assert_eq!(merged, merged_again);
    }

    #[test]
    fn workpad_narrative_is_not_an_operator_review_finding() {
        let body = "## Fugue Workpad\n\n### Current review\n- Native admission accepted; operator review findings will be handled if received.\n- No current code defect.\n";
        for prefix in ["", "\n\n", "  \n\t"] {
            assert!(extract_unresolved_operator_review_note(&format!("{prefix}{body}")).is_none());
        }
    }

    #[test]
    fn workpad_explicit_unresolved_findings_remain_actionable() {
        let body =
            "## Fugue Workpad\n\n### Review findings\n- [ ] Fix the Unicode boundary regression.\n";
        assert_eq!(
            extract_unresolved_operator_review_note(body).as_deref(),
            Some("Fix the Unicode boundary regression.")
        );
    }

    #[test]
    fn operator_review_comment_counts_as_unresolved_finding() {
        let body = "Operator review for PR #186: not accepted yet. Blocking rework: derive lanes from config before review-ready handoff.";

        let finding = extract_unresolved_operator_review_note(body).unwrap();

        assert!(finding.contains("Operator review"));
    }

    #[test]
    fn multiline_operator_review_after_rework_counts_as_unresolved_finding() {
        let body = "Operator review for PR #187 after rework: not accepted yet.\n\nBlocking findings:\n\n1. `src/orchestrator.rs:757` only computes the final gate request.\n\nRequired rework:\n\n- Wire the final gate into the real Kapellmeister/control-plane path.\n";

        let finding = extract_unresolved_operator_review_note(body).unwrap();

        assert_eq!(
            finding,
            "Operator review for PR #187 after rework: not accepted yet."
        );
    }

    #[test]
    fn green_review_summary_does_not_count_as_unresolved_operator_finding() {
        let body = "- [x] Code review findings: no blocking findings. No focused rework items.";

        assert!(extract_unresolved_operator_review_note(body).is_none());
    }
}
