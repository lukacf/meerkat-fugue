Work on {{ issue.identifier }}: {{ issue.title }}.

{% if issue.description %}
{{ issue.description }}
{% endif %}

Follow repository instructions and work only inside the supplied issue workspace.
Preserve unrelated changes. The native issue team owns planning, implementation,
and review. Keep one persistent comment headed `## Fugue Workpad` with the plan,
acceptance criteria, check results, and PR handoff evidence. Use typed WorkGraph
obligations for blockers and review findings.

Implement the requested change on a separate branch. Add focused tests for the
behavior and run the repository's checks. After independent review is green,
commit and push the reviewed changes, and hand the branch to the repository
coordinator to create or update the pull request and inspect its real checks.
Record the PR URL and exact reviewed commit in the Workpad. Do not invent check
results or merge from worker agents. Merging is a separate operator action
through the configured merge lane and repository coordinator.

Report lifecycle facts through the native issue lead. Planner, coder, and
reviewer must not move tracker lanes directly. Follow the configured projection
rather than choosing status names yourself. If required access or a check is
blocked, record the concrete blocker and leave the work incomplete.
