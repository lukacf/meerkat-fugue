# Security policy

## Reporting a vulnerability

Please use [GitHub private vulnerability reporting](https://github.com/lukacf/meerkat-fugue/security/advisories/new)
to report a suspected vulnerability. Include the affected revision, a minimal
reproduction, expected and actual behavior, and the impact. If private reporting
is unavailable, contact the repository maintainer through their GitHub profile
before sharing sensitive details.

Do not post credentials, private source, exploit details, or unredacted agent
transcripts in a public issue. If a credential has been exposed, revoke or rotate
it with its provider; removing it from a file does not remove it from history.

## Supported versions

Fugue is under active development. Security fixes target the latest `main`
revision; older snapshots do not have a separate maintenance commitment.

## Deployment boundary

Fugue executes enabled agent tools and repository hooks with the service user's
permissions. It is not an OS sandbox. Run it on a trusted host, use credentials
with appropriate scope, and review the tools granted to agents. Treat repository
content, issues, and peer messages as untrusted input.

Keep operator endpoints local or behind authenticated access. Persistent state,
agent memory, and logs can contain source code and operational information;
protect their storage and backups accordingly.
