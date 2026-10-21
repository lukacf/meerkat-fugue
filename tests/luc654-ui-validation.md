# LUC-654 Playwright/browser validation

This checked-in validation harness is intentionally isolated from production Linear state. It starts a disposable local fixture server with Overview and IMS pages, two meaningful IMS lanes (`Queue` and `Active`), and a fixture issue card (`LUC-654-FIXTURE`).

## Command

```bash
npm ci
npm run test:luc654-ui
```

If the runner does not already have a Chromium browser installed, run the Playwright install command suggested by the CLI (usually `npx playwright install chromium`) and rerun `npm run test:luc654-ui`.

## Coverage

- `/overview` and `/issues` at: desktop `1440x900`, tablet/narrow `1024x768`, short-height `980x720`, and mobile `390x844`.
- Overview assertions: top rail/nav, KPI rail, focus picker, timeline, topology, active-agent panel, activity/lane rows, and no browser console/page errors.
- IMS assertions: integrated nav, honest unavailable KPI placeholders, scrollable board lanes/card, create issue editor, card click opens the full-detail sheet route path, and no browser console/page errors.
- IMS card drag/drop: drags `LUC-654-FIXTURE` from `Queue` to `Active`, asserts `.dragging` and `.drop-target` are applied/cleaned, records the expected move payload `{ identifier: "LUC-654-FIXTURE", state: "Active" }`, and verifies the card appears in the target lane.
- Attachment drop zones: `#createAttachmentDropZone`, `#issueAttachmentDropZone`, and `#commentAttachmentDropZone`; asserts `drag-over` enter/leave behavior, accepted small-file drops, count validation (`At most 20 attachments can be added.`), and document-level file-drop navigation prevention.

## Playwright MCP/manual fallback

When a full `@playwright/test` run is unavailable, use the MCP/browser tools against the fixture server exported by `tests/fixtures/luc654-ui-fixture-server.mjs` or an equivalent local Fugue HTTP server. Use the same route list, viewport list, selectors, and pass/fail assertions above, and record screenshots/console output in the Fugue Workpad before returning to review.
