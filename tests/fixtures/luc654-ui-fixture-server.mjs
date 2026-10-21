import http from 'node:http';
import { once } from 'node:events';

const lanes = ['Backlog', 'Queue', 'Active', 'Review', 'Done', 'Canceled', 'Duplicate'];
const issue = {
  identifier: 'LUC-654-FIXTURE',
  title: 'Fixture issue card',
  lane: 'Queue',
};

const pageShell = (title, body, script = '') => `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title}</title>
<style>
:root { --bg:#ECEAE4; --panel:#FAF8F2; --soft:#F6F4EE; --ink:#1C1A17; --muted:#6B6760; --line:#D6D2C8; --accent:#D95A1E; --ok:#3E6B4F; font-family: Inter, system-ui, sans-serif; }
* { box-sizing:border-box; }
body { margin:0; min-height:100vh; overflow:auto; background:var(--bg); color:var(--ink); font-size:13px; }
a { color:inherit; }
.top-rail { position:sticky; top:0; z-index:5; min-height:38px; display:flex; align-items:stretch; gap:0; border-bottom:1px solid var(--line); background:var(--panel); }
.brand { display:flex; align-items:center; gap:8px; min-width:210px; padding:0 14px; text-decoration:none; font-weight:700; }
.mark { width:14px; height:14px; border:2px solid var(--accent); display:inline-block; }
.tabs { display:flex; align-items:stretch; overflow-x:auto; }
.tabs a { display:flex; align-items:center; min-height:38px; padding:0 14px; border-left:1px solid var(--line); text-decoration:none; white-space:nowrap; }
.tabs a.active { background:var(--soft); color:var(--accent); }
.spacer { flex:1; }
.kpis { display:flex; min-height:38px; flex:0 1 auto; overflow-x:auto; }
.kpis span { display:flex; align-items:center; gap:4px; min-width:max-content; padding:0 10px; border-left:1px solid var(--line); color:var(--muted); }
.panel { border:1px solid var(--line); background:var(--panel); min-width:0; overflow:auto; }
.eyebrow, .meta { color:var(--muted); font:11px/1.35 ui-monospace, SFMono-Regular, Menlo, monospace; letter-spacing:.04em; text-transform:uppercase; }
button, input, textarea, select { font:inherit; }
button { border:1px solid var(--line); background:var(--panel); min-height:34px; cursor:pointer; }
button.primary { background:var(--accent); color:var(--bg); border-color:var(--accent); }
.attachment-drop-zone { border:1px dashed var(--line); background:var(--panel); padding:12px; transition:border-color 120ms ease, background 120ms ease; }
.attachment-drop-zone.drag-over { border-color:var(--accent); background:color-mix(in oklch, var(--accent) 8%, var(--panel)); }
.attachment-drop-affordance { padding:8px; text-align:center; color:var(--muted); border:1px dashed var(--line); margin-top:8px; }
@media (max-height: 720px), (max-width: 980px) { .top-rail { flex-wrap:wrap; } .kpis { order:2; flex:1 0 100%; border-top:1px solid var(--line); } }
@media (max-width: 720px) { .brand { min-width:auto; } .spacer { display:none; } }
${title.includes('Overview') ? overviewStyles() : imsStyles()}
</style>
</head>
<body>${body}<script>${commonScript()}</script><script>${script}</script></body>
</html>`;

function overviewStyles() {
  return `.picker { min-height:64px; display:flex; flex-wrap:wrap; align-items:stretch; gap:8px; padding:12px; border-bottom:1px solid var(--line); }
.mob-tab { flex:1 1 240px; min-width:220px; min-height:64px; padding:10px; background:var(--panel); border:1px solid var(--line); text-decoration:none; }
.hero { min-height:520px; display:grid; grid-template-rows:auto auto minmax(360px,1fr); gap:12px; padding:12px; }
.identity { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
.identity h1 { margin:0; font-size:16px; font-weight:600; flex:1 1 260px; }
.timeline { display:grid; grid-template-columns:repeat(4,minmax(180px,1fr)); gap:8px; overflow:auto; }
.phase { min-height:54px; padding:10px; border:1px solid var(--line); background:var(--panel); }
.hero-body { min-height:0; display:grid; grid-template-columns:minmax(420px,1fr) minmax(280px,340px); gap:12px; }
.flow { padding:24px; min-width:0; overflow:auto; }
.flow svg { width:100%; height:100%; min-width:520px; min-height:260px; }
.agent-card { min-width:0; padding:18px; overflow:auto; border-bottom:1px solid var(--line); }
.lower { min-height:220px; display:grid; grid-template-columns:minmax(0,1.4fr) minmax(280px,1fr); gap:12px; padding:0 12px 12px; }
.log-row { display:grid; grid-template-columns:minmax(64px,74px) minmax(60px,72px) minmax(0,1fr); gap:8px; }
.lane-row { display:grid; grid-template-columns:28px minmax(0,1fr) minmax(70px,88px) auto; gap:8px; }
@media (max-width: 980px) { .hero-body, .lower { grid-template-columns:1fr; } }
@media (max-width: 720px) { .timeline { grid-template-columns:repeat(4,minmax(150px,1fr)); } .hero { padding:8px; } }`;
}

function imsStyles() {
  return `.ims-shell { display:grid; grid-template-rows:auto 1fr; min-height:calc(100vh - 38px); }
.toolbar { display:flex; flex-wrap:wrap; gap:8px; align-items:center; padding:10px 12px; border-bottom:1px solid var(--line); background:var(--soft); }
.board { display:grid; grid-auto-flow:column; grid-auto-columns:minmax(270px,1fr); gap:8px; align-items:stretch; overflow:auto; padding:12px; min-height:58vh; }
.lane { min-width:270px; display:flex; flex-direction:column; gap:8px; border:1px solid var(--line); background:var(--soft); padding:8px; }
.lane.drop-target { outline:2px solid var(--accent); outline-offset:-2px; }
.card { display:block; width:100%; text-align:left; border:1px solid var(--line); background:var(--panel); padding:10px; }
.card.dragging { opacity:.48; }
.create-panel { padding:12px; border-top:1px solid var(--line); background:var(--soft); }
.create-form { display:grid; gap:10px; max-width:760px; }
.create-primary-fields { display:grid; grid-template-columns:minmax(0,1fr) minmax(160px,220px); gap:8px; }
.issue-sheet { position:fixed; inset:48px 24px 24px; display:grid; grid-template-columns:minmax(220px,280px) minmax(0,1fr); gap:12px; padding:12px; border:1px solid var(--line); background:var(--bg); overflow:auto; }
.issue-sheet[hidden] { display:none; }
.lane-row { width:100%; min-height:50px; text-align:left; }
.comment-zone { margin-top:12px; }
@media (max-width: 720px) { .board { grid-auto-columns:minmax(270px,88vw); } .create-primary-fields, .issue-sheet { grid-template-columns:1fr; } .issue-sheet { inset:42px 8px 8px; } }`;
}

function commonRail(active) {
  return `<div class="top-rail" data-fugue-console-chrome>
    <a class="brand" href="/overview" aria-label="Meerkat Fugue overview"><span class="mark" aria-hidden="true"></span><span>MEERKAT FUGUE</span></a>
    <nav class="tabs" aria-label="Primary app navigation"><a class="${active === 'overview' ? 'active' : ''}" href="/overview">OVERVIEW</a><a class="${active === 'issues' ? 'active' : ''}" href="/issues">IMS</a><a href="/console">CONSOLE</a></nav>
    <div class="spacer"></div><div class="kpis" aria-label="Fugue KPIs"><span>RUN <b>1</b></span><span>RETRY <b>0</b></span><span>TOK <b>unavailable</b></span><span>T <b>unavailable</b></span><span>SHAD <b>unavailable</b></span></div>
  </div>`;
}

function overviewPage() {
  return pageShell('Fugue Overview', `${commonRail('overview')}
    <section class="picker" aria-label="Focus picker"><div><div class="eyebrow">FOCUS MOB</div><strong>1</strong><span class="meta"> in flight</span></div><a class="mob-tab" href="/issues/${issue.identifier}"><span class="meta">${issue.identifier} ACTIVE</span><div>${issue.title}</div></a><a class="mob-tab" href="/issues">◇ FOCUS OTHER</a></section>
    <main class="hero"><section class="identity"><span class="eyebrow">${issue.identifier} · ACTIVE</span><h1>${issue.title}</h1><span class="meta">RUNTIME · CODER</span><a class="meta" href="/issues">OPEN IMS</a></section><section class="timeline" aria-label="Issue timeline"><div class="phase">planning</div><div class="phase">implementing</div><div class="phase">reviewing</div><div class="phase">finalizing</div></section><section class="hero-body"><div class="flow panel"><svg viewBox="0 0 760 300" role="img" aria-label="actual Fugue mob topology"><rect x="306" y="174" width="148" height="70" fill="#F5E6DF" stroke="#D95A1E"/><text x="322" y="210">CODER</text></svg></div><aside class="panel"><div class="agent-card"><span class="eyebrow">ACTIVE AGENT</span><h2>Coder</h2><p>Browser validation fixture active.</p></div></aside></section></main>
    <section class="lower"><div class="panel"><div class="log-row"><span>now</span><span>${issue.identifier}</span><span>Fixture event</span></div></div><div class="panel"><div class="lane-row"><span>▶</span><span>Queue</span><span>1</span><span>ready</span></div></div></section>`);
}

function issuesPage() {
  const laneHtml = lanes.map((lane) => `<section class="lane" data-lane="${lane}" aria-label="${lane} lane"><h2>${lane}</h2><div class="lane-cards">${lane === issue.lane ? cardHtml(issue) : ''}</div></section>`).join('');
  const body = `${commonRail('issues')}<main class="ims-shell"><section class="toolbar"><button id="newIssue" class="primary">+ NEW</button><input id="filter" aria-label="Filter issues" placeholder="to search…"><span class="meta">grouped by lane · sorted by updated</span></section><section class="board" id="board" aria-label="Issue board">${laneHtml}</section><section class="create-panel"><form class="create-form issue-editor-form" id="createForm"><div class="create-primary-fields"><label>Title <input id="createTitle" required></label><label>Lane <select id="createLane">${lanes.map((lane) => `<option value="${lane}">${lane}</option>`).join('')}</select></label></div><div class="attachment-drop-zone" id="createAttachmentDropZone" data-attachment-drop-zone><textarea id="createDescription" class="issue-markdown-editor" placeholder="Write Markdown"></textarea><label>Attach files <input id="createAttachments" type="file" multiple></label><div class="attachment-drop-affordance">Drop files here to attach them</div><div id="createAttachmentList" aria-live="polite"></div><div id="createError" role="alert"></div></div><button id="createSubmit" class="primary" type="submit">Create issue</button></form></section></main>${issueSheet()}`;
  return pageShell('Fugue IMS', body, imsScript());
}

function cardHtml(row) {
  return `<button class="card" data-issue="${row.identifier}" draggable="true" type="button"><span class="meta">${row.identifier}</span><h3>${row.title}</h3><p>Fixture card for browser validation.</p></button>`;
}

function issueSheet() {
  return `<section class="issue-sheet" id="issueSheet" hidden aria-label="Issue detail sheet"><aside><a href="/issues">← IMS</a><h1>${issue.identifier}</h1><button class="lane-row" data-lane="Active">Move target: Active</button></aside><article><div class="attachment-drop-zone" id="issueAttachmentDropZone" data-attachment-drop-zone><label>Issue attachments <input id="issueAttachmentInput" type="file" multiple></label><div class="attachment-drop-affordance">Drop files here to attach them to this issue</div><div id="issueAttachmentList" aria-live="polite"></div><div id="issueAttachmentError" role="alert"></div></div><div class="attachment-drop-zone comment-zone" id="commentAttachmentDropZone" data-attachment-drop-zone><textarea id="commentEditor" placeholder="Comment"></textarea><label>Comment attachments <input id="commentAttachmentInput" type="file" multiple></label><div class="attachment-drop-affordance">Drop files here to attach them to this comment</div><div id="commentAttachmentList" aria-live="polite"></div><div id="commentError" role="alert"></div><button id="postComment">Post comment</button></div></article></section>`;
}

function commonScript() {
  return `window.__luc654 = window.__luc654 || { moves: [], uploads: [], dropsPrevented: 0, consoleErrors: [] };
  window.addEventListener('error', (event) => window.__luc654.consoleErrors.push(event.message));`;
}

function imsScript() {
  return `const board = document.getElementById('board');
  let dragging = null;
  const hasFiles = (event) => Array.from(event.dataTransfer?.types || []).includes('Files');
  const validateFiles = (files) => { const list = Array.from(files || []); const total = list.reduce((sum, file) => sum + file.size, 0); if (list.length > 20) throw new Error('At most 20 attachments can be added.'); if (list.some((file) => file.size > 25 * 1024 * 1024)) throw new Error('25 MiB per-file attachment limit'); if (total > 100 * 1024 * 1024) throw new Error('100 MiB total attachment limit'); return list; };
  const installAttachmentDropTarget = (zone, listId, errorId) => { const list = document.getElementById(listId); const error = document.getElementById(errorId); const setActive = (active) => zone.classList.toggle('drag-over', active); for (const type of ['dragenter', 'dragover']) zone.addEventListener(type, (event) => { if (!hasFiles(event)) return; event.preventDefault(); event.stopPropagation(); event.dataTransfer.dropEffect = 'copy'; setActive(true); }); zone.addEventListener('dragleave', () => setActive(false)); zone.addEventListener('drop', (event) => { if (!hasFiles(event)) return; event.preventDefault(); event.stopPropagation(); setActive(false); try { const files = validateFiles(event.dataTransfer.files); window.__luc654.uploads.push({ zone: zone.id, files: files.map((file) => file.name) }); list.textContent = files.length ? files.map((file) => file.name).join(', ') : ''; error.textContent = files.length ? 'Attached ' + files.length + ' file(s).' : ''; } catch (caught) { error.textContent = caught.message; } }); };
  for (const [zone, list, error] of [['createAttachmentDropZone','createAttachmentList','createError'], ['issueAttachmentDropZone','issueAttachmentList','issueAttachmentError'], ['commentAttachmentDropZone','commentAttachmentList','commentError']]) installAttachmentDropTarget(document.getElementById(zone), list, error);
  document.addEventListener('dragover', (event) => { if (hasFiles(event)) { event.preventDefault(); window.__luc654.dropsPrevented += 1; } }, true);
  document.addEventListener('drop', (event) => { if (hasFiles(event)) { event.preventDefault(); window.__luc654.dropsPrevented += 1; } }, true);
  board.addEventListener('click', (event) => { const card = event.target.closest('.card'); if (!card) return; window.history.pushState({}, '', '/issues/' + encodeURIComponent(card.dataset.issue)); document.getElementById('issueSheet').hidden = false; });
  board.addEventListener('dragstart', (event) => { const card = event.target.closest('.card'); if (!card) return; dragging = card.dataset.issue; card.classList.add('dragging'); event.dataTransfer.effectAllowed = 'move'; event.dataTransfer.setData('text/plain', dragging); });
  board.addEventListener('dragend', () => { dragging = null; document.querySelectorAll('.dragging, .drop-target').forEach((node) => node.classList.remove('dragging', 'drop-target')); });
  board.addEventListener('dragover', (event) => { const lane = event.target.closest('.lane'); if (!lane || !dragging) return; event.preventDefault(); lane.classList.add('drop-target'); });
  board.addEventListener('dragleave', (event) => { const lane = event.target.closest('.lane'); if (lane && !lane.contains(event.relatedTarget)) lane.classList.remove('drop-target'); });
  board.addEventListener('drop', async (event) => { const lane = event.target.closest('.lane'); if (!lane || !dragging) return; event.preventDefault(); const identifier = dragging; const targetLane = lane.dataset.lane; window.__luc654.moves.push({ identifier, state: targetLane }); const card = board.querySelector('.card[data-issue="' + identifier + '"]'); lane.querySelector('.lane-cards').appendChild(card); document.querySelectorAll('.dragging, .drop-target').forEach((node) => node.classList.remove('dragging', 'drop-target')); dragging = null; });`;
}

export async function startFixtureServer() {
  const requests = [];
  const server = http.createServer((request, response) => {
    requests.push({ method: request.method, url: request.url });
    if (request.url === '/' || request.url === '/overview') {
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end(overviewPage());
    } else if (request.url === '/issues' || request.url?.startsWith('/issues/')) {
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end(issuesPage());
    } else if (request.url === '/api/v1/issues/board') {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ lanes: lanes.map((lane) => ({ name: lane, issues: lane === issue.lane ? [issue] : [] })), totals: { issues: 1, running: 1, retrying: 0 } }));
    } else {
      response.writeHead(404, { 'content-type': 'text/plain' });
      response.end('not found');
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  return { server, requests, url: `http://127.0.0.1:${address.port}` };
}
