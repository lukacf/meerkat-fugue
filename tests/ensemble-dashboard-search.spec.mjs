import {browserDocument} from './browser-document.mjs';
import { test, expect } from '@playwright/test';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { once } from 'node:events';

const FRONTEND_MEMBER_LABEL = 'Frontend Surface';
const DASHBOARD_PATH = new URL('../src/ensemble/dashboard.html', import.meta.url);

const fixture = {
  name: 'Searchable Ensemble Fixture',
  repositories: {
    api: { path: '/workspaces/api-service', description: 'Backend service repository' },
    web: { path: '/workspaces/web-client', description: 'Frontend web repository' },
  },
  runtime: {
    peers: [{ from: 'api', to: 'web' }],
    repositories: [
      repository('api', 'API service coordinates request routing.', [
        member('representative', 'API Representative', 'Coordinate API specialists and repository federation.', []),
        member('backend-contracts', 'Backend Contracts', 'Own REST API schema, handlers, and compatibility.', ['src/api.rs', 'docs/api.md']),
        member('storage-runtime', 'Storage Runtime', 'Own database migrations and persistence behavior.', ['src/storage.rs', 'migrations/']),
      ]),
      repository('web', 'Web client renders the operator cockpit.', [
        member('representative', 'Web Representative', 'Coordinate frontend specialists and repository federation.', []),
        member('frontend-surface', FRONTEND_MEMBER_LABEL, 'Own frontend filtering, accessible search, and cockpit rendering.', ['src/App.tsx', 'tests/ui/']),
        member('web-persistence', 'Web Persistence', 'Own browser cache, local storage, and offline behavior.', ['src/storage.ts']),
      ]),
    ],
  },
};

let server;
let baseUrl;

test.beforeAll(async () => {
  const dashboard = (await readFile(DASHBOARD_PATH, 'utf8')).replace('</head>', `<style>${await readFile(new URL('../src/ui/app.css', import.meta.url), 'utf8')}</style></head>`);
  server = http.createServer((request, response) => {
    const url = new URL(request.url, 'http://127.0.0.1');
    if (url.pathname === '/' || url.pathname === '/ensemble') {
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
      response.end(browserDocument(dashboard));
      return;
    }
    if (url.pathname === '/api/workspaces') { response.writeHead(200, {'content-type':'application/json'});response.end(JSON.stringify({hub:null,workspaces:[]}));return; }
    if (url.pathname === '/api/v1/workspace') {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({tracker:'Fixture',project:'Test',repositories:[]}));
      return;
    }
    if (url.pathname === '/api/ensemble') {
      response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      response.end(JSON.stringify(fixture));
      return;
    }
    if (url.pathname.startsWith('/api/repos/') && url.pathname.endsWith('/messages')) {
      response.writeHead(202, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ repo: 'api', identity: 'api:representative', session_id: 'session-1', interaction_id: 'interaction-1' }));
      return;
    }
    if (url.pathname.startsWith('/repos/') && url.pathname.endsWith('/console')) {
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end('<!doctype html><title>console</title>');
      return;
    }
    response.writeHead(404, { 'content-type': 'text/plain' });
    response.end('not found');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

test.afterAll(async () => {
  await new Promise((resolve) => server.close(resolve));
});

test.beforeEach(async ({ page }) => {
  const consoleErrors = [];
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('pageerror', (error) => consoleErrors.push(error.message));
  await page.goto(`${baseUrl}/ensemble`);
  await expect(page.getByLabel('Search repository teams')).toBeVisible();
  await expect(page.locator('.repo')).toHaveCount(2);
  await expect.poll(() => consoleErrors).toEqual([]);
});

test.describe('Ensemble dashboard team search behavior', () => {
  test('typing a repository match shows the whole team', async ({ page }) => {
    await page.getByLabel('Search repository teams').fill('api');

    await expect(page.locator('.repo')).toHaveCount(1);
    const apiCard = repoCard(page, 'api');
    await expect(apiCard).toBeVisible();
    await expect(apiCard.locator('details.specialist')).toHaveCount(3);
    await expect(apiCard).toContainText('API Representative');
    await expect(apiCard).toContainText('Backend Contracts');
    await expect(apiCard).toContainText('Storage Runtime');
    await expect(apiCard.locator('.match-chip')).toContainText('Repository match · showing whole team');
    await expect(page.locator('#matchSummary')).toContainText('Showing 1 of 2 repositories and 3 of 6 specialists');
  });

  test('specialist-only search narrows to matching members', async ({ page }) => {
    await page.getByLabel('Search repository teams').fill('accessible search');

    await expect(page.locator('.repo')).toHaveCount(1);
    const webCard = repoCard(page, 'web');
    await expect(webCard).toBeVisible();
    await expect(webCard.locator('details.specialist')).toHaveCount(1);
    await expect(webCard).toContainText(FRONTEND_MEMBER_LABEL);
    await expect(webCard).not.toContainText('Web Representative');
    await expect(webCard).not.toContainText('Web Persistence');
    await expect(webCard.locator('.match-chip')).toContainText('1 matching specialist');
    await expect(page.locator('#matchSummary')).toContainText('Showing 1 of 2 repositories and 1 of 6 specialists');
  });

  test('query and expanded specialist survive refresh', async ({ page }) => {
    const search = page.getByLabel('Search repository teams');
    await search.fill('accessible search');
    const frontendDetails = repoCard(page, 'web').locator('details.specialist').filter({ hasText: FRONTEND_MEMBER_LABEL });
    await frontendDetails.locator('summary').click();
    await expect(frontendDetails).toHaveJSProperty('open', true);

    await page.evaluate(() => window.refresh());

    await expect(search).toHaveValue('accessible search');
    await expect(repoCard(page, 'web').locator('details.specialist').filter({ hasText: FRONTEND_MEMBER_LABEL })).toHaveJSProperty('open', true);
    await expect(page.locator('.repo')).toHaveCount(1);
  });

  test('restores q on initial load and updates q without history spam', async ({ page }) => {
    await page.goto(`${baseUrl}/ensemble?view=teams&q=api&mode=full#roster`);
    const search = page.getByLabel('Search repository teams');
    await expect(search).toHaveValue('api');
    await expect(page.locator('.repo')).toHaveCount(1);
    await expect(repoCard(page, 'api')).toBeVisible();
    expect(new URL(page.url()).searchParams.get('view')).toBe('teams');
    expect(new URL(page.url()).hash).toBe('#roster');

    const historyLength = await page.evaluate(() => history.length);
    await search.fill('ą accessible search & <tag>');
    let url = new URL(page.url());
    expect(url.searchParams.get('q')).toBe('ą accessible search & <tag>');
    expect(url.searchParams.get('view')).toBe('teams');
    expect(url.searchParams.get('mode')).toBe('full');
    expect(url.hash).toBe('#roster');
    await expect(page.locator('.no-results')).toContainText('No repository teams match your search');
    expect(await page.evaluate(() => history.length)).toBe(historyLength);

    await search.fill('accessible search');
    url = new URL(page.url());
    expect(url.searchParams.get('q')).toBe('accessible search');
    await expect(page.locator('.repo')).toHaveCount(1);
    await expect(repoCard(page, 'web').locator('details.specialist')).toHaveCount(1);
  });

  test('empty filter removes q and browser back and forward restore bookmarkable query', async ({ page }) => {
    await page.goto(`${baseUrl}/ensemble?view=teams&q=api#roster`);
    const search = page.getByLabel('Search repository teams');
    await expect(search).toHaveValue('api');
    await expect(page.locator('.repo')).toHaveCount(1);

    await page.evaluate(() => {
      const url = new URL(window.location.href);
      url.searchParams.set('q', 'accessible search');
      history.pushState(null, '', `${url.pathname}${url.search}${url.hash}`);
    });
    await page.evaluate(() => dispatchEvent(new PopStateEvent('popstate')));
    await expect(search).toHaveValue('accessible search');
    await expect(repoCard(page, 'web').locator('details.specialist')).toHaveCount(1);

    await page.goBack();
    await expect(search).toHaveValue('api');
    await expect(page.locator('.repo')).toHaveCount(1);

    await page.goForward();
    await expect(search).toHaveValue('accessible search');
    await expect(repoCard(page, 'web').locator('details.specialist')).toHaveCount(1);

    await page.getByRole('button', { name: 'Clear' }).click();
    const url = new URL(page.url());
    expect(url.searchParams.get('q')).toBeNull();
    expect(url.searchParams.get('view')).toBe('teams');
    expect(url.hash).toBe('#roster');
    await expect(search).toHaveValue('');
    await expect(page.locator('.repo')).toHaveCount(2);
  });

  test('clear and no-results states remain keyboard accessible', async ({ page }) => {
    const search = page.getByLabel('Search repository teams');
    await search.focus();
    await page.keyboard.type('does-not-exist');
    await expect(page.locator('.repo')).toHaveCount(0);
    await expect(page.locator('.no-results')).toContainText('No repository teams match your search');
    await expect(page.locator('#matchSummary')).toContainText('No repository teams match “does-not-exist”');
    const clear = page.getByRole('button', { name: 'Clear' });
    await expect(clear).toBeVisible();
    await page.keyboard.press('Tab');
    await expect(clear).toBeFocused();
    await page.keyboard.press('Enter');
    await expect(search).toHaveValue('');
    await expect(search).toBeFocused();
    await expect(page.locator('.repo')).toHaveCount(2);
    await expect(page.locator('#matchSummary')).toContainText('Showing all 2 repositories and 6 specialists.');
  });
});

function repository(repo, summary, members) {
  return {
    repo,
    mob_id: `ensemble-${repo}`,
    brief: { summary, languages: ['Rust', 'TypeScript'], key_paths: [], guidance_files: [] },
    members,
    preparation: 'ready',
    discovery: { model: 'fixture', discovered_at: '2026-01-01T00:00:00Z', inspected_files: [] },
  };
}

function member(member_id, label, focus, ownership_paths) {
  return {
    member_id,
    study_state: member_id === 'representative' ? 'not_required' : 'completed',
    label,
    focus,
    capability: member_id === 'representative' ? 'read_only' : 'code',
    memory_enabled: true,
    ownership_paths,
    identity: `${member_id}:identity`,
    runtime_alias: `${member_id}:runtime`,
    session_id: `${member_id}:session`,
    state: 'active',
  };
}

function repoCard(page, name) {
  return page.locator('.repo').filter({ has: page.locator('h3', { hasText: name }) });
}

test('setup is separate from ticket-based operation', async ({ page }) => {
  await expect(page.getByRole('heading', { name: 'Repositories', exact: true })).toBeVisible();
  await expect(page.locator('#objective-form')).toHaveCount(0);
  await expect(page.locator('textarea')).toHaveCount(0);
  await expect(page.getByRole('link', { name: /Open board/ }).first()).toHaveJSProperty('href', baseUrl + '/issues');
  await expect(page.getByRole('button', { name: '+ Add repository', exact: true })).toBeVisible();
});

test('adding a remote repository opens its independent configuration', async ({ page }) => {
  let submitted;
  await page.route('**/api/workspaces', async route => {
    if(route.request().method()==='GET')return route.fulfill({json:{hub:null,workspaces:[]}});
    submitted=route.request().postDataJSON();
    await route.fulfill({status:201,json:{url:baseUrl+'/setup'}});
  });
  await page.route('**/setup', route=>route.fulfill({contentType:'text/html',body:'<h1>Repository configuration</h1>'}));
  await page.getByRole('button', {name:'+ Add repository',exact:true}).click();
  await page.getByLabel('Repository URL', {exact:true}).fill('https://git.example.com/team/new-repo');
  await page.getByRole('dialog').getByRole('button', {name:'Configure repository',exact:true}).click();
  await expect(page).toHaveURL(baseUrl+'/setup');
  expect(submitted).toEqual({repository_url:'https://git.example.com/team/new-repo'});
});

test('failed enrollment keeps the URL and shows the actionable error', async ({ page }) => {
  await page.route('**/api/workspaces', route => route.request().method()==='GET'?route.fulfill({json:{hub:null,workspaces:[]}}):route.fulfill({status:409,json:{error:'This repository already has a workspace.'}}));
  await page.getByRole('button', {name:'+ Add repository',exact:true}).click();
  await page.getByLabel('Repository URL', {exact:true}).fill('https://github.com/example/repository');
  await page.getByRole('dialog').getByRole('button', {name:'Configure repository',exact:true}).click();
  await expect(page.locator('#addError')).toContainText('already has a workspace');
  await expect(page.getByLabel('Repository URL', {exact:true})).toHaveValue('https://github.com/example/repository');
  await expect(page.getByRole('dialog').getByRole('button', {name:'Configure repository',exact:true})).toBeEnabled();
  await page.getByRole('button', {name:'Cancel',exact:true}).click();
  await expect(page.getByRole('dialog')).not.toBeVisible();
});

test('failed formation has a retry action and never offers a ready console', async ({ page }) => {
  const current = {...structuredClone(fixture),setup:{phase:'failed',error:'Model authentication failed'},runtime:{repositories:[],peers:[]}};
  await page.route('**/api/ensemble', route => route.fulfill({json:current}));
  await page.route('**/api/ensemble/retry', async route => {
    current.setup = {phase:'preparing'};
    await route.fulfill({status:202,json:{phase:'preparing'}});
  });
  await page.reload();
  await expect(page.locator('#setup')).toContainText('Model authentication failed');
  await expect(page.getByRole('link', {name:/Inspect team/})).toHaveCount(0);
  await page.getByRole('button', {name:'Retry setup',exact:true}).click();
  await expect(page.locator('#setup')).toContainText('Preparing repository teams');
});

for (const width of [390, 1024, 1440]) {
  test(`repository setup fits a ${width}px viewport`, async ({ page }) => {
    await page.setViewportSize({width,height:900});
    await expect(page.getByRole('button', {name:'+ Add repository',exact:true})).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await page.getByRole('button', {name:'+ Add repository',exact:true}).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });
}

test('all repository teams open the same console', async ({ page }) => {
  const links = page.getByRole('link', {name: /Inspect team/});
  await expect(links).toHaveCount(2);
  for (const link of await links.all()) await expect(link).toHaveAttribute('href', '/console');
});

test('specialist subscriptions and notification failures are visible', async ({ page }) => {
  const current = structuredClone(fixture);
  current.runtime.repositories[0].notifications = {enabled: true, health: {state: 'retrying', error: 'Repository access failed; retrying.'}};
  current.runtime.repositories[0].members[1].subscriptions = ['src/api.rs', 'tests/api'];
  await page.route('**/api/ensemble', route => route.fulfill({json: current}));
  await page.reload();
  const api = repoCard(page, 'api');
  await expect(api.locator('.notification-health')).toContainText('Repository access failed; retrying.');
  const specialist = api.locator('details.specialist').filter({hasText: 'Backend Contracts'});
  await specialist.locator('summary').click();
  await expect(specialist).toContainText('Following: src/api.rs · tests/api');
});
