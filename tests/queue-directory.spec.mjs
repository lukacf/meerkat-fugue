import {browserDocument} from './browser-document.mjs';
import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

const css = await readFile(new URL('../src/ui/app.css', import.meta.url), 'utf8');
const portfolio = await readFile(new URL('../src/ui/portfolio.js', import.meta.url), 'utf8');
async function document(view) {
  return browserDocument((await readFile(new URL(`../src/ui/${view === 'home' ? 'overview' : 'queues'}.html`, import.meta.url), 'utf8'))
    .replace('__PORTFOLIO_JS__', portfolio).replace('<body>', '<body data-queue-enabled="false">')
    .replace('</head>', `<style>${css}</style></head>`));
}
const workspaces = [
  {id:'repository-service',repository:{host:'github.com',owner:'example',repository:'service'},running:true,configured:true},
  {id:'repository-web',repository:{host:'git.example.com',owner:'example',repository:'web'},running:true,configured:true},
  {id:'repository-draft',repository:{host:'github.com',owner:'example',repository:'draft'},running:true,configured:false},
  {id:'repository-stopped',repository:{host:'github.com',owner:'example',repository:'stopped'},running:false,configured:true},
];
const board = (identifier, title) => ({generated_at:'2026-10-23T00:00:00Z', totals:{issues:1,running:0,retrying:0},lanes:[
  {name:'Reopened',kind:'attention',action:'merge',issues:[{identifier,title,runtime:null,updated_at:'2026-10-22T00:00:00Z'}]},
  {name:'Closed',kind:'terminal',action:null,issues:[]},
]});
async function show(page, {view='board', listing=workspaces, status=200, failedBoard=null}={}) {
  const html = await document(view);
  await page.route('http://fugue.test/**', route => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/workspaces') return route.fulfill({status,json:{hub:null,workspaces:listing}});
    if (path.endsWith('/api/ensemble')) return route.fulfill({json:{setup:{phase:'ready'}}});
    if (path.endsWith('/api/v1/issues/board')) {
      if (path.includes(failedBoard || 'impossible')) return route.fulfill({status:503});
      const web = path.includes('repository-web');
      return route.fulfill({json:board(web ? 'WEB-1' : 'API-1', web ? 'Web change' : 'Service change')});
    }
    return route.fulfill({contentType:'text/html',body:html});
  });
  await page.goto('http://fugue.test/' + (view === 'home' ? 'overview' : 'queues'));
}

test('boards retain repository scope and Jira status names', async ({page}) => {
  await show(page);
  const service = page.getByRole('region',{name:'example/service',exact:true});
  const web = page.getByRole('region',{name:'example/web',exact:true});
  await expect(service.getByRole('link',{name:'Open board',exact:true})).toHaveAttribute('href','/repositories/repository-service/issues');
  await expect(service.getByRole('link',{name:/API-1/})).toHaveAttribute('href','/repositories/repository-service/issues/API-1');
  await expect(service).not.toContainText('WEB-1');
  await expect(web.getByRole('link',{name:/WEB-1/})).toHaveAttribute('href','/repositories/repository-web/issues/WEB-1');
  await expect(service.getByRole('heading',{name:'Reopened (1)',exact:true})).toBeVisible();
  await expect(page.getByRole('region',{name:'example/draft',exact:true}).getByRole('link')).toHaveAttribute('href','/repositories/repository-draft/setup');
  await expect(page.getByRole('region',{name:'example/stopped',exact:true}).getByRole('link')).toHaveAttribute('href','/ensemble#repositoryWorkspacesPanel');
});

test('empty directory offers repository onboarding', async ({page}) => {
  await show(page,{listing:[]});
  await expect(page.getByRole('link',{name:'Add your first repository'})).toHaveAttribute('href','/ensemble#add');
  await expect(page.getByRole('status')).toContainText('0 repositories');
});

test('failed directory reads are not presented as an empty queue', async ({page}) => {
  await show(page,{status:500});
  await expect(page.getByRole('alert')).toContainText('HTTP 500');
  await expect(page.getByRole('status')).toHaveText('Update failed');
  await expect(page.locator('#portfolioRepositories')).toContainText('Repository boards are unavailable.');
  await expect(page.locator('#portfolioRepositories')).not.toContainText('Loading');
  await expect(page.getByRole('alert')).not.toContainText('Previously loaded');
  await expect(page.getByRole('link',{name:'Add your first repository'})).toHaveCount(0);
});

test('one failed board does not hide another repository or claim no attention', async ({page}) => {
  await show(page,{view:'home',failedBoard:'repository-service'});
  await expect(page.getByRole('heading',{level:1})).toHaveText('1 issue needs attention.');
  await expect(page.locator('#portfolioAttention').getByRole('link',{name:/WEB-1/})).toBeVisible();
  await expect(page.getByRole('region',{name:'example/service',exact:true})).toContainText('HTTP 503');
  await expect(page.locator('#portfolioProblems')).toContainText('HTTP 503');
});

test('mobile attention rows do not overlap or overflow their container', async ({page}) => {
  await page.setViewportSize({width:390,height:844});
  await show(page,{view:'home'});
  const rows = page.locator('#portfolioAttention .portfolio-issue');
  await expect(rows).toHaveCount(2);
  const first = await rows.nth(0).boundingBox(), second = await rows.nth(1).boundingBox();
  expect(second.y).toBeGreaterThanOrEqual(first.y + first.height - 1);
  expect(first.x + first.width).toBeLessThanOrEqual(390);
});

test('home lists unfinished configuration once, outside active work', async ({page}) => {
  await show(page,{view:'home',listing:[workspaces[2]]});
  await expect(page.locator('#portfolioProblems')).toContainText('example/draft');
  await expect(page.locator('#portfolioRepositories')).not.toContainText('example/draft');
  await expect(page.locator('#portfolioRepositories')).toContainText('Prepare your team');
  await expect(page.getByRole('link',{name:'Add repository',exact:true})).toBeVisible();
});


test('home surfaces failed team admission in an active Jira lane', async ({page}) => {
  await show(page, {view:'home', listing:[workspaces[0]]});
  await page.route('http://fugue.test/repositories/repository-service/api/v1/issues/board', route => route.fulfill({json:{
    generated_at:'2026-10-23T00:00:00Z', totals:{issues:1,running:0,retrying:0,launch_failed:1},
    lanes:[{name:'In Development',kind:'active',action:null,issues:[{
      identifier:'API-2',title:'Improve documentation',runtime:{kind:'running',launch_failure:{
        occurred_at:'2026-10-23T00:00:00Z',message:'wire planner -> specialist: wire requires comms runtime'
      }}
    }]}]
  }}));
  await page.reload();
  await expect(page.getByRole('heading',{level:1})).toHaveText('1 issue needs attention.');
  const attention = page.locator('#portfolioAttention');
  await expect(attention).toContainText('Team startup failed');
  await expect(attention).toContainText('requires comms runtime');
  await expect(attention.getByRole('link',{name:/API-2/})).toHaveAttribute('href','/repositories/repository-service/issues/API-2');
  await expect(page.getByRole('region',{name:'example/service',exact:true})).toContainText('1 team startup failed');
});
