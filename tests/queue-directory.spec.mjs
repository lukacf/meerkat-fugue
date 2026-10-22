import {browserDocument} from './browser-document.mjs';
import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

const html = (await readFile(new URL('../src/ui/queues.html', import.meta.url), 'utf8'))
  .replace('</head>', `<style>${await readFile(new URL('../src/ui/app.css', import.meta.url), 'utf8')}</style></head>`);

async function showQueues(page, workspaces, status = 200) {
  await page.route('http://fugue.test/**', route => {
    if (new URL(route.request().url()).pathname === '/api/workspaces') {
      return route.fulfill({status, json:{hub:null, workspaces}});
    }
    return route.fulfill({contentType:'text/html', body:browserDocument(html)});
  });
  await page.goto('http://fugue.test/queues');
}

test('repository queues retain their separate destinations and unavailable states', async ({page}) => {
  await showQueues(page, [
    {repository:{host:'github.com',owner:'example',repository:'service'},url:'http://127.0.0.1:9001/issues',running:true,configured:true},
    {repository:{host:'git.example.com',owner:'example',repository:'web'},url:'http://127.0.0.1:9002/issues',running:true,configured:true},
    {repository:{host:'github.com',owner:'example',repository:'draft'},url:'http://127.0.0.1:9003/setup',running:true,configured:false},
    {repository:{host:'github.com',owner:'example',repository:'stopped'},url:'http://127.0.0.1:9004/issues',running:false,configured:true},
  ]);
  const card = name => page.getByRole('article').filter({has:page.getByRole('heading',{name,exact:true})});
  await expect(card('example/service').getByRole('link')).toHaveAttribute('href','http://127.0.0.1:9001/issues');
  await expect(card('example/web').getByRole('link')).toHaveAttribute('href','http://127.0.0.1:9002/issues');
  await expect(card('example/draft').getByRole('link',{name:'Continue configuration'})).toHaveAttribute('href','http://127.0.0.1:9003/setup');
  await expect(card('example/stopped').getByRole('link',{name:'Manage repository'})).toHaveAttribute('href','/ensemble#repositoryWorkspacesPanel');
});

test('empty directory offers onboarding instead of a dead board', async ({page}) => {
  await showQueues(page, []);
  await expect(page.getByRole('status')).toContainText('No repositories yet');
  await expect(page.getByRole('link',{name:'Add repository'})).toHaveAttribute('href','./ensemble#add');
});

test('a failed directory read is visible', async ({page}) => {
  await showQueues(page, [], 500);
  await expect(page.getByRole('status')).toContainText('Could not load work queues');
  await expect(page.getByRole('article')).toHaveCount(0);
});
