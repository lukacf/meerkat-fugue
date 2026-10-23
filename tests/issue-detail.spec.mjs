import {browserDocument} from './browser-document.mjs';
import {test,expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('../src/http.rs',import.meta.url),'utf8');
const html=source.split('const ISSUE_DETAIL_HTML: &str = r#"')[1].split('"#;')[0];

test('ticket details keep workpad evidence separate and preserve comment drafts',async({page})=>{
  const detail={issue:{identifier:'TEST-1',title:'A real task',state:{name:'Active'},labels:[],description:'Implement the requested behavior'},comments:[{comment_id:'workpad-1',body_markdown:'Detailed agent evidence'},{comment_id:'human-1',body_markdown:'Please check the edge case'}],attachments:[],workpad:{comment_id:'workpad-1',body_markdown:'Detailed agent evidence'},runtime:null,lanes:['Active','Review','Deploy','Done'],lane_actions:{Deploy:'merge'}};
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.route('http://fugue.test/**',route=>route.request().url().includes('/api/')?route.fulfill({json:detail}):route.fulfill({contentType:'text/html',body:browserDocument(html)}));
  await page.goto('http://fugue.test/issues/TEST-1');
  await expect(page.locator('.comment')).toHaveCount(1);
  await expect(page.locator('.comment')).toContainText('Please check the edge case');
  await expect(page.locator('#agentWorkpad')).not.toHaveAttribute('open','');
  await page.locator('#commentEditor').fill('Keep this draft while I choose a lane.');
  await page.getByRole('button',{name:/Review/}).click();
  await expect(page.locator('#commentEditor')).toHaveValue('Keep this draft while I choose a lane.');
  await expect(page.locator('#save')).toContainText('SAVE CHANGES');
  await page.getByRole('button',{name:/Deploy/}).click();
  await expect(page.locator('#save')).toContainText('SAVE & RUN MERGE');
  await page.locator('#agentWorkpad summary').click();
  await expect(page.getByLabel('Agent workpad',{exact:true})).toHaveValue('Detailed agent evidence');
  await expect(page.getByLabel('Agent workpad',{exact:true})).toHaveAttribute('readonly','');
  expect(errors).toEqual([]);
});

test('Jira statuses remain literal and the configured merge action stays available',async({page})=>{
  const detail={issue:{identifier:'TEST-2',title:'Closed acceptance attempt',state:{name:'Closed'},labels:[],description:'No PR was created'},comments:[],attachments:[],workpad:null,runtime:null,lanes:['Open','In Progress','Resolved','Reopened','Closed'],lane_actions:{Reopened:'merge'}};
  await page.route('http://fugue.test/**',route=>route.request().url().includes('/api/')?route.fulfill({json:detail}):route.fulfill({contentType:'text/html',body:browserDocument(html)}));
  await page.goto('http://fugue.test/issues/TEST-2');
  await expect(page.locator('#stateChip')).toContainText('Closed', {ignoreCase:true});
  await expect(page.locator('#lanePicker')).not.toContainText('Canceled', {ignoreCase:true});
  await expect(page.locator('#lanePicker')).not.toContainText('Done', {ignoreCase:true});
  await page.getByRole('button',{name:/Reopened/}).click();
  await expect(page.locator('#save')).toContainText('SAVE & RUN MERGE');
});

test('saved attachments keep their repository scope after reload',async({page})=>{
  const prefix='/repositories/repository-test';
  const local='/api/v1/issues/TEST-1/attachments/trace.txt';
  const external='https://example.com/evidence.txt';
  const detail={issue:{identifier:'TEST-1',title:'Attachment scope',state:{name:'Active'},labels:[],description:''},comments:[],attachments:[{title:'Trace',url:local},{title:'External',url:external}],workpad:null,runtime:null,lanes:['Active'],lane_actions:{}};
  await page.route('http://fugue.test/**',route=>route.request().url().includes('/api/')?route.fulfill({json:detail}):route.fulfill({contentType:'text/html',body:browserDocument(html)}));
  await page.goto(`http://fugue.test${prefix}/issues/TEST-1`);
  for(let attempt=0;attempt<2;attempt++){
    await expect(page.getByRole('link',{name:'Trace',exact:true})).toHaveAttribute('href',prefix+local);
    await expect(page.getByRole('link',{name:'External',exact:true})).toHaveAttribute('href',external);
    if(attempt===0)await page.reload();
  }
});
