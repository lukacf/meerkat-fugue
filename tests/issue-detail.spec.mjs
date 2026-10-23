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


for (const lostResponse of [false, true]) {
  test(`Jira comment upload ${lostResponse ? 'lost response' : 'acknowledged files'} keeps the draft and blocks resubmission`, async ({page}) => {
    const detail={issue:{identifier:'TEST-1',title:'Jira upload',state:{name:'Active'},labels:[],description:''},comments:[],attachments:[],workpad:null,runtime:null,lanes:['Active','Review'],lane_actions:{}};
    let posts=0;
    await page.route('http://fugue.test/**',route=>{
      if(route.request().url().endsWith('/comments') && route.request().method()==='POST') {
        posts++;
        return lostResponse ? route.abort('connectionclosed') : route.fulfill({json:{
          attachments:[{name:'trace.txt',url:'https://jira.example.test/secure/attachment/7/trace.txt'}],
          comment_id:null,warning:{code:'comment_not_confirmed',message:'Jira accepted the files, but the comment was not confirmed. Check the existing issue before posting the comment again. Do not upload these files again.'}
        }});
      }
      return route.request().url().includes('/api/') ? route.fulfill({json:detail}) : route.fulfill({contentType:'text/html',body:browserDocument(html)});
    });
    await page.goto('http://fugue.test/issues/TEST-1');
    await page.locator('#commentEditor').fill('Keep my review notes.');
    await page.locator('#commentAttachmentInput').setInputFiles({name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')});
    await page.locator('#postComment').click();
    await expect(page.locator('#commentError')).toContainText('Check the existing issue');
    await expect(page.locator('#commentEditor')).toHaveValue('Keep my review notes.');
    await expect(page.locator('#postComment')).toBeDisabled();
    if (!lostResponse) await expect(page.locator('#commentAttachmentList')).not.toContainText('trace.txt');
    await page.getByRole('button',{name:/Review/}).click();
    await expect(page.locator('#postComment')).toBeDisabled();
    await expect(page.locator('#commentError')).toContainText('Check the existing issue');
    expect(posts).toBe(1);
  });
}

for (const lostResponse of [false, true]) {
  test(`Jira direct upload ${lostResponse ? 'lost response' : 'partial acceptance'} keeps a visible reconciliation warning`, async ({page}) => {
    const detail={issue:{identifier:'TEST-1',title:'Jira upload',state:{name:'Active'},labels:[],description:''},comments:[],attachments:[],workpad:null,runtime:null,lanes:['Active','Review'],lane_actions:{}};
    let posts=0;
    await page.route('http://fugue.test/**',route=>{
      if(route.request().url().endsWith('/attachments') && route.request().method()==='POST') {
        posts++;
        return lostResponse ? route.abort('connectionclosed') : route.fulfill({json:{
          attachments:[],comment_id:null,warning:{code:'upload_not_confirmed',message:'Jira did not confirm the complete upload. Check the existing issue before uploading any files again.'}
        }});
      }
      return route.request().url().includes('/api/') ? route.fulfill({json:detail}) : route.fulfill({contentType:'text/html',body:browserDocument(html)});
    });
    await page.goto('http://fugue.test/issues/TEST-1');
    await page.locator('#issueAttachmentInput').setInputFiles({name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')});
    await expect(page.locator('#issueAttachmentError')).toContainText('Check the existing issue');
    await expect(page.locator('#issueAttachmentInput')).toBeDisabled();
    await page.getByRole('button',{name:/Review/}).click();
    await expect(page.locator('#issueAttachmentInput')).toBeDisabled();
    await expect(page.locator('#issueAttachmentError')).toContainText('Check the existing issue');
    expect(posts).toBe(1);
  });
}

test('a rejected direct upload can be retried and only accepted files appear as attachments', async ({page}) => {
  const detail={issue:{identifier:'TEST-1',title:'Jira upload',state:{name:'Active'},labels:[],description:''},comments:[],attachments:[],workpad:null,runtime:null,lanes:['Active'],lane_actions:{}};
  let posts=0;
  await page.route('http://fugue.test/**',route=>{
    if(route.request().url().endsWith('/attachments') && route.request().method()==='POST') {
      posts++;
      if(posts===1) return route.fulfill({status:403,json:{error:{message:'Jira rejected this upload.'}}});
      detail.attachments=[{title:'trace.txt',url:'https://jira.example.test/secure/attachment/7/trace.txt'}];
      return route.fulfill({json:{attachments:detail.attachments,comment_id:null}});
    }
    return route.request().url().includes('/api/') ? route.fulfill({json:detail}) : route.fulfill({contentType:'text/html',body:browserDocument(html)});
  });
  await page.goto('http://fugue.test/issues/TEST-1');
  const file={name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')};
  await page.locator('#issueAttachmentInput').setInputFiles(file);
  await expect(page.locator('#issueAttachmentError')).toContainText('Jira rejected this upload.');
  await expect(page.locator('#issueAttachmentInput')).toBeEnabled();
  await expect(page.locator('#issueAttachmentList')).not.toContainText('trace.txt');
  await page.locator('#issueAttachmentInput').setInputFiles(file);
  await expect(page.getByRole('link',{name:'trace.txt',exact:true})).toHaveCount(1);
  await expect(page.locator('#issueAttachmentList .attachment-chip')).toHaveCount(1);
  await expect(page.locator('#issueAttachmentInput')).toBeEnabled();
  expect(posts).toBe(2);
});
