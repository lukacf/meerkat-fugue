import {browserDocument} from './browser-document.mjs';
import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

// Render the actual Rust templates with fixture field values, then execute the
// production Board script. The fixture contains no copy of creation behavior.
const source = await readFile(new URL('../src/http.rs', import.meta.url), 'utf8');
const boardTemplate = source.split('const ISSUES_BOARD_HTML: &str = r#"')[1].split('"#;')[0];
const formTemplate = source.split('fn create_issue_editor_html(')[1].match(/r#"(<form[\s\S]*?)"#/)[1];
const editorTemplate = source.match(/r#"(<div class="form-field editor-field">[\s\S]*?)"#,/)[1];
const editor = editorTemplate.replaceAll('{id}', 'createDescription').replaceAll('{label}', 'Description')
  .replaceAll('{name_attr}', ' name="description"').replaceAll('{placeholder_attr}', '')
  .replaceAll('{ISSUE_MARKDOWN_EDITOR_CLASS}', 'issue-markdown-editor').replaceAll('{value}', '');
const html = browserDocument(boardTemplate.replace('__FUGUE_CREATE_EDITOR__', formTemplate
  .replace('{lane_options}', '<option value="Open">Open</option>').replace('{description_editor}', editor)));
const receipt = {id:'10001', identifier:'TEST-1', url:'https://jira.example.test/browse/TEST-1'};
const warning = {code:'issue_created_followup_failed', message:'Created TEST-1. Fugue could not confirm its initial status. Open the existing issue to check it.'};

async function show(page, {partial=false, uploadFailures=0, uploadGate, uploadWarning=null, uploadReply=null, scope=()=>"tracker-a"}={}) {
  const observed = {creates:0, uploads:[], uploadBodies:[], uploadHeaders:[]};
  await page.route('http://fugue.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith('/api/v1/issues') && route.request().method() === 'POST') {
      observed.creates++;
      return route.fulfill({status:201, json: partial
        ? {outcome:'created_with_warning', created_issue:receipt, warning}
        : {outcome:'created', issue:receipt}});
    }
    if (path.endsWith('/attachments')) {
      observed.uploads.push(path);
      observed.uploadHeaders.push(route.request().headers());
      observed.uploadBodies.push(route.request().postDataBuffer().toString());
      if (uploadGate) await uploadGate;
      if (uploadReply) return route.fulfill(uploadReply);
      return observed.uploads.length <= uploadFailures
        ? route.fulfill({status:403,json:{error:{message:'Upload unavailable'}}})
        : route.fulfill({json:{attachments:[{title:'trace.txt',url:'https://jira.example.test/attachment/1'}], ...(uploadWarning ? {warning:uploadWarning} : {})}});
    }
    if (path.endsWith('/api/v1/issues/board')) {
      return route.fulfill({json:{totals:{issues:0,running:0,retrying:0},lanes:[{name:'Open',kind:'intake',issues:[]}]}});
    }
    return route.fulfill({contentType:'text/html',body:html.replaceAll("__FUGUE_TRACKER_SCOPE__",scope())});
  });
  await page.goto('http://fugue.test/repositories/repository-test/issues');
  await page.getByRole('button',{name:'+ New issue',exact:true}).click();
  await page.getByLabel('Title',{exact:true}).fill('A single Jira issue');
  await page.getByLabel('Description',{exact:true}).fill('Keep this context if the upload fails.');
  return observed;
}

test('attachment retry targets the acknowledged issue without creating another', async ({page}) => {
  const observed = await show(page,{uploadFailures:1});
  await page.locator('#createAttachments').setInputFiles({name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')});
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText('Upload unavailable');
  await page.locator('#createSubmit').click();
  await expect(page.locator('#notice')).toContainText('TEST-1');
  expect(observed.creates).toBe(1);
  expect(observed.uploads).toEqual([
    '/repositories/repository-test/api/v1/issues/TEST-1/attachments',
    '/repositories/repository-test/api/v1/issues/TEST-1/attachments',
  ]);
});

for (const uploadReply of [{status:502,json:{error:{message:'gateway failure'}}}, {status:200,json:{}}]) {
  test(`creation upload blocks blind retry after ambiguous response ${uploadReply.status}`, async ({page})=>{
    const observed=await show(page,{uploadReply});
    await page.locator('#createAttachments').setInputFiles({name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')});
    await page.locator('#createSubmit').click();
    await expect(page.locator('#createError')).toContainText('Check the existing issue');
    await expect(page.locator('#createSubmit')).toBeHidden();
    await page.reload();
    await expect(page.locator('#createSubmit')).toBeHidden();
    await expect(page.locator('#createError')).toContainText('Check the existing issue');
    expect(observed.creates).toBe(1);
    expect(observed.uploads).toHaveLength(1);
  });
}

test('partial Jira creation preserves a scoped link and does not offer creation again', async ({page}) => {
  const observed = await show(page,{partial:true});
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText('could not confirm its initial status');
  await expect(page.getByRole('link',{name:'Open TEST-1',exact:true}))
    .toHaveAttribute('href','/repositories/repository-test/issues/TEST-1');
  await expect(page.getByLabel('Title',{exact:true})).toBeDisabled();
  await expect(page.locator('#createSubmit')).not.toHaveText('Create issue');
  expect(observed.creates).toBe(1);
});

test('reload restores the acknowledged issue and retries only its attachment upload', async ({page}) => {
  const observed = await show(page,{uploadFailures:1});
  const file = {name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')};
  await page.locator('#createAttachments').setInputFiles(file);
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText('Upload unavailable');
  await page.reload();
  await expect(page.getByLabel('Title',{exact:true})).toHaveValue('A single Jira issue');
  await expect(page.getByLabel('Title',{exact:true})).toBeDisabled();
  await expect(page.getByLabel('Description',{exact:true})).toHaveValue('Keep this context if the upload fails.');
  await expect(page.locator('#createError')).toContainText('Re-select any files');
  await page.locator('#createAttachments').setInputFiles(file);
  await page.getByRole('button',{name:'Retry attachments',exact:true}).click();
  await expect(page.locator('#notice')).toContainText('Created TEST-1.');
  expect(observed.creates).toBe(1);
  expect(observed.uploads).toHaveLength(2);
  await page.reload();
  await expect(page.locator('#createPanel')).not.toHaveClass(/open/);
});

test('Done explicitly clears a partial creation before a new issue can be submitted', async ({page}) => {
  const observed = await show(page,{partial:true});
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText('could not confirm');
  await page.reload();
  await expect(page.locator('#createError')).toContainText('could not confirm');
  await page.getByRole('button',{name:'Done',exact:true}).click();
  await page.reload();
  await page.getByRole('button',{name:'+ New issue',exact:true}).click();
  await expect(page.getByLabel('Title',{exact:true})).toBeEnabled();
  await expect(page.getByLabel('Title',{exact:true})).toHaveValue('');
  await expect(page.getByLabel('Description',{exact:true})).toHaveValue('');
  await page.getByLabel('Title',{exact:true}).fill('An explicitly new issue');
  await page.getByRole('button',{name:'Create issue',exact:true}).click();
  await expect(page.locator('#createError')).toContainText('could not confirm');
  expect(observed.creates).toBe(2);
});

test('another repository never inherits the pending issue or attachment target', async ({page}) => {
  const observed = await show(page,{partial:true});
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText('could not confirm');
  await page.goto('http://fugue.test/repositories/another-repository/issues');
  await expect(page.locator('#createPanel')).not.toHaveClass(/open/);
  await page.getByRole('button',{name:'+ New issue',exact:true}).click();
  await expect(page.getByLabel('Title',{exact:true})).toBeEnabled();
  await expect(page.getByLabel('Title',{exact:true})).toHaveValue('');
  expect(observed.creates).toBe(1);
  expect(observed.uploads).toEqual([]);
});

test('files selected during an upload remain pending for the same issue', async ({page}) => {
  let release;
  const uploadGate = new Promise(resolve => {release = resolve;});
  const observed = await show(page,{uploadGate});
  await page.locator('#createAttachments').setInputFiles({name:'first.txt',mimeType:'text/plain',buffer:Buffer.from('first')});
  await page.locator('#createSubmit').click();
  await expect.poll(()=>observed.uploads.length).toBe(1);
  await page.locator('#createAttachments').setInputFiles({name:'second.txt',mimeType:'text/plain',buffer:Buffer.from('second')});
  release();
  await expect(page.locator('#createError')).toContainText('Upload the remaining attachments');
  await expect(page.locator('#createAttachmentList')).toContainText('second.txt');
  await expect(page.locator('#createAttachmentList')).not.toContainText('first.txt');
  await page.getByRole('button',{name:'Retry attachments',exact:true}).click();
  await expect(page.locator('#notice')).toContainText('Created TEST-1.');
  expect(observed.creates).toBe(1);
  expect(observed.uploadBodies).toHaveLength(2);
  expect(observed.uploadBodies[0]).toContain('first.txt');
  expect(observed.uploadBodies[0]).not.toContain('second.txt');
  expect(observed.uploadBodies[1]).toContain('second.txt');
  expect(observed.uploadBodies[1]).not.toContain('first.txt');
});


test('a changed tracker configuration cannot restore an old ticket receipt', async ({page}) => {
  let scope = 'tracker-a';
  const observed = await show(page,{partial:true,scope:()=>scope});
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText('could not confirm');
  scope = 'tracker-b';
  await page.reload();
  await expect(page.locator('#createPanel')).not.toHaveClass(/open/);
  await page.getByRole('button',{name:'+ New issue',exact:true}).click();
  await expect(page.getByLabel('Title',{exact:true})).toBeEnabled();
  await expect(page.getByLabel('Title',{exact:true})).toHaveValue('');
  expect(observed.uploads).toHaveLength(0);
});

test('an uncertain Jira upload preserves the ticket and does not offer blind retry after reload', async ({page}) => {
  const uploadWarning = {code:'upload_not_confirmed',message:'Some files may already be attached. Check the existing issue before uploading any files again.'};
  const observed = await show(page,{uploadWarning});
  await page.locator('#createAttachments').setInputFiles({name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')});
  await page.locator('#createSubmit').click();
  await expect(page.locator('#createError')).toContainText(uploadWarning.message);
  await expect(page.locator('#createSubmit')).toBeHidden();
  await page.reload();
  await expect(page.locator('#createError')).toContainText(uploadWarning.message);
  await page.locator('#createAttachments').setInputFiles({name:'trace.txt',mimeType:'text/plain',buffer:Buffer.from('trace')});
  await expect(page.locator('#createSubmit')).toBeHidden();
  expect(observed.creates).toBe(1);
  expect(observed.uploads).toHaveLength(1);
  expect(observed.uploadHeaders[0]['x-fugue-tracker-scope']).toBe('tracker-a');
});


test('repository board labels a partial launch instead of showing running activity', async ({page}) => {
  await page.route('http://fugue.test/**', route => route.request().url().endsWith('/api/v1/issues/board')
    ? route.fulfill({json:{totals:{issues:1,running:0,retrying:0,launch_failed:1},lanes:[{name:'In Development',kind:'active',issues:[{
      identifier:'TEST-1',title:'Partial launch',lane:'In Development',labels:[],runtime:{kind:'running',turn_count:2,launch_failure:{message:'wire requires comms runtime'}}
    }]}]}})
    : route.fulfill({contentType:'text/html',body:html}));
  await page.goto('http://fugue.test/repositories/repository-test/issues');
  const card=page.locator('.card[data-issue="TEST-1"]');
  await expect(card).toContainText('Team startup failed');
  await expect(card).not.toContainText('Running');
  await expect(card).toHaveAttribute('data-runtime','launch_failed');
});
