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

async function show(page, {partial=false, uploadFailures=0, uploadGate}={}) {
  const observed = {creates:0, uploads:[], uploadBodies:[]};
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
      observed.uploadBodies.push(route.request().postDataBuffer().toString());
      if (uploadGate) await uploadGate;
      return observed.uploads.length <= uploadFailures
        ? route.fulfill({status:503,json:{error:{message:'Upload unavailable'}}})
        : route.fulfill({json:{attachments:[{title:'trace.txt'}]}});
    }
    if (path.endsWith('/api/v1/issues/board')) {
      return route.fulfill({json:{totals:{issues:0,running:0,retrying:0},lanes:[{name:'Open',kind:'intake',issues:[]}]}});
    }
    return route.fulfill({contentType:'text/html',body:html});
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
