import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';
const source = await readFile(new URL('../src/http.rs', import.meta.url), 'utf8');
const navigation = await readFile(new URL('../src/ui/navigation.js', import.meta.url), 'utf8');
const css = await readFile(new URL('../src/ui/app.css', import.meta.url), 'utf8');
const frame = source.split('let chrome = format!(')[1].match(/r#"([\s\S]*?)"#/)[1];
const document = '<html><head><style>' + css + '</style></head><body class="fugue-console-embedded">' + frame
  .replace('{brand}', await readFile(new URL('../src/ui/brand.svg', import.meta.url), 'utf8')).replace('{area}', 'agents').replace('{title}', 'Agents').replace('{repositories_selected}', '')
  .replace('{account}', '<a href="/auth/sign-out">Sign out of this browser</a>')
  .replace('{nav}', '<a href="/overview">Home</a><a href="/queues">Board</a><a href="/console">Agents</a>')
  + '<script>' + navigation + '</script></body></html>';
const listing = {workspaces:[{id:'repository-service',repository:{owner:'example',repository:'service'},running:true,configured:true}]};

async function open(page, response) {
  await page.route('http://fugue.test/**', route => new URL(route.request().url()).pathname === '/api/workspaces'
    ? route.fulfill(response)
    : route.fulfill({contentType:'text/html',body:document}));
  await page.goto('http://fugue.test/console?workspace=repository-service');
}

test('shared console keeps repository return links and fixed primary navigation', async ({page}) => {
  await open(page,{json:listing});
  await expect(page.getByRole('combobox',{name:'Repository scope'})).toHaveValue('repository-service');
  await expect(page.getByRole('link',{name:'Home',exact:true})).toHaveAttribute('href','/repositories/repository-service/overview');
  await expect(page.getByRole('link',{name:'Board',exact:true})).toHaveAttribute('href','/repositories/repository-service/issues');
  await expect(page.getByRole('link',{name:'Agents',exact:true})).toHaveAttribute('href','/console?workspace=repository-service');
  await expect(page.getByRole('navigation',{name:'Primary app navigation'}).getByRole('link')).toHaveText(['Home','Board','Agents']);
  await expect(page.getByText('Shared console - all teams')).toBeVisible();
  await expect(page.getByRole('alert')).toBeHidden();
});

for (const response of [{status:401,json:{error:'expired'}},{status:200,contentType:'text/html',body:'<h1>Sign in</h1>'}]) {
  test(`access expiry is visible for ${response.status} ${response.contentType || 'JSON'}`, async ({page}) => {
    await open(page,response);
    await expect(page.getByRole('alert')).toContainText('Displayed activity may be out of date');
    await expect(page.getByRole('link',{name:'Sign in again'})).toHaveAttribute('href','/console?workspace=repository-service');
    await expect(page.getByRole('navigation',{name:'Primary app navigation'}).getByRole('link')).toHaveText(['Home','Board','Agents']);
  });
}

test('mobile shared navigation stays on two non-overlapping rows', async ({page}) => {
  await page.setViewportSize({width:390,height:844});
  await open(page,{json:listing});
  const scope = await page.getByRole('combobox',{name:'Repository scope'}).boundingBox();
  const board = await page.getByRole('link',{name:'Board',exact:true}).boundingBox();
  const repositories = await page.getByRole('link',{name:'Repositories',exact:true}).boundingBox();
  expect(scope.y + scope.height).toBeLessThanOrEqual(board.y);
  expect(board.x + board.width).toBeLessThan(repositories.x);
  expect(repositories.x + repositories.width).toBeLessThanOrEqual(390);
});

const queueHeading = source.match(/<section class="work-heading">[\s\S]*?<\/section>/)[0];
test('board console shortcut preserves repository context without claiming an agent selection', async ({page}) => {
  const boardDocument = document.replace('data-area="agents"', 'data-area="board"').replace('<script>', queueHeading + '<script>');
  await page.route('http://fugue.test/**', route => new URL(route.request().url()).pathname === '/api/workspaces'
    ? route.fulfill({json:listing})
    : route.fulfill({contentType:'text/html',body:boardDocument}));
  await page.goto('http://fugue.test/repositories/repository-service/issues');
  const shortcut = page.getByRole('link',{name:'Open agent console',exact:true});
  await expect(shortcut).toHaveAttribute('href','/console?workspace=repository-service');
  await shortcut.click();
  await expect(page).toHaveURL('http://fugue.test/console?workspace=repository-service');
  await expect(page.getByRole('link',{name:'Board',exact:true})).toHaveAttribute('href','/repositories/repository-service/issues');
});
