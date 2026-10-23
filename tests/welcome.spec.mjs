import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

const read = async name => readFile(new URL(`../src/${name}`, import.meta.url),'utf8').catch(error => {
  if (error.code === 'ENOENT') return '';
  throw error;
});
const source = await read('http.rs');
const frame = source.split('let chrome = format!(')[1].match(/r#"([\s\S]*?)"#/)[1];
const css = await read('ui/app.css');
const welcome = await read('ui/welcome.html');
const script = await read('ui/welcome.js');
const brand = await read('ui/brand.svg');
const document = area => '<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>' + css + '</style></head><body class="fugue-console-embedded">' + frame
  .replace('{area}',area).replace('{title}','Home').replace('{repositories_selected}','').replace('{brand}',brand)
  .replace('{account}','<a href="/auth/sign-out">Sign out of this browser</a>')
  .replace('{nav}','<a href="/overview">Home</a><a href="/queues">Board</a><a href="/console">Agents</a>')
  + '<main><button data-fugue-welcome>Getting started</button></main>' + welcome + '<script>' + script + '</script></body></html>';
async function open(page, area='home') {
  await page.route('http://fugue.test/**',route=>route.fulfill({contentType:'text/html',body:document(area)}));
  await page.goto('http://fugue.test/overview');
}

test('welcome explains the full journey and remembers dismissal, with a way back', async ({page}) => {
  await open(page);
  const guide = page.getByRole('dialog');
  await expect(guide).toBeVisible();
  await expect(guide.getByRole('heading',{name:'Connect your tools.'})).toBeVisible();
  for (const title of ['Prepare your team.','Give it an issue.','Review, then merge.']) {
    await guide.getByRole('button',{name:'Next',exact:true}).click();
    await expect(guide.getByRole('heading',{name:title})).toBeVisible();
    await expect(guide.getByRole('img')).toHaveCount(1);
  }
  await expect(guide.getByRole('link',{name:'Add repository',exact:true})).toHaveAttribute('href','/ensemble#add');
  await guide.getByRole('button',{name:'Done',exact:true}).click();
  await expect(guide).toBeHidden();
  await page.reload();
  await expect(guide).toBeHidden();
  const launch = page.locator('main').getByRole('button',{name:'Getting started'});
  await launch.click();
  await expect(guide).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(guide).toBeHidden();
  await expect(launch).toBeFocused();
});

test('guide does not interrupt operational pages and traps focus when opened', async ({page}) => {
  await open(page,'agents');
  await expect(page.getByRole('dialog')).toBeHidden();
  await page.locator('main').getByRole('button',{name:'Getting started'}).click();
  const guide = page.getByRole('dialog');
  await guide.getByRole('button',{name:'Close guide'}).focus();
  await page.keyboard.press('Shift+Tab');
  await expect(guide.getByRole('button',{name:'Next',exact:true})).toBeFocused();
  await page.keyboard.press('Tab');
  await expect(guide.getByRole('button',{name:'Close guide'})).toBeFocused();
});

test('small screens and reduced motion retain every guide control', async ({page}, testInfo) => {
  await page.setViewportSize({width:390,height:700});
  await page.emulateMedia({reducedMotion:'reduce'});
  await open(page);
  const guide = page.getByRole('dialog');
  await expect(guide).toBeVisible();
  for (let step=0;step<4;step++) {
    const bounds = await guide.boundingBox();
    expect(bounds.x).toBeGreaterThanOrEqual(0);
    expect(bounds.x + bounds.width).toBeLessThanOrEqual(390);
    expect(await guide.evaluate(el=>el.scrollWidth <= el.clientWidth)).toBe(true);
    expect(await guide.locator('.welcome-scene:not([hidden]) .welcome-packet').first().evaluate(el=>getComputedStyle(el).animationName)).toBe('none');
    const next = guide.getByRole('button',{name:step===3 ? 'Done' : 'Next',exact:true});
    await next.scrollIntoViewIfNeeded();
    await expect(next).toBeInViewport();
    await page.screenshot({path:testInfo.outputPath(`welcome-mobile-${step + 1}.png`)});
    if (step<3) await next.click();
  }
});
