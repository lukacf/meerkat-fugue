import { test, expect } from '@playwright/test';
import { startFixtureServer } from './fixtures/luc654-ui-fixture-server.mjs';

const VIEWPORTS = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'tablet-narrow', width: 1024, height: 768 },
  { name: 'short-height', width: 980, height: 720 },
  { name: 'mobile', width: 390, height: 844 },
];
const ROUTES = ['/overview', '/issues'];
const CARD_SELECTOR = '.card[data-issue="LUC-654-FIXTURE"]';
const SOURCE_LANE_SELECTOR = '.lane[data-lane="Queue"]';
const TARGET_LANE_SELECTOR = '.lane[data-lane="Active"]';
const ATTACHMENT_ZONE_SELECTORS = [
  '#createAttachmentDropZone',
  '#issueAttachmentDropZone',
  '#commentAttachmentDropZone',
];

let fixture;

test.beforeAll(async () => {
  fixture = await startFixtureServer();
});

test.afterAll(async () => {
  await new Promise((resolve) => fixture.server.close(resolve));
});

test.describe('LUC-654 Fugue UI browser validation', () => {
  for (const viewport of VIEWPORTS) {
    for (const route of ROUTES) {
      test(`${route} renders cleanly at ${viewport.name} ${viewport.width}x${viewport.height}`, async ({ page }) => {
        const consoleErrors = [];
        page.on('console', (message) => {
          if (message.type() === 'error') consoleErrors.push(message.text());
        });
        page.on('pageerror', (error) => consoleErrors.push(error.message));
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        await page.goto(`${fixture.url}${route}`);
        const primaryNav = page.getByLabel('Primary app navigation');
        await expect(primaryNav).toBeVisible();
        await expect(primaryNav.getByRole('link', { name: 'OVERVIEW', exact: true })).toBeVisible();
        await expect(primaryNav.getByRole('link', { name: 'IMS', exact: true })).toBeVisible();
        await expect(page.locator('.kpis')).toBeVisible();
        if (route === '/overview') {
          await expect(page.getByLabel('Focus picker')).toBeVisible();
          await expect(page.getByRole('img', { name: /actual Fugue mob topology/ })).toBeVisible();
          await expect(page.locator('.timeline')).toBeVisible();
          await expect(page.locator('.agent-card')).toBeVisible();
          await expect(page.locator('.lane-row').first()).toBeVisible();
        } else {
          await expect(page.getByLabel('Issue board')).toBeVisible();
          await expect(page.locator(SOURCE_LANE_SELECTOR)).toBeVisible();
          await expect(page.locator(CARD_SELECTOR)).toBeVisible();
          await expect(page.locator('#createTitle')).toBeVisible();
          await expect(page.locator('#createAttachmentDropZone')).toBeVisible();
          await page.locator(CARD_SELECTOR).click();
          await expect(page.locator('#issueSheet')).toBeVisible();
        }
        expect(consoleErrors).toEqual([]);
      });
    }
  }

  test('IMS card drag/drop updates target lane and cleans drag/drop classes', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`${fixture.url}/issues`);
    const card = page.locator(CARD_SELECTOR);
    const targetLane = page.locator(TARGET_LANE_SELECTOR);
    await expect(card).toBeVisible();

    await card.dispatchEvent('dragstart', { dataTransfer: await dataTransfer(page, 'text/plain', 'LUC-654-FIXTURE') });
    await expect(card).toHaveClass(/dragging/);
    await targetLane.dispatchEvent('dragover', { dataTransfer: await dataTransfer(page, 'text/plain', 'LUC-654-FIXTURE') });
    await expect(targetLane).toHaveClass(/drop-target/);
    await targetLane.dispatchEvent('drop', { dataTransfer: await dataTransfer(page, 'text/plain', 'LUC-654-FIXTURE') });
    await card.dispatchEvent('dragend');

    await expect(page.locator(`${TARGET_LANE_SELECTOR} ${CARD_SELECTOR}`)).toBeVisible();
    await expect(page.locator('.dragging')).toHaveCount(0);
    await expect(page.locator('.drop-target')).toHaveCount(0);
    await expect.poll(async () => page.evaluate(() => window.__luc654.moves)).toEqual([
      { identifier: 'LUC-654-FIXTURE', state: 'Active' },
    ]);
  });

  test('attachment drop zones accept small files, validate limits, and prevent navigation', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto(`${fixture.url}/issues`);
    await page.locator(CARD_SELECTOR).click();
    await expect(page.locator('#issueSheet')).toBeVisible();

    for (const selector of ATTACHMENT_ZONE_SELECTORS) {
      const zone = page.locator(selector);
      await expect(zone).toBeVisible();
      await zone.dispatchEvent('dragenter', { dataTransfer: await filesDataTransfer(page, [{ name: 'small.txt', body: 'ok', type: 'text/plain' }]) });
      await expect(zone).toHaveClass(/drag-over/);
      await zone.dispatchEvent('dragleave', { dataTransfer: await filesDataTransfer(page, [{ name: 'small.txt', body: 'ok', type: 'text/plain' }]) });
      await expect(zone).not.toHaveClass(/drag-over/);
      await zone.dispatchEvent('drop', { dataTransfer: await filesDataTransfer(page, [{ name: 'small.txt', body: 'ok', type: 'text/plain' }]) });
      await expect(zone).not.toHaveClass(/drag-over/);
    }

    await page.locator('#createAttachmentDropZone').dispatchEvent('drop', {
      dataTransfer: await filesDataTransfer(page, Array.from({ length: 21 }, (_, index) => ({ name: `too-many-${index}.txt`, body: 'x', type: 'text/plain' }))),
    });
    await expect(page.locator('#createError')).toContainText('At most 20 attachments can be added.');

    const prevented = await page.evaluate(async () => {
      const dataTransfer = new DataTransfer();
      dataTransfer.items.add(new File(['nav'], 'nav.txt', { type: 'text/plain' }));
      const event = new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer });
      const dispatchResult = document.dispatchEvent(event);
      return { defaultPrevented: event.defaultPrevented, dispatchResult, href: location.href, dropsPrevented: window.__luc654.dropsPrevented };
    });
    expect(prevented.defaultPrevented).toBe(true);
    expect(prevented.dispatchResult).toBe(false);
    expect(prevented.href).toContain('/issues');
    expect(prevented.dropsPrevented).toBeGreaterThan(0);
    await expect.poll(async () => page.evaluate(() => window.__luc654.uploads.length)).toBeGreaterThanOrEqual(3);
  });
});

async function dataTransfer(page, mimeType, value) {
  return page.evaluateHandle(([type, text]) => {
    const dataTransfer = new DataTransfer();
    dataTransfer.setData(type, text);
    return dataTransfer;
  }, [mimeType, value]);
}

async function filesDataTransfer(page, files) {
  return page.evaluateHandle((fileInputs) => {
    const dataTransfer = new DataTransfer();
    for (const input of fileInputs) {
      dataTransfer.items.add(new File([input.body], input.name, { type: input.type }));
    }
    return dataTransfer;
  }, files);
}
