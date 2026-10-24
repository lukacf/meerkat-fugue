import {readFile} from 'node:fs/promises';
const paths = await readFile(new URL('../src/ui/browser-paths.js', import.meta.url), 'utf8');
export function browserDocument(html) {
  return html.replace('<head>', `<head><script>${paths}</script>`).replaceAll('href="/', 'href="./');
}
