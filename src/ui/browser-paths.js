// Repository pages share an origin and use a stable path beneath the host.
// These helpers belong to Fugue pages; native Meta transport stays untouched.
const fuguePrefix = location.pathname.match(/^\/repositories\/(repository-[a-zA-Z0-9-]+)(?:\/|$)/)?.[0].replace(/\/$/, '') || '';
const fugueBase = document.createElement('base');
fugueBase.href = location.origin + fuguePrefix + '/';
document.head.prepend(fugueBase);
function fugueUrl(path) {
  if (typeof path !== 'string' || !path.startsWith('/') || path.startsWith('//')) return path;
  return fuguePrefix + path;
}
function fugueFetch(path, options) { return fetch(fugueUrl(path), options); }
function fugueNavigate(path) { location.href = fugueUrl(path); }
