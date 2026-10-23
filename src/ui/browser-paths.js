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

// A gateway can return JSON after the provider has accepted a mutation. Only
// explicit rejection or a complete acknowledgement makes retry/clearing safe.
async function fugueIssueMutation(path, options, {kind, fileCount = 0}) {
  const uncertain = () => Object.assign(new Error(
    'The result was not confirmed. Check the existing issue before posting the comment or uploading files again.'
  ), {uploadUncertain: true});
  let response, payload;
  try {
    response = await fugueFetch(path, options);
    payload = await response.json();
  } catch { throw uncertain(); }
  if (!response.ok) {
    if ([400, 401, 403, 404, 405, 409, 413, 415, 422, 429].includes(response.status)
        && typeof payload?.error?.message === 'string') {
      throw Object.assign(new Error(payload.error.message), {confirmedRejected:true});
    }
    throw uncertain();
  }
  const attachmentsValid = Array.isArray(payload?.attachments) && payload.attachments.every(file =>
    typeof file?.url === 'string' && file.url.length > 0
    && ((typeof file.name === 'string' && file.name.length > 0)
      || (typeof file.title === 'string' && file.title.length > 0)));
  if (!attachmentsValid) throw uncertain();
  if (payload.warning !== undefined) {
    if (!['upload_not_confirmed', 'comment_not_confirmed'].includes(payload.warning?.code)
        || typeof payload.warning.message !== 'string' || !payload.warning.message) throw uncertain();
    return payload;
  }
  if (payload.attachments.length !== fileCount) throw uncertain();
  if (kind === 'comment' && (typeof payload.result?.comment_id !== 'string' || !payload.result.comment_id)) {
    throw uncertain();
  }
  return payload;
}
