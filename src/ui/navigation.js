(() => {
  const rail = document.querySelector('[data-area]');
  const scope = document.getElementById('fugueScope');
  if (!rail || !scope) return;
  const area = rail.dataset.area;
  const pathId = location.pathname.match(/^\/repositories\/(repository-[a-zA-Z0-9-]+)(?:\/|$)/)?.[1];
  const queryId = new URL(location.href).searchParams.get('workspace');
  let selected = pathId || (/^repository-[a-zA-Z0-9-]+$/.test(queryId || '') ? queryId : '');
  const crumb = document.getElementById('fugueScopeCrumb');
  const settings = document.getElementById('fugueRepoSettings');
  const tabs = rail.querySelectorAll('.fugue-console-tabs a');
  const workspacesById = new Map();
  if (selected && area === 'setup') workspacesById.set(selected, {running: true, configured: false});
  const destination = (page, id) => {
    const prefix = id ? `/repositories/${encodeURIComponent(id)}` : '';
    if (page === 'agents') return '/console' + (id ? `?workspace=${encodeURIComponent(id)}` : '');
    const workspace = workspacesById.get(id);
    if (workspace && !workspace.running) return '/ensemble#repositoryWorkspacesPanel';
    if (workspace && !workspace.configured) return prefix + '/setup';
    if (page === 'board') return id ? prefix + '/issues' : '/queues';
    if (['repositories', 'settings', 'setup'].includes(page)) return prefix + '/ensemble';
    return prefix + '/overview';
  };
  const update = () => {
    for (const [index, page] of ['home', 'board', 'agents'].entries()) tabs[index].href = destination(page, selected);
    document.getElementById('fugueRepositories').href = destination('repositories', selected);
    crumb.href = destination('home', selected);
    settings.hidden = !selected || ['setup', 'agents'].includes(area);
    settings.href = selected ? `/repositories/${encodeURIComponent(selected)}/settings` : './settings';
  };
  if (selected) {
    scope.add(new Option('Repository', selected));
    scope.value = selected;
    crumb.textContent = 'Repository';
  }
  update();
  scope.addEventListener('change', () => { location.href = destination(area, scope.value); });
  const sessionAlert = document.createElement('aside');
  sessionAlert.className = 'fugue-session-alert';
  sessionAlert.setAttribute('role', 'alert');
  sessionAlert.hidden = true;
  document.body.append(sessionAlert);
  function showSessionExpired() {
    const text = document.createElement('span');
    text.textContent = 'Your browser session has expired or access was rejected. Displayed activity may be out of date.';
    const signIn = document.createElement('a');
    signIn.href = location.pathname + location.search + location.hash;
    signIn.textContent = 'Sign in again';
    sessionAlert.replaceChildren(text, signIn);
    sessionAlert.hidden = false;
  }
  let refreshing = false;
  async function refreshWorkspaces() {
    if (refreshing) return;
    refreshing = true;
    try {
      const response = await fetch('/api/workspaces', {cache: 'no-store', headers: {'Accept': 'application/json'}});
      if ([401, 403].includes(response.status) || (response.ok && !response.headers.get('content-type')?.includes('application/json'))) {
        showSessionExpired();
        return;
      }
      if (!response.ok) throw new Error('Repository list unavailable');
      const {workspaces} = await response.json();
      sessionAlert.hidden = true;
      scope.removeAttribute('title');
      workspacesById.clear();
      for (const option of Array.from(scope.options)) if (option.value && option.value !== selected) option.remove();
      for (const workspace of workspaces) {
        workspacesById.set(workspace.id, workspace);
        const name = `${workspace.repository.owner}/${workspace.repository.repository}`;
        let option = Array.from(scope.options).find(item => item.value === workspace.id);
        if (!option) { option = new Option(name, workspace.id); scope.add(option); }
        option.textContent = name;
        if (workspace.id === selected) crumb.textContent = name;
      }
      scope.value = selected;
      update();
    } catch {
      scope.title = 'Repository list unavailable. Reload to retry.';
    } finally {
      refreshing = false;
    }
  }
  refreshWorkspaces();
  // Check only browser access and repository labels. Native console state and
  // stream recovery remain owned by Meta.
  setInterval(() => { if (document.visibilityState === 'visible') refreshWorkspaces(); }, 60000);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') refreshWorkspaces();
  });
  // The native console owns its team filters. Repository context is retained for
  // returning to Home or Board; the shared console continues to show all teams.
  if (area === 'agents') {
    const note = document.createElement('span');
    note.className = 'fugue-console-scope-note';
    note.textContent = 'Shared console - all teams';
    document.querySelector('.fugue-breadcrumb').append(note);
  }
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape') rail.querySelector('details').open = false;
  });
})();
