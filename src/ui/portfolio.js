// Read-only projections of each repository's existing board and setup APIs.
// Lane kinds and actions come from the server; this view never advances work.
(() => {
  const root = document.getElementById('portfolio');
  if (!root) return;
  const mode = root.dataset.view;
  const nodes = id => document.getElementById(id);
  const make = (tag, cls, text) => {
    const element = document.createElement(tag); element.className = cls; element.textContent = text; return element;
  };
  const link = (text, href, cls = 'fugue-button') => {
    const element = make('a', cls, text); element.href = href; return element;
  };
  let loading = false;
  let hasSnapshot = false;
  async function read(url) {
    const response = await fetch(url, {cache: 'no-store'});
    if (!response.ok) throw new Error(response.status === 401 || response.status === 403
      ? 'Access was rejected. Sign in again or check this repository\'s connections.'
      : `Could not load repository data (HTTP ${response.status}).`);
    if (!response.headers.get('content-type')?.includes('application/json')) throw new Error('Your browser session may have expired. Sign in again.');
    return response.json();
  }
  function issueLink(workspace, issue, lane) {
    const row = link('', `${workspace.prefix}/issues/${encodeURIComponent(issue.identifier)}`, 'portfolio-issue');
    row.setAttribute('aria-label', `${issue.identifier}, ${issue.title}, ${lane.name}. Open issue`);
    const identity = make('div', 'portfolio-issue-title', '');
    identity.append(make('span', 'portfolio-identifier', issue.identifier), make('strong', '', issue.title));
    const text = make('div', 'portfolio-issue-content', '');
    text.append(identity, make('small', '', issue.runtime?.launch_failure ? issue.runtime.launch_failure.message : issue.runtime?.kind === 'retrying' ? issue.runtime.error || 'Waiting to retry' : issue.runtime?.last_message || issue.runtime?.last_event || workspace.name));
    row.append(make('span', `portfolio-state ${lane.kind === 'attention' || issue.runtime?.launch_failure ? 'attention' : ''}`, issue.runtime?.launch_failure ? 'Team startup failed' : lane.name), text, make('span', 'portfolio-open', 'Open →'));
    return row;
  }
  function message(text, action) {
    const empty = make('div', 'fugue-empty', text);
    if (action) empty.append(document.createElement('br'), action);
    return empty;
  }
  function quietState(title, text, kind = '') {
    const state = make('div', `portfolio-quiet ${kind}`, '');
    const motif = make('span', 'portfolio-quiet-mark', '');
    motif.setAttribute('aria-hidden', 'true');
    for (let i = 0; i < 3; i++) motif.append(make('i', '', ''));
    const copy = make('div', '', '');
    copy.append(make('strong', '', title), make('p', '', text));
    state.append(motif, copy);
    return state;
  }
  function repoCard(workspace) {
    const card = make('section', 'fugue-panel portfolio-repository', '');
    const header = make('div', 'fugue-panel-head', '');
    const title = make('div', '', '');
    const heading = make('h2', '', workspace.name);
    heading.id = `portfolio-${workspace.id || 'local'}`;
    card.setAttribute('aria-labelledby', heading.id);
    title.append(heading);
    const status = !workspace.running ? 'Stopped' : !workspace.configured ? 'Configuration needed' : workspace.setupError ? 'Team preparation needs attention' : workspace.setupPhase === 'preparing' ? 'Preparing team' : workspace.board ? `${workspace.board.totals.running} running · ${workspace.board.totals.retrying} awaiting retry${workspace.board.totals.launch_failed ? ` · ${workspace.board.totals.launch_failed} team startup failed` : ''}` : 'Board unavailable';
    title.append(make('p', 'portfolio-repository-status', status));
    const target = workspace.running ? workspace.configured ? `${workspace.prefix}/issues` : `${workspace.prefix}/setup` : '/ensemble#repositoryWorkspacesPanel';
    header.append(title, link(!workspace.running ? 'Open repository' : workspace.configured ? 'Open board' : 'Continue setup', target));
    card.append(header);
    if (workspace.error) card.append(message(workspace.error, link('Repository settings', `${workspace.prefix}/settings`)));
    if (workspace.board) {
      const counts = make('div', 'portfolio-counts', '');
      for (const lane of workspace.board.lanes) {
        const count = make('div', '', '');
        count.append(make('span', '', lane.action === 'merge' && lane.name === 'Deploy' ? 'Merge' : lane.name), make('strong', '', String(lane.issues.length)));
        counts.append(count);
      }
      card.append(counts);
      if (mode === 'board') {
        const lanes = make('div', 'portfolio-lanes', '');
        for (const lane of workspace.board.lanes) {
          const column = make('section', `portfolio-lane ${lane.kind}`, '');
          column.append(make('h3', '', `${lane.action === 'merge' && lane.name === 'Deploy' ? 'Merge' : lane.name} (${lane.issues.length})`));
          for (const issue of lane.issues) column.append(issueLink(workspace, issue, lane));
          if (!lane.issues.length) column.append(make('p', 'portfolio-no-issues', 'No issues'));
          lanes.append(column);
        }
        card.append(lanes);
      } else {
        for (const lane of workspace.board.lanes.filter(item => item.kind === 'active')) {
          for (const issue of lane.issues) card.append(issueLink(workspace, issue, lane));
        }
      }
      card.append(make('p', 'portfolio-freshness', `Board fetched ${new Date(workspace.board.generated_at).toLocaleTimeString()}. Lane and runtime data from this repository.`));
    }
    return card;
  }
  async function loadWorkspace(workspace) {
    if (!workspace.running || !workspace.configured) return workspace;
    const results = await Promise.allSettled([read(workspace.prefix + '/api/v1/issues/board'), read(workspace.prefix + '/api/ensemble')]);
    if (results[0].status === 'fulfilled') workspace.board = results[0].value;
    else workspace.error = results[0].reason.message;
    if (results[1].status === 'fulfilled') {
      workspace.setupPhase = results[1].value.setup?.phase;
      workspace.setupError = results[1].value.setup?.error;
    } else workspace.setupError = 'Team status unavailable. Open Repositories to check preparation.';
    return workspace;
  }
  async function load() {
    if (loading) return;
    loading = true;
    nodes('portfolioRetry').disabled = true;
    try {
      let workspaces;
      if (document.body.dataset.queueEnabled !== 'false') {
        const context = await read(fugueUrl('/api/v1/workspace'));
        workspaces = [{prefix: fuguePrefix, name: context.repositories.join(', ') || context.project || 'Repository', running:true, configured:true}];
      } else {
        const listing = await read('/api/workspaces');
        workspaces = listing.workspaces.map(workspace => ({...workspace, prefix: `/repositories/${encodeURIComponent(workspace.id)}`, name: `${workspace.repository.owner}/${workspace.repository.repository}`}));
      }
      // Bound tracker requests on hosts with many repositories.
      for (let offset = 0; offset < workspaces.length; offset += 4) await Promise.all(workspaces.slice(offset, offset + 4).map(loadWorkspace));
      const cards = workspaces.filter(workspace => mode === 'board' || (workspace.running && workspace.configured)).map(repoCard);
      nodes('portfolioRepositories').replaceChildren(...cards);
      if (!cards.length) nodes('portfolioRepositories').append(workspaces.length
        ? quietState('Prepare your team', 'Finish repository setup to see its board and active work here.', 'waiting')
        : message('Connect a GitHub or GitHub Enterprise repository to give it a tracker, a board and a team.', link('Add your first repository', '/ensemble#add', 'fugue-button primary')));
      if (mode === 'home') {
        const attention = [], recent = [], problems = [];
        for (const workspace of workspaces) {
          if (!workspace.running || !workspace.configured || workspace.error || workspace.setupError || workspace.setupPhase === 'preparing') {
            const problem = make('div', 'portfolio-problem', '');
            problem.append(make('strong', '', workspace.name), make('p', '', workspace.error || workspace.setupError || (!workspace.running ? 'Repository is stopped.' : !workspace.configured ? 'Finish connecting the repository, issue tracker and AI provider.' : 'Mapping the code and preparing its specialists.')),
              link(workspace.configured ? 'Open repository' : 'Continue configuration', workspace.running ? workspace.prefix + (workspace.configured ? '/ensemble' : '/setup') : '/ensemble#repositoryWorkspacesPanel'));
            problems.push(problem);
          }
          for (const lane of workspace.board?.lanes || []) for (const issue of lane.issues) {
            if (lane.kind === 'attention' || issue.runtime?.kind === 'retrying' || issue.runtime?.launch_failure) attention.push({workspace, issue, lane});
            if (lane.kind === 'terminal') recent.push({workspace, issue, lane});
          }
        }
        nodes('portfolioAttention').replaceChildren(...attention.map(({workspace,issue,lane}) => issueLink(workspace,issue,lane)));
        if (!attention.length) nodes('portfolioAttention').append(workspaces.some(w => w.error)
          ? message('Some boards could not be loaded. Check the repository errors below.')
          : quietState(cards.length ? 'Nothing waiting on you' : 'A clear place to begin', cards.length ? 'No issues need attention or are awaiting retry.' : 'Connect a repository and prepare its team to start working.', cards.length ? 'clear' : 'waiting'));
        nodes('portfolioTitle').textContent = !workspaces.length ? 'Welcome to Meerkat Fugue.' : attention.length ? `${attention.length} ${attention.length === 1 ? 'issue needs' : 'issues need'} attention.` : 'Your work at a glance.';
        nodes('portfolioProblems').replaceChildren(...problems);
        if (!problems.length) nodes('portfolioProblems').append(message('No setup problems reported.'));
        recent.sort((a,b) => Date.parse(b.issue.updated_at || 0) - Date.parse(a.issue.updated_at || 0));
        nodes('portfolioRecent').replaceChildren(...recent.slice(0,5).map(({workspace,issue,lane}) => issueLink(workspace,issue,lane)));
        if (!recent.length) nodes('portfolioRecent').append(quietState('Room for your next improvement', 'Completed and canceled issues will appear here.', 'recent'));
      }
      hasSnapshot = true;
      nodes('portfolioError').hidden = true;
      nodes('portfolioStatus').textContent = `Updated ${new Date().toLocaleTimeString()} · ${workspaces.length} ${workspaces.length === 1 ? 'repository' : 'repositories'}`;
    } catch (error) {
      nodes('portfolioError').textContent = error.message + (hasSnapshot ? ' Previously loaded data may be out of date.' : ' Use Refresh to try again.');
      if (!hasSnapshot) {
        nodes('portfolioRepositories').replaceChildren(message('Repository boards are unavailable.'));
        if (mode === 'home') {
          nodes('portfolioAttention').replaceChildren(message('Issue states could not be checked.'));
          nodes('portfolioProblems').replaceChildren(message('Repository setup could not be checked.'));
          nodes('portfolioRecent').replaceChildren(message('Recent work is unavailable.'));
        }
      }
      nodes('portfolioError').hidden = false;
      nodes('portfolioStatus').textContent = 'Update failed';
    } finally {
      loading = false;
      nodes('portfolioRetry').disabled = false;
    }
  }
  nodes('portfolioRetry').addEventListener('click', load);
  async function refresh() { await load(); setTimeout(refresh, 30000); }
  refresh();
})();
