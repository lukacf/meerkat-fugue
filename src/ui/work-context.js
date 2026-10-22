(() => {
  const label = document.getElementById('queueConnection');
  if (!label) return;
  fugueFetch('/api/v1/workspace', {cache:'no-store'}).then(async response => {
    if (!response.ok) throw new Error('Unable to load queue connection');
    return response.json();
  }).then(context => {
    const tracker = {'fugue-ims':'Fugue tickets',linear:'Linear',jira:'Jira'}[context.tracker] || context.tracker;
    const repositories = context.repositories.filter(repo => repo.split('/').slice(1).join('/') !== context.project);
    label.textContent = `${tracker} / ${context.project || 'Configured project'}${repositories.length ? ' / ' + repositories.join(', ') : ''}`;
  }).catch(error => { label.textContent = error.message; });
})();
