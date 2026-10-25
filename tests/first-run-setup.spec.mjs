import {browserDocument} from './browser-document.mjs';
import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

const html = (await readFile(new URL('../src/setup/setup.html',import.meta.url),'utf8'))
  .replaceAll('__DEFAULT_MODEL__','gpt-6-astra').replaceAll('__OPENAI_MODEL__','gpt-6-astra').replaceAll('__ANTHROPIC_MODEL__','claude-test').replaceAll('__GEMINI_MODEL__','gemini-test')
  .replace('__APP_CSS__',await readFile(new URL('../src/ui/app.css',import.meta.url),'utf8'));

async function setup(page, initial={}, prefix='', initialWorkflow=null) {
  const state={model:null,github:null,tracker:null,environment_providers:['openai'],...initial};
  const writes=[];
  let workflow=initialWorkflow;
  let preparation={phase:'draft'};
  await page.route('http://fugue.test/**',async route=>{
    const url=new URL(route.request().url());
    if(prefix && !url.pathname.startsWith(prefix+'/')) return route.fulfill({status:404});
    url.pathname=url.pathname.slice(prefix.length);
    if(url.pathname==='/setup')return route.fulfill({contentType:'text/html',body:browserDocument(html)});
    if(url.pathname==='/api/setup/auth/accounts') return route.fulfill({json:[{provider:'openai',name:'rkat-openai',method:'api_key',auth_binding:{realm:'global',binding:'default_openai'}}]});
    if(url.pathname==='/api/setup/connections') {
      if(route.request().method()==='POST') {
        const {connection,value}=route.request().postDataJSON();writes.push({connection,value});
        if(connection==='model') state.model={provider:value.provider,model:value.model,method:value.credential.method==='api_key'?'binding':value.credential.method,auth_binding:value.credential.method==='api_key'?{realm:'global',binding:'default_openai'}:value.credential.auth_binding||null};
        if(connection==='github') state.github={host:value.host,method:value.credential.method};
        if(connection==='tracker') state.tracker={kind:value.kind,site:value.site||null,email:value.email||null};
        preparation={phase:'draft'};
      }
      return route.fulfill({json:state});
    }
    if(url.pathname==='/api/setup/workflow') return route.fulfill({body:JSON.stringify(workflow),contentType:'application/json'});
    if(url.pathname==='/api/setup/preparation') {
      if(route.request().method()==='POST') {workflow={...route.request().postDataJSON(),lane_configuration:state.tracker.kind};writes.push({connection:'workflow',value:workflow});preparation={phase:'ready'};}
      return route.fulfill({json:preparation});
    }
    if(url.pathname==='/api/setup/check/jira-states') return route.fulfill({json:{states:[{name:'Open'},{name:'In Progress'},{name:'Resolved'},{name:'Reopened'},{name:'Closed'}]}});
    if(url.pathname==='/api/setup/check/github') return route.fulfill({json:{login:'example-user'}});
    if(url.pathname==='/api/setup/start') {writes.push({connection:'start'});return route.fulfill({status:202,json:{phase:'ready'}});}
    if(url.pathname==='/api/v1/build')return route.fulfill({json:{version:'test'}});
    if(url.pathname==='/issues')return route.fulfill({contentType:'text/html',body:'<h1>Work queue</h1>'});
    return route.fulfill({status:404});
  });
  await page.goto('http://fugue.test'+prefix+'/setup');
  return {state,writes};
}

const connected={model:{provider:'openai',model:'example-model',method:'environment'},github:{host:'github.com',method:'gh_cli'},tracker:{kind:'local'}};

test('new built-in queues save a Merge lane and keep it after reload',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Merge');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.lanes.deploy_action).toBe('Merge');
  await page.reload();
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Merge');
});

test('saved built-in merge status names are preserved when reopening setup',async({page})=>{
  await setup(page,connected,'',{...structuredClone(managedDraft),lane_configuration:'local'});
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Deploy');
  await page.reload();
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Deploy');
});

test('reviewer policy is saved explicitly and changing it requires preparation',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  const count=page.getByLabel('Independent specialists required',{exact:true});
  await expect(count).toHaveValue('1');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.independent_reviewer_count).toBe(1);
  await count.fill('3');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.reload();
  await expect(count).toHaveValue('3');
  expect(writes.at(-1).value.independent_reviewer_count).toBe(3);
});

async function holdConnectionSave(page,connection) {
  let release,started;
  const pending=new Promise(resolve=>release=resolve);
  const requested=new Promise(resolve=>started=resolve);
  await page.route('**/api/setup/connections',async route=>{
    if(route.request().method()==='POST'&&route.request().postDataJSON().connection===connection) {
      started();
      await pending;
    }
    await route.fallback();
  });
  return {release,requested};
}

test('blank setup cannot start agents before connections and preparation',async({page})=>{
  await setup(page);
  await expect(page.getByRole('heading',{name:'Connect your repository.'})).toBeVisible();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeDisabled();
  await expect(page.locator('#environmentHint')).toContainText('openai');
});

test('API keys leave the form after saving and are not restored on refresh',async({page})=>{
  const {writes}=await setup(page);
  await page.getByLabel('AI authentication',{exact:true}).first().selectOption('api_key');
  await page.getByLabel('API key',{exact:true}).fill('model-secret-sentinel');
  await page.getByRole('button',{name:'Save AI selection'}).click();
  await expect(page.locator('#modelResult')).toHaveText('AI account selected.');
  expect(writes[0].value.credential.key).toBe('model-secret-sentinel');
  await expect(page.locator('#modelKey')).toHaveValue('');
  await page.reload();
  await expect(page.locator('#modelKey')).toHaveValue('');
  await expect(page.locator('body')).not.toContainText('model-secret-sentinel');
});

test('GitHub account checks use the saved hostname and show the account',async({page})=>{
  const {writes}=await setup(page);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://git.example.com/example/application');
  await page.getByRole('button',{name:'Save repository access'}).click();
  await expect(page.locator('#githubSaved')).toContainText('git.example.com');
  await page.getByRole('button',{name:'Check account access'}).click();
  await expect(page.locator('#githubResult')).toHaveText('Connected as example-user.');
  expect(writes[0]).toEqual({connection:'github',value:{host:'git.example.com',credential:{method:'gh_cli'}}});
});

test('Jira credentials are separate from repository workflow scope',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('Issue tracker',{exact:true}).selectOption('jira');
  await page.getByLabel('Jira site',{exact:true}).fill('https://example.atlassian.net');
  await page.getByLabel('Account email',{exact:true}).fill('test@example.com');
  await page.getByLabel('API token',{exact:true}).fill('jira-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#jiraScopeFields')).toBeVisible();
  await expect(page.locator('#trackerKey')).toHaveValue('');
  expect(writes[0].value.api_token).toBe('jira-secret-sentinel');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
});

test('saved Jira access restores its defaults when no workflow exists yet',async({page})=>{
  await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  for(let attempt=0;attempt<2;attempt++) {
    await expect(page.getByLabel('Issue tracker',{exact:true})).toHaveValue('jira');
    await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Open');
    await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Resolved');
    await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Reopened');
    if(attempt===0) await page.reload();
  }
});

// Add repository writes this workflow before the user chooses a tracker.
const managedDraft={
  repository_url:'https://github.com/example/application',owner:'example',repository:'application',
  checkout:null,tracker_project:null,tracker_team:null,jira_issue_type:'Task',jira_jql:null,
  jira_issue_labels:[],lane_configuration:'generated',
  lanes:{parked:'Backlog',intake:'Queue',active:'Active',review_action:'Review',deploy_action:'Deploy',blocked:'Blocked',success:'Done',canceled:'Canceled'},
};

test('managed Jira setup retains tracker defaults after reload before preparation',async({page})=>{
  await setup(page,connected,'',structuredClone(managedDraft));
  await page.getByLabel('Issue tracker',{exact:true}).selectOption('jira');
  await page.getByLabel('Jira site',{exact:true}).fill('https://example.atlassian.net');
  await page.getByLabel('Account email',{exact:true}).fill('test@example.com');
  await page.getByLabel('API token',{exact:true}).fill('jira-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerResult')).toHaveText('Saved privately.');
  await page.reload();
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Open');
  await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Resolved');
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Reopened');
  await expect(page.getByLabel('Repository URL',{exact:true})).toHaveValue(managedDraft.repository_url);
});

for(const tracker of ['local','linear']) test(`managed Jira workflow switches to ${tracker} across reload before preparation`,async({page})=>{
  const jiraDraft={...structuredClone(managedDraft),lane_configuration:'jira',tracker_project:'EXAMPLE',jira_jql:'labels = "fugue-app"',
    lanes:{parked:'Open',intake:'Open',active:'In Progress',review_action:'Resolved',deploy_action:'Reopened',blocked:'In Progress',success:'Closed',canceled:'Closed'}};
  await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}},'',jiraDraft);
  await page.getByLabel('Issue tracker',{exact:true}).selectOption(tracker);
  if(tracker==='linear') await page.getByLabel('API token',{exact:true}).fill('linear-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerResult')).toHaveText('Saved privately.');
  await page.reload();
  await expect(page.getByLabel('Draft tickets',{exact:true})).toHaveValue('Backlog');
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Queue');
  await expect(page.getByLabel('Review',{exact:true})).toHaveValue('Review');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
});

test('unannotated saved workflows preserve custom mappings on reload',async({page})=>{
  const draft={...structuredClone(managedDraft),lanes:{...managedDraft.lanes,intake:'Triage accepted'}};
  delete draft.lane_configuration;
  await setup(page,connected,'',draft);
  await page.reload();
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Triage accepted');
});

for(const failure of ['client','server']) test(`stale ready status cannot undo ${failure} preparation rejection`,async({page})=>{
  const {writes}=await setup(page,connected,'',structuredClone(managedDraft));
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  let release,started;
  const pending=new Promise(resolve=>release=resolve),requested=new Promise(resolve=>started=resolve);
  await page.route('**/api/setup/preparation',async route=>{
    if(route.request().method()==='POST') return route.fulfill({status:400,json:{error:'Repository preparation refused'}});
    started();await pending;return route.fulfill({json:{phase:'ready'}});
  });
  await page.reload();await requested;
  await expect(page.getByLabel('Repository URL',{exact:true})).toBeEnabled();
  await page.getByLabel('Repository URL',{exact:true}).fill(failure==='client'?'invalid repository':'https://github.com/example/another');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  const error=failure==='client'?'Enter a GitHub or GitHub Enterprise repository URL.':'Repository preparation refused';
  await expect(page.locator('#workflowResult')).toHaveText(error);
  const response=page.waitForResponse(r=>r.url().endsWith('/api/setup/preparation')&&r.request().method()==='GET');
  release();await (await response).finished();
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await expect(page.locator('#workflowResult')).toHaveText(error);
  expect(writes.filter(write=>write.connection==='start')).toHaveLength(0);
});

for(const [label,value]of [['Commit author name','New Author'],['Repository URL','https://github.com/example/another']]) test(`pending GitHub save does not dirty access after editing ${label}`,async({page})=>{
  await setup(page,connected,'',structuredClone(managedDraft));
  if(label==='Commit author name') await page.getByText('Commit author',{exact:true}).click();
  const pending=await holdConnectionSave(page,'github');
  await page.getByRole('button',{name:'Save repository access'}).click();await pending.requested;
  await page.getByLabel(label,{exact:true}).fill(value);
  pending.release();
  await expect(page.locator('#githubResult')).toHaveText('Saved privately.');
  await expect(page.getByLabel(label,{exact:true})).toHaveValue(value);
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeEnabled();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
});

test('saved Jira workflow mappings survive a slower connection restore',async({page})=>{
  await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByLabel('Tracker project',{exact:true}).fill('EXAMPLE');
  await page.getByLabel('Jira ticket filter',{exact:true}).fill('labels = "fugue-app"');
  await page.getByText('Workflow statuses',{exact:true}).click();
  await page.getByLabel('Ready to start',{exact:true}).fill('Ready for work');
  await page.getByLabel('Awaiting review',{exact:true}).fill('Peer review');
  await page.getByLabel('Merge',{exact:true}).fill('Awaiting merge');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.route('**/api/setup/connections',async route=>{
    await new Promise(resolve=>setTimeout(resolve,150));
    await route.fallback();
  });
  await page.reload();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Ready for work');
  await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Peer review');
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Awaiting merge');
});

for(const endpoint of ['connections','workflow']) test(`setup waits for saved ${endpoint} before accepting edits`,async({page})=>{
  await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  let release,started;
  const pending=new Promise(resolve=>release=resolve);
  const requested=new Promise(resolve=>started=resolve);
  await page.route(`**/api/setup/${endpoint}`,async route=>{
    started();
    await pending;
    await route.fallback();
  });
  await page.reload();
  await requested;
  try {
    // Late saved values must not replace edits made on an apparently ready form.
    await expect(page.getByLabel('Repository URL',{exact:true})).toBeDisabled();
    await expect(page.getByLabel('Issue tracker',{exact:true})).toBeDisabled();
    await expect(page.locator('#model')).toBeDisabled();
    await expect(page.locator('#lane-intake')).toBeDisabled();
    await expect(page.getByRole('button',{name:'Save tracker access'})).toBeDisabled();
    await expect(page.getByRole('button',{name:'Check account access'})).toBeDisabled();
    await expect(page.getByText('Loading saved configuration...',{exact:true})).toBeVisible();
  } finally {
    release();
  }
  await expect(page.getByLabel('Issue tracker',{exact:true})).toBeEnabled();
  await expect(page.getByLabel('Issue tracker',{exact:true})).toHaveValue('jira');
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Open');
  await expect(page.getByText('Loading saved configuration...',{exact:true})).toBeHidden();
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/new-selection');
  await expect(page.getByLabel('Repository URL',{exact:true})).toHaveValue('https://github.com/example/new-selection');
});

test('failed workflow restore keeps preparation disabled',async({page})=>{
  await setup(page,connected);
  await page.route('**/api/setup/workflow',route=>route.fulfill({status:500,json:{error:'Saved workflow is temporarily unavailable.'}}));
  await page.reload();
  await expect(page.locator('#pageError')).toContainText('Saved workflow is temporarily unavailable.');
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeDisabled();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await expect(page.getByLabel('Repository URL',{exact:true})).toBeDisabled();
  await expect(page.getByRole('button',{name:'Save tracker access'})).toBeDisabled();
  await expect(page.locator('#pageError')).toContainText('Reload this page to retry.');
  await page.unroute('**/api/setup/workflow');
  await page.reload();
  await expect(page.getByLabel('Repository URL',{exact:true})).toBeEnabled();
  await expect(page.getByRole('button',{name:'Save tracker access'})).toBeEnabled();
});

for(const tracker of ['local','linear']) test(`switching from Jira to ${tracker} restores that tracker's workflow defaults`,async({page})=>{
  const {writes}=await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Open');
  await page.getByLabel('Issue tracker',{exact:true}).selectOption(tracker);
  if(tracker==='linear') await page.getByLabel('API token',{exact:true}).fill('linear-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerSaved')).toHaveText(`Saved: ${tracker}`);
  await expect(page.getByLabel('Draft tickets',{exact:true})).toHaveValue('Backlog');
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Queue');
  await expect(page.getByLabel('Review',{exact:true})).toHaveValue('Review');
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue(tracker==='local'?'Merge':'Deploy');
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  if(tracker==='linear') {
    await page.getByLabel('Tracker project',{exact:true}).fill('example-project');
    await page.getByLabel('Linear team',{exact:true}).fill('example-team');
  }
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.find(write=>write.connection==='workflow').value.lanes).toMatchObject({parked:'Backlog',intake:'Queue',review_action:'Review',deploy_action:tracker==='local'?'Merge':'Deploy'});
});

test('a rejected tracker change preserves workflow mappings and blocks preparation',async({page})=>{
  await setup(page,connected);
  await page.getByText('Workflow statuses',{exact:true}).click();
  await page.getByLabel('Ready to start',{exact:true}).fill('Ready for work');
  await page.getByLabel('Review',{exact:true}).fill('Peer review');
  await page.route('**/api/setup/connections',route=>route.request().method()==='POST'
    ? route.fulfill({status:400,json:{error:'Tracker connection could not be saved.'}})
    : route.fallback());
  await page.getByLabel('Issue tracker',{exact:true}).selectOption('jira');
  await page.getByLabel('Jira site',{exact:true}).fill('https://example.atlassian.net');
  await page.getByLabel('Account email',{exact:true}).fill('test@example.com');
  await page.getByLabel('API token',{exact:true}).fill('jira-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerResult')).toHaveText('Tracker connection could not be saved.');
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Ready for work');
  await expect(page.getByLabel('Review',{exact:true})).toHaveValue('Peer review');
  await expect(page.locator('#trackerSaved')).toHaveText('Saved: local');
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeDisabled();
});

test('resaving Jira credentials preserves custom workflow mappings',async({page})=>{
  await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  await page.getByText('Workflow statuses',{exact:true}).click();
  await page.getByLabel('Ready to start',{exact:true}).fill('Ready for work');
  await page.getByLabel('Awaiting review',{exact:true}).fill('Peer review');
  await page.getByLabel('API token',{exact:true}).fill('replacement-jira-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerResult')).toHaveText('Saved privately.');
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Ready for work');
  await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Peer review');
});

test('editing Jira credentials during a save keeps the newer token unsaved',async({page})=>{
  const {writes}=await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  const save=await holdConnectionSave(page,'tracker');
  await page.getByLabel('API token',{exact:true}).fill('first-jira-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await save.requested;
  await page.getByLabel('API token',{exact:true}).fill('newer-jira-sentinel');
  save.release();
  await expect(page.getByRole('button',{name:'Save tracker access'})).toBeEnabled();
  await expect(page.getByLabel('API token',{exact:true})).toHaveValue('newer-jira-sentinel');
  await expect(page.locator('#trackerResult')).toContainText('unsaved');
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeDisabled();
  expect(writes[0].value.api_token).toBe('first-jira-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerResult')).toHaveText('Saved privately.');
  expect(writes[1].value.api_token).toBe('newer-jira-sentinel');
  await expect(page.getByLabel('API token',{exact:true})).toHaveValue('');
});

test('editing AI selection during a save keeps the newer authentication choice',async({page})=>{
  await setup(page,connected);
  const save=await holdConnectionSave(page,'model');
  await page.getByLabel('AI authentication',{exact:true}).selectOption('api_key');
  await page.getByLabel('API key',{exact:true}).fill('model-key-sentinel');
  await page.getByRole('button',{name:'Save AI selection'}).click();
  await save.requested;
  await page.getByLabel('AI authentication',{exact:true}).selectOption('environment');
  await page.locator('#model').fill('new-model-selection');
  save.release();
  await expect(page.getByRole('button',{name:'Save AI selection'})).toBeEnabled();
  await expect(page.getByLabel('AI authentication',{exact:true})).toHaveValue('environment');
  await expect(page.locator('#model')).toHaveValue('new-model-selection');
  await expect(page.locator('#modelResult')).toContainText('unsaved');
  await expect(page.locator('#modelKey')).toHaveValue('');
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeDisabled();
});

test('workflow edits during a tracker save survive its default initialization',async({page})=>{
  await setup(page,connected);
  const save=await holdConnectionSave(page,'tracker');
  await page.getByLabel('Issue tracker',{exact:true}).selectOption('jira');
  await page.getByLabel('Jira site',{exact:true}).fill('https://example.atlassian.net');
  await page.getByLabel('Account email',{exact:true}).fill('test@example.com');
  await page.getByLabel('API token',{exact:true}).fill('jira-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await save.requested;
  await page.getByText('Workflow statuses',{exact:true}).click();
  await page.getByLabel('Ready to start',{exact:true}).fill('Ready for work');
  save.release();
  await expect(page.locator('#trackerResult')).toHaveText('Saved privately.');
  await expect(page.getByLabel('Ready to start',{exact:true})).toHaveValue('Ready for work');
  await expect(page.getByLabel('Draft tickets',{exact:true})).toHaveValue('Open');
  await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Resolved');
});

test('an older AI save response does not replace a newer saved Jira connection',async({page})=>{
  const {state}=await setup(page,connected);
  let release,started;
  const pending=new Promise(resolve=>release=resolve);
  const requested=new Promise(resolve=>started=resolve);
  await page.route('**/api/setup/connections',async route=>{
    if(route.request().method()==='POST'&&route.request().postDataJSON().connection==='model') {
      const {value}=route.request().postDataJSON();
      state.model={provider:value.provider,model:value.model,method:value.credential.method};
      const response=structuredClone(state);
      started();
      await pending;
      return route.fulfill({json:response});
    }
    await route.fallback();
  });
  await page.locator('#model').fill('new-model-selection');
  await page.getByRole('button',{name:'Save AI selection'}).click();
  await requested;
  await page.getByLabel('Issue tracker',{exact:true}).selectOption('jira');
  await page.getByLabel('Jira site',{exact:true}).fill('https://example.atlassian.net');
  await page.getByLabel('Account email',{exact:true}).fill('test@example.com');
  await page.getByLabel('API token',{exact:true}).fill('jira-secret-sentinel');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await expect(page.locator('#trackerSaved')).toHaveText('Saved: jira / https://example.atlassian.net');
  release();
  await expect(page.locator('#modelResult')).toHaveText('AI account selected.');
  await expect(page.locator('#trackerSaved')).toHaveText('Saved: jira / https://example.atlassian.net');
  await expect(page.locator('#jiraScopeFields')).toBeVisible();
  await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Resolved');
});

test('one repository selection produces preparation and a same-origin handoff',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.locator('#workflowResult')).toContainText('Ready to start');
  expect(writes[0].value.owner).toBe('example');
  expect(writes[0].value.lanes.intake).toBe('Queue');
  expect(writes[0].value.checkout).toBeNull();
  await page.getByRole('button',{name:'Start Fugue'}).click();
  await expect(page).toHaveURL('http://fugue.test/issues');
  expect(writes.at(-1).connection).toBe('start');
});

test('failed runtime startup reports its setup error without the long startup timeout',async({page})=>{
  await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.route('**/api/v1/build',route=>route.fulfill({status:404}));
  await page.route('**/api/setup/preparation',route=>route.fulfill({json:{phase:'failed',error:'The repository runtime could not start. Check the host log, then prepare the saved settings and start again.'}}));
  await page.getByRole('button',{name:'Start Fugue'}).click();
  await expect(page.locator('#launchResult')).toContainText('The repository runtime could not start.');
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeEnabled();
});

for(const width of [390,1024,1440])test(`first-run setup fits ${width}px`,async({page})=>{
  await page.setViewportSize({width,height:900});
  await setup(page);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  await expect(page.getByRole('button',{name:'Save AI selection'})).toBeVisible();
});

test('choosing a native account saves a binding without an AI secret',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('AI authentication',{exact:true}).selectOption('binding');
  await page.getByLabel('rkat account',{exact:true}).selectOption('0');
  await page.getByLabel('Model',{exact:true}).fill('replacement-model');
  await page.getByRole('button',{name:'Save AI selection'}).click();
  await expect(page.locator('#modelResult')).toHaveText('AI account selected.');
  expect(writes[0].value).toEqual({provider:'openai',model:'replacement-model',credential:{method:'binding',auth_binding:{realm:'global',binding:'default_openai'}}});
  await expect(page.locator('#modelKeyFields')).not.toBeVisible();
});

test('changing a prepared repository requires a fresh check before starting',async({page})=>{
  await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/different-application');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await expect(page.locator('#workflowResult')).toContainText('Settings changed');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.getByLabel('Model',{exact:true}).fill('new-model');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeDisabled();
  await page.getByRole('button',{name:'Save AI selection'}).click();
  await expect(page.getByRole('button',{name:'Check and prepare repository'})).toBeEnabled();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
});

test('classic Jira statuses support a review handoff and explicit merge',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByLabel('Issue tracker',{exact:true}).selectOption('jira');
  await page.getByLabel('Jira site',{exact:true}).fill('https://example.atlassian.net');
  await page.getByLabel('Account email',{exact:true}).fill('test@example.com');
  await page.getByLabel('API token',{exact:true}).fill('test-token');
  await page.getByRole('button',{name:'Save tracker access'}).click();
  await page.getByLabel('Tracker project',{exact:true}).fill('EXAMPLE');
  await page.getByLabel('Jira ticket filter',{exact:true}).fill('reporter = currentUser()');
  await page.getByRole('button',{name:'Load project statuses'}).click();
  await expect(page.locator('#jiraStatesResult')).toContainText('Open, In Progress, Resolved, Reopened, Closed');
  await expect(page.getByLabel('Awaiting review',{exact:true})).toHaveValue('Resolved');
  await expect(page.getByLabel('Merge',{exact:true})).toHaveValue('Reopened');
  await page.getByText('Advanced: required Jira transition fields',{exact:true}).click();
  await page.getByLabel('Transition fields (JSON)',{exact:true}).fill('{"Closed":{"resolution":{"name":"Done"}}}');
  await expect(page.getByLabel('Allow merge when GitHub reports no CI checks')).not.toBeChecked();
  await page.getByLabel('Allow merge when GitHub reports no CI checks').check();
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.allow_no_ci_checks).toBe(true);
  expect(writes.at(-1).value.lanes).toEqual({parked:'Open',intake:'Open',active:'In Progress',review_action:'Resolved',deploy_action:'Reopened',blocked:'In Progress',success:'Closed',canceled:'Closed'});
  expect(writes.at(-1).value.jira_transition_fields).toEqual({Closed:{resolution:{name:'Done'}}});
});
test('new setup uses recommended models and preserves explicit selections',async({page})=>{
  await setup(page);
  await expect(page.getByLabel('Model',{exact:true})).toHaveValue('gpt-6-astra');
  await page.getByLabel('Provider',{exact:true}).selectOption('anthropic');
  await expect(page.getByLabel('Model',{exact:true})).toHaveValue('claude-test');
});

test('Jira creation labels survive reload and require preparation after changing',async({page})=>{
  const {writes}=await setup(page,{...connected,tracker:{kind:'jira',site:'https://example.atlassian.net',email:'test@example.com'}});
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByLabel('Tracker project',{exact:true}).fill('EXAMPLE');
  await page.getByLabel('Jira ticket filter',{exact:true}).fill('labels = "fugue-app"');
  await page.getByLabel('Labels for new issues',{exact:true}).fill(' fugue-app, regression ');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.jira_issue_labels).toEqual(['fugue-app','regression']);
  await page.reload();
  await expect(page.getByLabel('Labels for new issues',{exact:true})).toHaveValue('fugue-app, regression');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.getByLabel('Labels for new issues',{exact:true}).fill('replacement');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.jira_issue_labels).toEqual(['replacement']);
  await page.getByLabel('Labels for new issues',{exact:true}).fill('');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.jira_issue_labels).toEqual([]);
  await page.reload();
  await expect(page.getByLabel('Labels for new issues',{exact:true})).toHaveValue('');
});


test('remote repository setup keeps checks and navigation on its own path',async({page})=>{
  const prefix='/repositories/repository-test-a';
  const {writes}=await setup(page,connected,prefix);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue',exact:true})).toBeEnabled();
  await page.getByRole('button',{name:'Start Fugue',exact:true}).click();
  await expect(page).toHaveURL('http://fugue.test'+prefix+'/issues');
  expect(writes.some(write=>write.connection==='start')).toBeTruthy();
});

test('commit identity is explicit, saved per repository and invalidates preparation',async({page})=>{
  const {writes}=await setup(page,connected);
  await page.getByLabel('Repository URL',{exact:true}).fill('https://github.com/example/application');
  await page.getByText('Commit author',{exact:true}).click();
  await expect(page.getByLabel('Commit author name',{exact:true})).toHaveValue('Meerkat Fugue');
  await expect(page.getByLabel('Commit author email',{exact:true})).toHaveValue('fugue@localhost');
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  expect(writes.at(-1).value.commit_identity).toEqual({name:'Meerkat Fugue',email:'fugue@localhost'});
  await page.getByLabel('Commit author name',{exact:true}).fill('Example Automation');
  await page.getByLabel('Commit author email',{exact:true}).fill('automation@example.com');
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeDisabled();
  await page.getByRole('button',{name:'Check and prepare repository'}).click();
  await expect(page.getByRole('button',{name:'Start Fugue'})).toBeEnabled();
  await page.reload();
  await expect(page.getByLabel('Commit author name',{exact:true})).toHaveValue('Example Automation');
  await expect(page.getByLabel('Commit author email',{exact:true})).toHaveValue('automation@example.com');
});
