import {browserDocument} from './browser-document.mjs';
import {test, expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

const html = (await readFile(new URL('../src/setup/setup.html',import.meta.url),'utf8'))
  .replaceAll('__DEFAULT_MODEL__','gpt-6-astra').replaceAll('__OPENAI_MODEL__','gpt-6-astra').replaceAll('__ANTHROPIC_MODEL__','claude-test').replaceAll('__GEMINI_MODEL__','gemini-test')
  .replace('__APP_CSS__',await readFile(new URL('../src/ui/app.css',import.meta.url),'utf8'));

async function setup(page, initial={}, prefix='') {
  const state={model:null,github:null,tracker:null,environment_providers:['openai'],...initial};
  const writes=[];
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
    if(url.pathname==='/api/setup/workflow') return route.fulfill({body:'null',contentType:'application/json'});
    if(url.pathname==='/api/setup/preparation') {
      if(route.request().method()==='POST') {writes.push({connection:'workflow',value:route.request().postDataJSON()});preparation={phase:'ready'};}
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
