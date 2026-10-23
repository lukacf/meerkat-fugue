// A browser preference and a short explanation, never an operational state machine.
(() => {
  const dialog = document.getElementById('fugueWelcome');
  if (!dialog) return;
  const key = 'meerkat-fugue.welcome.v1';
  const node = id => document.getElementById(id);
  const steps = [
    {stage:'CONNECT', eyebrow:'Start with one repository', title:'Connect your tools.', description:'Add a GitHub or GitHub Enterprise repository, choose its issue tracker, then connect your AI provider through Meerkat auth.', tip:'Each repository has its own tracker, board and team. Manage connections in Repositories.'},
    {stage:'PREPARE', eyebrow:'Prepare once, build understanding over time', title:'Prepare your team.', description:'Run repository preparation. Fugue maps the code and creates domain specialists alongside the coordinator, coder and reviewer in one team.', tip:'Check the repository map and specialist roster before starting work. This is setup, separate from the daily board.'},
    {stage:'WORK', eyebrow:'Make the outcome clear', title:'Give it an issue.', description:'Write an issue with a clear goal and acceptance criteria, then move it into the configured ready lane. Follow its progress on Board.', tip:'Open Agents to talk with the team and inspect its conversations, memory and work graph. Use the repository selector to return to its board.'},
    {stage:'REVIEW', eyebrow:'Keep the decision grounded in evidence', title:'Review, then merge.', description:'Follow the issue to its pull request. Inspect the change, test results and independent review, then approve the next step when the board asks for you.', tip:'Your configured review and CI policy gates merging. A merged PR completes the code change; application deployment is a separate step.'},
  ];
  let step = 0;
  let returnFocus;
  function remember() {
    try { localStorage.setItem(key, 'seen'); } catch { /* The guide still works when browser storage is disabled. */ }
  }
  function dismiss() {
    // close is queued by the browser; a fast reload can discard its handler.
    remember();
    dialog.close();
  }
  function render() {
    const current = steps[step];
    node('welcomeStage').textContent = `0${step + 1} / ${current.stage}`;
    for (const [id, field] of [['welcomeEyebrow','eyebrow'],['welcomeTitle','title'],['welcomeDescription','description'],['welcomeTip','tip']]) node(id).textContent = current[field];
    dialog.querySelectorAll('[data-welcome-step]').forEach((scene,index) => scene.toggleAttribute('hidden', index !== step));
    dialog.querySelectorAll('.welcome-progress > span:not([id])').forEach((dot,index) => dot.classList.toggle('current',index === step));
    node('welcomePosition').textContent = `${step + 1} of ${steps.length}`;
    node('welcomeBack').hidden = step === 0;
    node('welcomeAdd').hidden = step !== steps.length - 1;
    node('welcomeNext').textContent = step === steps.length - 1 ? 'Done' : 'Next';
  }
  function open(trigger) {
    if (dialog.open) return;
    returnFocus = trigger || document.querySelector('main [data-fugue-welcome]');
    step = 0;
    render();
    document.querySelector('.fugue-account')?.removeAttribute('open');
    dialog.showModal();
  }
  document.querySelectorAll('[data-fugue-welcome]').forEach(button => button.addEventListener('click', () => open(button)));
  node('welcomeClose').addEventListener('click', dismiss);
  dialog.addEventListener('cancel', remember);
  dialog.addEventListener('close', () => {
    // A launch button inside a closed Session menu cannot receive focus.
    const target = returnFocus?.closest('.fugue-account')?.querySelector('summary') || returnFocus;
    target?.focus();
  });
  dialog.addEventListener('keydown', event => {
    if (event.key !== 'Tab') return;
    const first = node('welcomeClose'), last = node('welcomeNext');
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  });
  node('welcomeNext').addEventListener('click', () => {
    if (step === steps.length - 1) dismiss();
    else { step += 1; render(); }
  });
  node('welcomeBack').addEventListener('click', () => {
    step = Math.max(0, step - 1); render();
    if (step === 0) node('welcomeNext').focus();
  });
  node('welcomeAdd').addEventListener('click', remember);
  node('welcomeReplay').addEventListener('click', () => {
    const scene = dialog.querySelector('[data-welcome-step]:not([hidden])');
    for (const animation of scene.getAnimations({subtree:true})) { animation.cancel(); animation.play(); }
  });
  if (document.querySelector('[data-area="home"]')) {
    // Do not repeatedly interrupt people whose browser disallows persistence.
    try { if (localStorage.getItem(key) !== 'seen') open(); } catch { /* Manual launch remains available. */ }
  }
})();
