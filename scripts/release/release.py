#!/usr/bin/env python3
"""Pinned merge and supervised self-deployment. No external Python dependencies."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import uuid


class ReleaseError(RuntimeError):
    pass


def run(*args, cwd=None):
    result = subprocess.run(args, cwd=cwd, check=False, text=True, capture_output=True)
    if result.returncode:
        raise ReleaseError(f'{args[0]} failed ({result.returncode}): {result.stderr.strip()}')
    return result.stdout.strip()


def read(path):
    with open(path) as source:
        return json.load(source)


def write(path, value):
    path = Path(path)
    temporary = path.with_suffix(f'.{os.getpid()}.tmp')
    with temporary.open('w') as target:
        json.dump(value, target, indent=2, sort_keys=True)
        target.write('\n')
        target.flush()
        os.fsync(target.fileno())
    temporary.replace(path)


def sha(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{40}', value):
        raise ReleaseError('expected a full lowercase 40-character Git SHA')
    return value


def checked_repo(value):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', value):
        raise ReleaseError('repository must be owner/name')
    return value


def git(cwd, *args):
    return run('git', *args, cwd=cwd)


def gh(repo, endpoint, *args):
    return json.loads(run('gh', 'api', f'repos/{repo}/{endpoint}', *args))


def attest(args):
    """Record an agent's evidence for the exact clean checkout it inspected."""
    cwd = str(Path(args.checkout).resolve())
    if git(cwd, 'status', '--porcelain', '--untracked-files=normal'):
        raise ReleaseError('attestation requires a clean checkout, including untracked files')
    head = sha(git(cwd, 'rev-parse', 'HEAD'))
    if head != sha(args.head):
        raise ReleaseError('checkout HEAD differs from the inspected head')
    if not args.evidence.strip() or not args.actor.strip():
        raise ReleaseError('actor and concrete evidence are required')
    value = {'schema': 1, 'kind': args.kind, 'head': head,
             'tree': sha(git(cwd, 'rev-parse', 'HEAD^{tree}')),
             'actor': args.actor, 'evidence': args.evidence}
    write(args.output, value)
    return value


def validate_attestations(review, tests, head, tree, release_actor):
    for kind, record in [('review', review), ('test', tests)]:
        if record.get('schema') != 1 or record.get('kind') != kind:
            raise ReleaseError(f'invalid {kind} attestation')
        if record.get('head') != head or record.get('tree') != tree:
            raise ReleaseError(f'{kind} attestation is stale: head/tree mismatch')
        if not record.get('actor') or not record.get('evidence'):
            raise ReleaseError(f'{kind} attestation is missing actor/evidence')
    if review['actor'] in {release_actor, tests['actor']}:
        raise ReleaseError('reviewer must be independent of release/test owner')


def check_pr(pr, repo, head, base='main'):
    if pr['head']['sha'] != head:
        raise ReleaseError('PR head changed; obtain new exact-head review and tests')
    if pr['base']['repo']['full_name'].lower() != repo.lower():
        raise ReleaseError('PR repository mismatch')
    if pr['base']['ref'] != base:
        raise ReleaseError('PR targets an unexpected base branch')
    if pr['draft']:
        raise ReleaseError('draft PR cannot be released')


def check_ci(checks, required):
    if not required:
        raise ReleaseError('at least one explicitly required CI check is necessary')
    for name in required:
        matching = [item for item in checks if item['name'] == name]
        if not matching or any(item['bucket'] != 'pass' for item in matching):
            raise ReleaseError(f'required CI check is absent or not successful: {name}')
    if any(item['bucket'] in {'fail', 'pending', 'cancel'} for item in checks):
        raise ReleaseError('PR has failed, cancelled, or pending checks')


def merge(args):
    repo = checked_repo(args.repo)
    head = sha(args.head)
    cwd = str(Path(args.checkout).resolve())
    if git(cwd, 'status', '--porcelain', '--untracked-files=normal'):
        raise ReleaseError('merge requires a clean checkout; local fixes must be committed and reviewed')
    if git(cwd, 'rev-parse', 'HEAD') != head:
        raise ReleaseError('checkout HEAD differs from reviewed PR head')
    origin = git(cwd, 'remote', 'get-url', 'origin').removesuffix('.git')
    if origin not in {f'https://github.com/{repo}', f'git@github.com:{repo}', f'ssh://git@github.com/{repo}'}:
        raise ReleaseError('checkout origin differs from the selected repository')
    # Fetch the selected public repository explicitly; never trust a stale origin.
    git(cwd, 'fetch', '--no-tags', f'https://github.com/{repo}.git', head)
    tree = sha(git(cwd, 'rev-parse', f'{head}^{{tree}}'))
    review, tests = read(args.review), read(args.tests)
    validate_attestations(review, tests, head, tree, args.actor)
    pr = gh(repo, f'pulls/{args.pr}')
    check_pr(pr, repo, head, args.base)
    if not pr.get('merged'):
        base_sha = sha(pr['base']['sha'])
        git(cwd, 'fetch', '--no-tags', f'https://github.com/{repo}.git', base_sha)
        git(cwd, 'merge-base', '--is-ancestor', base_sha, head)
        if pr['state'] != 'open':
            raise ReleaseError('PR is closed without merge')
        checks = json.loads(run('gh', 'pr', 'checks', str(args.pr), '--repo', repo,
                                '--json', 'name,bucket,state,link'))
        check_ci(checks, args.require_check)
        latest = gh(repo, f'pulls/{args.pr}')
        check_pr(latest, repo, head, args.base)
        if latest['base']['sha'] != base_sha or latest['state'] != 'open':
            raise ReleaseError('PR base/state changed while checking CI; rebase and re-attest')
        # GitHub performs the final compare-and-swap. A push after the checks
        # cannot cause a different head to be merged.
        response = gh(repo, f'pulls/{args.pr}/merge', '--method', 'PUT',
                      '-f', f'sha={head}', '-f', 'merge_method=squash')
        if not response.get('merged'):
            raise ReleaseError(f'GitHub did not merge PR: {response.get("message")}')
        pr = gh(repo, f'pulls/{args.pr}')
    else:
        # Retrying a merge after a lost response must not manufacture CI evidence.
        checks = json.loads(run('gh', 'pr', 'checks', str(args.pr), '--repo', repo,
                                '--json', 'name,bucket,state,link'))
        check_ci(checks, args.require_check)
    check_pr(pr, repo, head, args.base)
    merged = sha(pr['merge_commit_sha'])
    git(cwd, 'fetch', '--no-tags', f'https://github.com/{repo}.git', merged)
    merged_tree = sha(git(cwd, 'rev-parse', f'{merged}^{{tree}}'))
    if merged_tree != tree:
        raise ReleaseError('merged tree differs from reviewed tree; do not deploy')
    value = {'schema': 1, 'status': 'merged', 'repository': repo, 'pr': args.pr,
             'head': head, 'tree': tree, 'merged_commit': merged, 'checks': checks,
             'review': review, 'tests': tests, 'release_actor': args.actor}
    write(args.output, value)
    return value


def local_url(value):
    url = urlparse(value)
    if (url.scheme != 'http' or url.hostname not in {'127.0.0.1', 'localhost', '::1'}
            or url.username or url.password or url.query or url.fragment):
        raise ReleaseError('service URL must be plain HTTP on loopback without credentials/query')
    return value.rstrip('/')


def request(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=5) as response:
        return json.load(response)


def session_map(status):
    result = {}
    for repo in status['runtime']['repositories']:
        for member in repo['members']:
            key = f'{repo["repo"]}:{member["member_id"]}'
            if key in result or not member.get('session_id'):
                raise ReleaseError('duplicate/missing native session identity')
            result[key] = {'session_id': member['session_id'],
                           'memory_enabled': member.get('memory_enabled', False)}
    if not result:
        raise ReleaseError('Ensemble has no native sessions')
    return result


def listener_pid(url):
    port = urlparse(url).port or 80
    result = subprocess.run(['lsof', '-nP', f'-iTCP:{port}', '-sTCP:LISTEN', '-t'],
                            text=True, capture_output=True)
    if result.returncode not in {0, 1}:
        raise ReleaseError(f'lsof failed: {result.stderr.strip()}')
    pids = set(result.stdout.split())
    if len(pids) > 1:
        raise ReleaseError('multiple listener processes; refusing an ambiguous cutover')
    return int(next(iter(pids))) if pids else None


def process_alive(pid):
    result = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], text=True, capture_output=True)
    if result.returncode == 1 and not result.stdout.strip():
        return False
    if result.returncode:
        raise ReleaseError(f'cannot inspect process {pid}: {result.stderr.strip()}')
    state = result.stdout.strip()
    return bool(state) and not state.startswith('Z')


def old_service_stopped(plan):
    return listener_pid(plan['url']) is None and not process_alive(plan['old_pid'])


def digest(path):
    result = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def build_info(binary):
    return json.loads(run(str(binary), 'build-info'))


def validate_build(info, receipt):
    if (info.get('dirty') is not False or info.get('commit') != receipt['merged_commit']
            or info.get('tree') != receipt['tree']):
        raise ReleaseError('binary must be a clean build of the exact merged commit and reviewed tree')


def prepare_deploy(args):
    receipt = read(args.receipt)
    if receipt.get('schema') != 1 or receipt.get('status') != 'merged':
        raise ReleaseError('deploy requires a successful merge receipt')
    config = read(args.service)
    url = local_url(config['url'])
    notify = config['notify']
    if (not notify['path'].startswith('/api/repos/')
            or not notify['path'].endswith('/meta/console/send') or '?' in notify['path']):
        raise ReleaseError('notify.path must address a native repository console send endpoint')
    if not notify['identity'] or not args.work_item:
        raise ReleaseError('native completion identity and work-item ID are required')
    if checked_repo(config['repository']) != receipt['repository']:
        raise ReleaseError('service and merge receipt repositories differ')
    root = Path(config['state_dir']).resolve()
    root.mkdir(parents=True, exist_ok=True)
    # This lock lives in the supervisor for the whole cutover, including callback.
    transaction = root / str(uuid.uuid4())
    transaction.mkdir(mode=0o700)
    binary = Path(args.binary).resolve()
    validate_build(build_info(binary), receipt)
    current = root / 'current.json'
    if current.exists():
        previous = Path(read(current)['binary'])
    else:
        previous = Path(config['initial_binary']).resolve()
    if not previous.is_file():
        raise ReleaseError('no existing rollback executable')
    try:
        running_identity = request(url + '/api/v1/build')
    except HTTPError as error:
        if error.code != 404:
            raise
        running_identity = None  # Bootstrap from a pre-build-identity release.
    if running_identity is not None and build_info(previous) != running_identity:
        raise ReleaseError('rollback executable does not match the running service build')
    previous_hash = digest(previous)
    candidate = transaction / 'meerkat-fugue'
    rollback = transaction / 'rollback'
    shutil.copy2(binary, candidate)
    shutil.copy2(previous, rollback)
    if digest(rollback) != previous_hash:
        raise ReleaseError('rollback binary changed while staging')
    validate_build(build_info(candidate), receipt)
    baseline = session_map(request(url + '/api/ensemble'))
    if any(not member['memory_enabled'] for member in baseline.values()):
        raise ReleaseError('baseline contains a member without persistent memory')
    pid = listener_pid(url)
    if pid is None:
        raise ReleaseError('no running service to replace')
    # Snapshot the supervisor itself: a concurrent checkout/update cannot change
    # the code that owns this deployment once the originating host exits.
    supervisor = transaction / 'release.py'
    shutil.copy2(__file__, supervisor)
    plan = {'schema': 1, 'id': transaction.name, 'config': config, 'receipt': receipt,
            'binary': str(candidate), 'binary_sha256': digest(candidate),
            'rollback_binary': str(rollback), 'rollback_sha256': previous_hash,
            'baseline': baseline, 'old_pid': pid, 'work_item': args.work_item,
            'url': url, 'transaction': str(transaction)}
    write(transaction / 'plan.json', plan)
    write(transaction / 'status.json', {'status': 'scheduled', 'transaction': str(transaction)})
    with (transaction / 'supervisor.log').open('ab', buffering=0) as log:
        child = subprocess.Popen([sys.executable, str(supervisor), '_supervise',
                                  str(transaction / 'plan.json')], cwd=transaction,
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True, close_fds=True)
    return {'status': 'scheduled', 'supervisor_pid': child.pid,
            'transaction': str(transaction), 'status_file': str(transaction / 'status.json')}


def wait_for(predicate, seconds, label):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, KeyError, ReleaseError, URLError) as error:
            last = str(error)
        time.sleep(0.2)
    raise ReleaseError(f'timeout waiting for {label}: {last}')


def launch(plan, binary, log_name):
    with (Path(plan['transaction']) / log_name).open('ab', buffering=0) as log:
        return subprocess.Popen([binary, *plan['config']['args']], cwd=plan['config']['cwd'],
                                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True, close_fds=True)


def health(plan, process, identity=None):
    if process.poll() is not None:
        raise ReleaseError(f'service exited {process.returncode}')
    if listener_pid(plan['url']) != process.pid:
        return False
    if identity is not None and request(plan['url'] + '/api/v1/build') != identity:
        return False
    if session_map(request(plan['url'] + '/api/ensemble')) != plan['baseline']:
        return False
    return True


def notification_payload(plan, status):
    config = plan['config']['notify']
    message = (f'Durable deployment {plan["id"]} finished with status {status["status"]}. '
               f'Continue and close the native WorkGraph delivery for {plan["work_item"]} '
               'using this machine receipt and independent post-deploy verification. '
               'Do not wait for another human message. '
               f'Receipt: {json.dumps(status, sort_keys=True)}')
    return {
        'identity': config['identity'], 'content': message, 'origin': 'console',
        'handling_mode': 'queue', 'idempotency_key': f'release-{plan["id"]}-{status["status"]}'}


def notify(plan, status):
    # Never rebuild content from mutable notification attempts/results: native
    # ingress binds its idempotency key to the exact payload fingerprint.
    payload = read(Path(plan['transaction']) / 'notification.json')
    response = request(plan['url'] + plan['config']['notify']['path'], payload)
    if (response.get('identity') != payload['identity']
            or not response.get('interaction_id') or not response.get('input_frame_id')
            or response.get('status') not in {'accepted', 'dispatching', 'delivered', 'completed'}):
        raise ReleaseError('native console did not return an accepted interaction receipt')
    return response


def supervise(plan):
    directory = Path(plan['transaction'])
    status_path = directory / 'status.json'
    state_root = Path(plan['config']['state_dir']).resolve()
    status = {'status': 'starting', 'transaction': str(directory),
              'work_item': plan['work_item'], 'merged_commit': plan['receipt']['merged_commit'],
              'tree': plan['receipt']['tree'], 'binary_sha256': plan['binary_sha256']}
    with (state_root / 'deploy.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            status.update(status='rejected', error='another deploy owns the service')
            return finish_notification(plan, status)
        write(status_path, status)
        process = None
        stopped = False
        try:
            # Parent tool may still be returning the accepted receipt. This delay
            # is bounded; the durable plan already exists and no pipe ties us to it.
            time.sleep(plan['config'].get('cutover_delay_seconds', 3))
            if listener_pid(plan['url']) != plan['old_pid']:
                raise ReleaseError('listener changed after staging; refusing to kill it')
            if digest(plan['binary']) != plan['binary_sha256'] or digest(plan['rollback_binary']) != plan['rollback_sha256']:
                raise ReleaseError('staged executable changed')
            identity = build_info(plan['binary'])
            validate_build(identity, plan['receipt'])
            os.kill(plan['old_pid'], signal.SIGTERM)
            stopped = True
            wait_for(lambda: old_service_stopped(plan), 30, 'old service process and listener shutdown')
            process = launch(plan, plan['binary'], 'service.log')
            wait_for(lambda: health(plan, process, identity),
                     plan['config'].get('health_timeout_seconds', 120), 'candidate health')
            # Recheck after a short settling interval, avoiding instant-200 false positives.
            time.sleep(1)
            if not health(plan, process, identity):
                raise ReleaseError('candidate failed settling health check')
            status.update(status='deployed', pid=process.pid, build=identity,
                          preserved_sessions=len(plan['baseline']))
            write(state_root / 'current.json', {'binary': plan['binary'], 'pid': process.pid,
                                               'build': identity, 'transaction': plan['id']})
        except Exception as error:
            status.update(status='failed', error=str(error))
            if stopped:
                try:
                    if process is not None and process.poll() is None:
                        process.terminate()
                        process.wait(timeout=15)
                    wait_for(lambda: listener_pid(plan['url']) is None, 15, 'failed candidate shutdown')
                    rollback = launch(plan, plan['rollback_binary'], 'rollback.log')
                    wait_for(lambda: health(plan, rollback),
                             plan['config'].get('health_timeout_seconds', 120), 'rollback health')
                    status.update(status='rolled_back', pid=rollback.pid,
                                  preserved_sessions=len(plan['baseline']))
                    write(state_root / 'current.json', {'binary': plan['rollback_binary'],
                                                       'pid': rollback.pid, 'transaction': plan['id']})
                except Exception as rollback_error:
                    status['rollback_error'] = str(rollback_error)
        return finish_notification(plan, status)


def finish_notification(plan, status):
    directory = Path(plan['transaction'])
    status_path = directory / 'status.json'
    # Every scheduled transaction gets a correlated terminal notification,
    # including a loser of the per-service deployment lock.
    write(directory / 'notification.json', notification_payload(plan, status))
    status['notification'] = 'pending'
    write(status_path, status)
    try:
        status['notification_receipt'] = wait_for(lambda: notify(plan, status),
            plan['config'].get('notify_timeout_seconds', 180), 'native completion callback')
        status['notification'] = 'accepted'
    except Exception as error:
        status['notification_error'] = str(error)
    write(status_path, status)
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('attest', help='record exact-head review or test evidence')
    p.add_argument('--checkout', required=True)
    p.add_argument('--head', required=True)
    p.add_argument('--kind', choices=['review', 'test'], required=True)
    p.add_argument('--actor', required=True)
    p.add_argument('--evidence', required=True)
    p.add_argument('--output', required=True)
    p = sub.add_parser('merge', help='merge only the independently reviewed, tested, green head')
    for name in ['checkout', 'repo', 'head', 'actor', 'review', 'tests', 'output']:
        p.add_argument('--' + name, required=True)
    p.add_argument('--pr', type=int, required=True)
    p.add_argument('--base', default='main')
    p.add_argument('--require-check', action='append', required=True)
    p = sub.add_parser('deploy', help='stage immutable binaries and detach a cutover supervisor')
    for name in ['receipt', 'service', 'binary', 'work-item']:
        p.add_argument('--' + name, required=True)
    p = sub.add_parser('_supervise', help='internal: execute an already staged deployment')
    p.add_argument('plan')
    p = sub.add_parser('resume-notification', help='retry an idempotent completion notification')
    p.add_argument('transaction')
    args = parser.parse_args()
    try:
        if args.command == 'attest':
            result = attest(args)
        elif args.command == 'merge':
            result = merge(args)
        elif args.command == 'deploy':
            result = prepare_deploy(args)
        elif args.command == '_supervise':
            result = supervise(read(args.plan))
        else:
            directory = Path(args.transaction)
            result = read(directory / 'status.json')
            result['notification_receipt'] = notify(read(directory / 'plan.json'), result)
            result['notification'] = 'accepted'
            write(directory / 'status.json', result)
        print(json.dumps(result, sort_keys=True))
    except (ReleaseError, OSError, KeyError, ValueError) as error:
        print(json.dumps({'status': 'rejected', 'error': str(error)}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
