import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError

spec = importlib.util.spec_from_file_location('fugue_release', Path(__file__).with_name('release.py'))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)

HEAD = 'a' * 40
TREE = 'b' * 40
MERGED = 'c' * 40


def attestation(kind, actor):
    return {'schema': 1, 'kind': kind, 'head': HEAD, 'tree': TREE,
            'actor': actor, 'evidence': 'independently checked behavior'}


class GateTests(unittest.TestCase):
    def test_review_and_test_receipts_bind_exact_tree_and_head(self):
        review, tests = attestation('review', 'reviewer'), attestation('test', 'coder')
        release.validate_attestations(review, tests, HEAD, TREE, 'releaser')
        for key in ['head', 'tree']:
            stale = {**tests, key: 'd' * 40}
            with self.subTest(key=key), self.assertRaisesRegex(release.ReleaseError, 'stale'):
                release.validate_attestations(review, stale, HEAD, TREE, 'releaser')
        with self.assertRaisesRegex(release.ReleaseError, 'independent'):
            release.validate_attestations(review, tests, HEAD, TREE, 'reviewer')

    def test_failed_pending_missing_or_empty_ci_cannot_merge(self):
        for checks, names in [([], ['Rust']), ([], []),
                              ([{'name': 'Rust', 'bucket': 'pending'}], ['Rust']),
                              ([{'name': 'Rust', 'bucket': 'fail'}], ['Rust']),
                              ([{'name': 'Rust', 'bucket': 'pass'}], ['Rust', 'Delivery'])]:
            with self.subTest(checks=checks, names=names), self.assertRaises(release.ReleaseError):
                release.check_ci(checks, names)
        release.check_ci([{'name': 'Rust', 'bucket': 'pass'},
                          {'name': 'Delivery', 'bucket': 'pass'}], ['Rust', 'Delivery'])

    def test_changed_pr_head_is_rejected(self):
        pr = {'head': {'sha': 'd' * 40}, 'base': {'repo': {'full_name': 'owner/repo'}, 'ref': 'main', 'sha': 'e' * 40}, 'draft': False}
        with self.assertRaisesRegex(release.ReleaseError, 'head changed'):
            release.check_pr(pr, 'owner/repo', HEAD)

    def test_dirty_or_different_binary_is_rejected(self):
        receipt = {'merged_commit': MERGED, 'tree': TREE}
        valid = {'commit': MERGED, 'tree': TREE, 'dirty': False}
        release.validate_build(valid, receipt)
        for key, value in [('dirty', True), ('commit', HEAD), ('tree', HEAD)]:
            with self.subTest(key=key), self.assertRaises(release.ReleaseError):
                release.validate_build({**valid, key: value}, receipt)

    def test_merge_uses_expected_sha_and_refuses_different_merged_tree(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            release.write(root / 'review.json', attestation('review', 'reviewer'))
            release.write(root / 'test.json', attestation('test', 'coder'))
            args = argparse.Namespace(repo='owner/repo', head=HEAD, checkout=temp, pr=3, base='main',
                review=root / 'review.json', tests=root / 'test.json', actor='releaser',
                require_check=['Rust'], output=root / 'merge.json')
            pr = {'head': {'sha': HEAD}, 'base': {'repo': {'full_name': 'owner/repo'}, 'ref': 'main', 'sha': 'e' * 40},
                  'draft': False, 'state': 'open', 'merged': False}
            with patch.object(release, 'git', side_effect=['', HEAD, 'https://github.com/owner/repo.git', '', TREE, '', '', '', 'd' * 40]), \
                 patch.object(release, 'run', return_value=json.dumps([{'name': 'Rust', 'bucket': 'pass'}])), \
                 patch.object(release, 'gh', side_effect=[pr, pr, {'merged': True},
                            {**pr, 'merged': True, 'merge_commit_sha': MERGED}]) as remote:
                with self.assertRaisesRegex(release.ReleaseError, 'merged tree differs'):
                    release.merge(args)
                merge_call = remote.call_args_list[2]
                self.assertIn('sha=' + HEAD, merge_call.args)
                self.assertFalse(args.output.exists())

    def test_base_drift_during_ci_check_never_calls_merge(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            release.write(root / 'review.json', attestation('review', 'reviewer'))
            release.write(root / 'test.json', attestation('test', 'coder'))
            args = argparse.Namespace(repo='owner/repo', head=HEAD, checkout=temp, pr=3, base='main',
                review=root / 'review.json', tests=root / 'test.json', actor='releaser',
                require_check=['Rust'], output=root / 'merge.json')
            pr = {'head': {'sha': HEAD}, 'base': {'repo': {'full_name': 'owner/repo'},
                  'ref': 'main', 'sha': 'e' * 40}, 'draft': False, 'state': 'open', 'merged': False}
            drifted = {**pr, 'base': {**pr['base'], 'sha': 'f' * 40}}
            with patch.object(release, 'git', side_effect=['', HEAD, 'https://github.com/owner/repo.git', '', TREE, '', '']), \
                 patch.object(release, 'run', return_value=json.dumps([{'name': 'Rust', 'bucket': 'pass'}])), \
                 patch.object(release, 'gh', side_effect=[pr, drifted]) as remote:
                with self.assertRaisesRegex(release.ReleaseError, 'base/state changed'):
                    release.merge(args)
                self.assertEqual(remote.call_count, 2)
                self.assertFalse(args.output.exists())

    def test_listener_change_is_not_killed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            plan = {'transaction': temp, 'work_item': 'work-test', 'id': 'test',
                    'receipt': {'merged_commit': MERGED, 'tree': TREE},
                    'binary_sha256': 'unused', 'old_pid': 10, 'url': 'http://127.0.0.1:1',
                    'config': {'state_dir': temp, 'cutover_delay_seconds': 0,
                               'notify': {'identity': 'repo:representative'}}}
            with patch.object(release, 'listener_pid', return_value=11), \
                 patch.object(release, 'notify', return_value={'accepted': True}), \
                 patch.object(release.os, 'kill') as kill:
                result = release.supervise(plan)
            kill.assert_not_called()
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['notification'], 'accepted')

    def test_closed_listener_does_not_mean_native_process_has_exited(self):
        with patch.object(release, 'listener_pid', return_value=None), \
             patch.object(release, 'process_alive', return_value=True):
            self.assertFalse(release.old_service_stopped({'url': 'http://127.0.0.1:1', 'old_pid': 12}))

    def test_deployment_lock_loser_gets_correlated_native_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            plan = {'transaction': temp, 'work_item': 'work-rejected', 'id': 'tx-rejected',
                    'receipt': {'merged_commit': MERGED, 'tree': TREE},
                    'binary_sha256': 'unused', 'config': {'state_dir': temp,
                        'notify': {'identity': 'repo:representative'}}}
            with (Path(temp) / 'deploy.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(release, 'notify', return_value={'accepted': True}) as notify, \
                     patch.object(release.os, 'kill') as kill:
                    result = release.supervise(plan)
            kill.assert_not_called()
            notify.assert_called_once()
            self.assertEqual(result['status'], 'rejected')
            self.assertEqual(result['notification'], 'accepted')
            self.assertIn('work-rejected', release.read(Path(temp) / 'notification.json')['content'])

    def test_notification_replay_keeps_exact_payload_after_lost_response(self):
        with tempfile.TemporaryDirectory() as temp:
            plan = {'transaction': temp, 'id': 'tx', 'work_item': 'work-one',
                    'url': 'http://127.0.0.1:1', 'config': {'notify': {
                        'path': '/api/repos/repo/meta/console/send', 'identity': 'repo:representative'}}}
            status = {'status': 'deployed', 'pid': 12}
            release.write(Path(temp) / 'notification.json', release.notification_payload(plan, status))
            accepted = {'identity': 'repo:representative', 'interaction_id': 'id',
                        'input_frame_id': 'frame', 'status': 'accepted'}
            with patch.object(release, 'request', side_effect=[URLError('lost response'), accepted]) as request:
                with self.assertRaises(URLError):
                    release.notify(plan, status)
                status.update(notification='pending', notification_error='timeout')
                release.notify(plan, status)
            self.assertEqual(request.call_args_list[0], request.call_args_list[1])
            with patch.object(release, 'request', return_value={'accepted': True}):
                with self.assertRaisesRegex(release.ReleaseError, 'receipt'):
                    release.notify(plan, status)

    def test_dirty_checkout_is_rejected_before_network_or_merge(self):
        args = argparse.Namespace(repo='owner/repo', head=HEAD, checkout='.')
        with patch.object(release, 'git', return_value=' M src/main.rs'), patch.object(release, 'gh') as remote:
            with self.assertRaisesRegex(release.ReleaseError, 'clean checkout'):
                release.merge(args)
            remote.assert_not_called()

    def test_unexpected_base_is_rejected(self):
        pr = {'head': {'sha': HEAD}, 'base': {'repo': {'full_name': 'owner/repo'}, 'ref': 'other'}, 'draft': False}
        with self.assertRaisesRegex(release.ReleaseError, 'base branch'):
            release.check_pr(pr, 'owner/repo', HEAD)

    def test_missing_memory_or_replaced_session_changes_health_snapshot(self):
        status = {'runtime': {'repositories': [{'repo': 'repo', 'members': [
            {'member_id': 'expert', 'session_id': 'persisted', 'memory_enabled': True}]}]}}
        before = release.session_map(status)
        status['runtime']['repositories'][0]['members'][0]['session_id'] = 'new'
        self.assertNotEqual(before, release.session_map(status))


SERVER = '''#!/usr/bin/env python3
import json, sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
IDENTITY = __IDENTITY__
SERVED_IDENTITY = __SERVED_IDENTITY__
if len(sys.argv) > 1 and sys.argv[1] == 'build-info':
    print(json.dumps(IDENTITY))
    raise SystemExit(0)
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        value = SERVED_IDENTITY if self.path == '/api/v1/build' else {
            'runtime': {'repositories': [{'repo': 'repo', 'members': [
                {'member_id':'expert','session_id':'persisted','memory_enabled':True}]}]}}
        data = json.dumps(value).encode()
        self.send_response(200); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        target = Path(sys.argv[2])
        # First request fails after recording its idempotency key. A retry must
        # use the same key and is accepted exactly once by native-style ingress.
        history = json.loads(target.read_text()) if target.exists() else []
        history.append(value)
        target.write_text(json.dumps(history))
        self.send_response(503 if len(history) == 1 else 202)
        self.end_headers()
        self.wfile.write(json.dumps({'identity': value['identity'], 'interaction_id': 'interaction',
            'input_frame_id': 'frame', 'status': 'accepted'}).encode())
HTTPServer.allow_reuse_address = True
HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
'''


@unittest.skipUnless(shutil.which('lsof'), 'local deployment requires lsof')
class DetachedDeploymentTests(unittest.TestCase):
    def exercise(self, wrong_identity):
        import socket
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            # Ask the OS for a currently free port. This test never contacts an
            # operator service and only terminates the child it started.
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            url = f'http://127.0.0.1:{port}'
            identity = {'commit': MERGED, 'tree': TREE, 'dirty': False, 'version': 'test'}
            old = root / 'old'
            new = root / 'new'
            for path, served in [(old, identity), (new, {**identity, 'commit': HEAD} if wrong_identity else identity)]:
                path.write_text(SERVER.replace('__IDENTITY__', repr(identity)).replace('__SERVED_IDENTITY__', repr(served)))
                path.chmod(0o755)
            callback = root / 'callback.json'
            old_process = subprocess.Popen([str(old), str(port), str(callback)],
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deployment_pid = None
            try:
                release.wait_for(lambda: release.request(url + '/api/ensemble'), 10, 'test service')
                release.write(root / 'receipt.json', {'schema': 1, 'status': 'merged',
                    'repository': 'owner/repo', 'merged_commit': MERGED, 'tree': TREE})
                release.write(root / 'service.json', {'repository': 'owner/repo', 'url': url,
                    'cwd': temp, 'args': [str(port), str(callback)], 'state_dir': str(root / 'state'),
                    # One lsof call can exceed two seconds on macOS. Allow a
                    # second probe if the first races with listener startup.
                    'initial_binary': str(old), 'health_timeout_seconds': 10, 'notify_timeout_seconds': 5,
                    'cutover_delay_seconds': 1, 'notify': {'path': '/api/repos/repo/meta/console/send',
                                                         'identity': 'repo:representative'}})
                # The launcher exits before cutover. Child stdout/stderr cannot
                # retain its pipes, and the supervisor must survive independently.
                result = subprocess.run([sys.executable, str(Path(release.__file__).resolve()), 'deploy',
                    '--receipt', str(root / 'receipt.json'), '--service', str(root / 'service.json'),
                    '--binary', str(new), '--work-item', 'work-test'], text=True,
                    capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                receipt = json.loads(result.stdout)
                self.assertEqual(os.getpgid(receipt['supervisor_pid']), receipt['supervisor_pid'])
                status_path = Path(receipt['status_file'])
                def finished():
                    status = release.read(status_path)
                    return status if status.get('notification') == 'accepted' else None
                final = release.wait_for(finished, 45, 'detached deploy + callback')
                self.assertEqual(final['status'], 'rolled_back' if wrong_identity else 'deployed',
                                 json.dumps(final, indent=2))
                deployment_pid = final['pid']
                self.assertEqual(final['preserved_sessions'], 1)
                self.assertEqual(os.getpgid(deployment_pid), deployment_pid)
                payloads = release.read(callback)
                self.assertGreaterEqual(len(payloads), 2)
                self.assertEqual(len({p['idempotency_key'] for p in payloads}), 1)
                self.assertEqual(payloads[-1]['identity'], 'repo:representative')
                self.assertIn('work-test', payloads[-1]['content'])
                self.assertEqual(release.request(url + '/api/v1/build'), identity)
            finally:
                if deployment_pid is None:
                    deployment_pid = release.listener_pid(url)
                if deployment_pid is not None:
                    try:
                        os.kill(deployment_pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                if old_process.poll() is None:
                    old_process.terminate()
                old_process.wait(timeout=5)

    def test_detached_cutover_survives_launcher_and_retries_native_callback(self):
        self.exercise(wrong_identity=False)

    def test_wrong_served_build_rolls_back_and_reports_to_native_agent(self):
        self.exercise(wrong_identity=True)


if __name__ == '__main__':
    unittest.main()
