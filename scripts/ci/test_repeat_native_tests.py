import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name('repeat-native-tests.py')
spec = importlib.util.spec_from_file_location('native_trials', SCRIPT)
trials = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trials)

FIXTURE = '''import json, os, signal, subprocess, sys, time
from pathlib import Path
selection = sys.argv[1]
hang = os.environ.get('FIXTURE_HANG', '')
if (hang == 'list' and '--list' in sys.argv) or (hang == 'retirement' and '--list' not in sys.argv and 'runtime_members' in selection):
    child_code = 'import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print("ready", flush=True); time.sleep(120)' if os.environ.get('FIXTURE_IGNORE_TERM') else 'import time; print("ready", flush=True); time.sleep(120)'
    child = subprocess.Popen([sys.executable, '-c', child_code], stdout=subprocess.PIPE)
    child.stdout.readline()
    child.stdout.close()
    if os.environ.get('FIXTURE_IGNORE_TERM'):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(os.environ['FIXTURE_PIDS'], str(os.getpid()) + '.json').write_text(json.dumps([os.getpid(), child.pid]))
    print('archive in flight', flush=True)
    time.sleep(120)
elif '--list' in sys.argv:
    if 'runtime_members' in selection:
        print(selection + ': test')
    else:
        print('ensemble::runtime::tests::native_flow_delivers_worker_final_without_peer_reply: test')
        print('ensemble::runtime::tests::native_flow_delivers_worker_failure_without_peer_reply: test')
elif 'runtime_members' in selection:
    assert os.environ['FUGUE_ISOLATED_RUNTIME_TEST'] == 'runtime_members_emit_canonical_structural_events'
    print(os.environ.get('FIXTURE_OUTPUT', 'test result: ok. 1 passed;'), flush=True)
else:
    print('test result: ok. 2 passed;', flush=True)
'''


def await_condition(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def running(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # An orphan can remain briefly as a zombie until the host reaps it.
    status = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True,
                            text=True, timeout=2)
    return bool(status.stdout.strip()) and not status.stdout.strip().startswith('Z')


@unittest.skipUnless(os.name == 'posix', 'CI process group ownership requires POSIX')
class NativeTrialEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.executable = self.root / 'fixture'
        self.executable.write_text(f'#!{sys.executable}\n' + FIXTURE)
        self.executable.chmod(0o700)
        self.pids = self.root / 'pids'
        self.pids.mkdir()
        self.env = patch.dict(os.environ, {'FIXTURE_PIDS': str(self.pids), 'FIXTURE_HANG': ''})
        self.env.start()

    def tearDown(self):
        self.clean_fixtures()
        self.env.stop()
        self.temp.cleanup()

    def fixture_pids(self):
        return [pid for path in self.pids.glob('*.json') for pid in json.loads(path.read_text())]

    def clean_fixtures(self):
        for pid in self.fixture_pids():
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for path in self.pids.glob('*.json'):
            path.unlink()

    def test_zero_exit_does_not_hide_conflicts_or_empty_test_selection(self):
        for index, (output, qualified) in enumerate([
            ('test result: ok. 1 passed;', True),
            ('test result: ok. 0 passed;', False),
            ('failed to realize committed handoffs\ntest result: ok. 1 passed;', False),
            ('rewrite rejected: previous transcript revision\ntest result: ok. 1 passed;', False),
        ], 1):
            with self.subTest(output=output), patch.dict(os.environ, FIXTURE_OUTPUT=output):
                row = trials.qualify_retirement(str(self.executable), self.root, index)
                self.assertEqual(row['qualified'], qualified)
                self.assertEqual((self.root / f'attempt-{index:02}.log').read_text(), output + '\n')
                self.assertEqual(json.loads((self.root / f'attempt-{index:02}.json').read_text()), row)

    def test_timeout_retains_live_output_and_stops_descendants(self):
        with patch.dict(os.environ, FIXTURE_HANG='retirement'):
            row = trials.qualify_retirement(str(self.executable), self.root, 1, timeout=0.3)
        self.assertFalse(row['qualified'])
        self.assertTrue(row['timeout'])
        self.assertEqual((self.root / 'attempt-01.log').read_text(), 'archive in flight\n')
        for pid in self.fixture_pids():
            self.assertTrue(await_condition(lambda: not running(pid)), f'process {pid} survived')

    def test_timeout_escalates_when_descendants_ignore_termination(self):
        with patch.dict(os.environ, FIXTURE_HANG='retirement', FIXTURE_IGNORE_TERM='1'):
            started = time.monotonic()
            row = trials.qualify_retirement(str(self.executable), self.root, 1, timeout=0.3)
        self.assertLess(time.monotonic() - started, 8)
        self.assertFalse(row['qualified'])
        self.assertTrue(row['timeout'])
        self.assertEqual(row['exit'], -signal.SIGKILL)
        for pid in self.fixture_pids():
            self.assertTrue(await_condition(lambda: not running(pid)), f'process {pid} survived')

    def test_complete_harness_retains_every_trial_and_executable_identity(self):
        report = self.root / 'complete'
        manifest = self.root / 'cargo.jsonl'
        manifest.write_text(json.dumps({'reason': 'compiler-artifact',
            'target': {'name': 'meerkat_fugue'}, 'profile': {'test': True},
            'executable': str(self.executable)}) + '\n')
        result = subprocess.run([sys.executable, str(SCRIPT), str(manifest), str(report)],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads((report / 'summary.json').read_text())
        self.assertTrue(summary['qualified'])
        self.assertEqual(summary['status'], 'completed')
        self.assertEqual(len(summary['executable_sha256']), 64)
        self.assertEqual(len(summary['trials']), 20)
        self.assertEqual(len(summary['phases']), 7)
        self.assertEqual(len(list(report.glob('*.log'))), 27)

    def test_outer_cancellation_retains_inflight_logs_and_stops_descendants(self):
        for phase in ('list', 'retirement'):
            with self.subTest(phase=phase):
                self.clean_fixtures()
                report = self.root / f'report-{phase}'
                manifest = self.root / 'cargo.jsonl'
                manifest.write_text(json.dumps({'reason': 'compiler-artifact',
                    'target': {'name': 'meerkat_fugue'}, 'profile': {'test': True},
                    'executable': str(self.executable)}) + '\n')
                with patch.dict(os.environ, FIXTURE_HANG=phase), (self.root / 'outer.log').open('w') as output:
                    process = subprocess.Popen([sys.executable, str(SCRIPT), str(manifest), str(report)],
                                               stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    self.assertTrue(await_condition(lambda: any(self.pids.glob('*.json'))), 'fixture did not start')
                    self.assertTrue(await_condition(lambda: any('archive in flight' in p.read_text()
                        for p in report.glob('*.log'))), 'inflight output was buffered')
                    rows = [json.loads(p.read_text()) for p in report.glob('*.json')]
                    self.assertTrue(any(row.get('status') == 'running' for row in rows))
                    self.assertTrue(all(not row.get('qualified', False) for row in rows if row.get('status') == 'running'))
                    process.terminate()
                    self.assertNotEqual(process.wait(timeout=10), 0)
                    self.assertLessEqual(len(self.fixture_pids()), 8, 'cancelled queued trials were launched')
                    for pid in self.fixture_pids():
                        self.assertTrue(await_condition(lambda: not running(pid)), f'process {pid} survived')
                    summary = json.loads((report / 'summary.json').read_text())
                    self.assertFalse(summary['qualified'])
                    self.assertEqual(summary['status'], 'cancelled')
                    self.assertTrue(any(json.loads(p.read_text()).get('cancelled')
                                        for p in report.glob('*.json')))
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=5)
                    self.clean_fixtures()


if __name__ == '__main__':
    unittest.main()
