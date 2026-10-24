"""Exercise publication checks against disposable repositories and tool errors."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name('scan.sh').resolve()
GIT = shutil.which('git')
TOKEN = 'ghp_' + 'a' * 36
PASSED = 'configured public readiness checks passed'


class PublicationScanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='publication-scan-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir()
        self.env = dict(os.environ)
        self.env.pop('FUGUE_PRIVATE_INFRA_PATTERN', None)
        self.env.update(GIT_AUTHOR_NAME='Fixture', GIT_AUTHOR_EMAIL='fixture@example.invalid',
                        GIT_COMMITTER_NAME='Fixture', GIT_COMMITTER_EMAIL='fixture@example.invalid')
        self.git('init', '-q')
        self.git('config', 'commit.gpgsign', 'false')
        self.commit_file('README.md', b'Safe sample.\n')

    def git(self, *args):
        return subprocess.run([GIT, *args], cwd=self.repo, env=self.env,
                              check=True, capture_output=True, text=True).stdout

    def commit_file(self, name, contents, message='Update fixture'):
        (self.repo / name).write_bytes(contents)
        self.git('add', '--', name)
        self.git('commit', '-qm', message)

    def scan(self, mode, pattern=None, failed_commands=()):
        env = dict(self.env)
        if pattern is not None:
            env['FUGUE_PRIVATE_INFRA_PATTERN'] = pattern
        if failed_commands:
            shim = self.root / 'tools'
            shim.mkdir(exist_ok=True)
            cases = '|'.join(failed_commands)
            wrapper = shim / 'git'
            wrapper.write_text('#!/bin/sh\ncase "$1" in\n' + cases +
                               ') echo "injected Git read failure" >&2; exit 72;;\nesac\nexec ' +
                               shlex.quote(GIT) + ' "$@"\n')
            wrapper.chmod(0o755)
            env['PATH'] = str(shim) + os.pathsep + env.get('PATH', '')
        return subprocess.run(['bash', str(SCRIPT), mode], cwd=self.repo, env=env,
                              capture_output=True, text=True, timeout=15)

    def assert_scan_error(self, result):
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertNotIn(PASSED, result.stdout)
        self.assertTrue(result.stderr)

    def test_clean_current_and_history(self):
        for mode in ('--current-only', '--history-only'):
            with self.subTest(mode=mode):
                result = self.scan(mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(PASSED, result.stdout)

    def test_current_match_reports_path_without_secret(self):
        self.commit_file('notes with spaces.txt', (TOKEN + '\n').encode())
        result = self.scan('--current-only')
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('notes with spaces.txt', result.stdout)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)
        self.assertNotIn(PASSED, result.stdout)

    def test_binary_match_is_not_ignored(self):
        self.commit_file('fixture.bin', b'\0' + TOKEN.encode() + b'\0')
        for mode in ('--current-only', '--history-only'):
            with self.subTest(mode=mode):
                result = self.scan(mode)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn('fixture.bin', result.stdout)
                self.assertNotIn(TOKEN, result.stdout + result.stderr)

    def test_dash_file_does_not_consume_remaining_inventory(self):
        self.commit_file('-', b'Safe.\n')
        self.commit_file('later.txt', b'Safe.\n')
        (self.repo / 'later.txt').write_text(TOKEN)
        result = self.scan('--current-only')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('later.txt', result.stdout)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)
        self.assertNotIn(PASSED, result.stdout)

    def test_large_history_result_remains_a_match(self):
        shim = self.root / 'history-tools'
        shim.mkdir()
        wrapper = shim / 'git'
        wrapper.write_text('''#!/bin/sh
if [ "$1" = log ]; then
  i=1
  while [ "$i" -le 10000 ]; do
    printf 'matched-history-file-%s.txt\\n' "$i"
    i=$((i + 1))
  done
  exit 0
fi
exec ''' + shlex.quote(GIT) + ' "$@"\n')
        wrapper.chmod(0o755)
        self.env['PATH'] = str(shim) + os.pathsep + self.env.get('PATH', '')
        result = self.scan('--history-only')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count('matched-history-file-'), 200)
        self.assertNotIn(PASSED, result.stdout)

    def test_removed_secret_stays_visible_in_history_without_printing_it(self):
        self.commit_file('removed.txt', (TOKEN + '\n').encode(), TOKEN)
        self.commit_file('removed.txt', b'Removed.\n')
        self.assertEqual(self.scan('--current-only').returncode, 0)
        result = self.scan('--history-only')
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('removed.txt', result.stdout)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)

    def test_invalid_current_pattern_is_an_error(self):
        self.assert_scan_error(self.scan('--current-only', pattern='['))

    def test_invalid_history_pattern_is_an_error(self):
        self.assert_scan_error(self.scan('--history-only', pattern='['))

    def test_current_git_failure_is_an_error(self):
        self.assert_scan_error(self.scan('--current-only', failed_commands=('grep', 'ls-files')))

    def test_history_git_failure_is_an_error(self):
        self.assert_scan_error(self.scan('--history-only', failed_commands=('log',)))

    def test_repository_lookup_failure_is_an_error(self):
        self.assert_scan_error(self.scan('--current-only', failed_commands=('rev-parse',)))

    @unittest.skipIf(os.geteuid() == 0, 'Root can read chmod 000 fixtures')
    def test_unreadable_tracked_file_is_an_error(self):
        self.commit_file('unreadable.txt', TOKEN.encode())
        path = self.repo / 'unreadable.txt'
        path.chmod(0)
        try:
            with self.assertRaises(PermissionError):
                path.read_bytes()
            self.assert_scan_error(self.scan('--current-only'))
        finally:
            path.chmod(0o600)

    def check_index_hint(self, flag):
        self.commit_file('hinted.txt', b'Safe.\n')
        self.git('update-index', '--' + flag, 'hinted.txt')
        (self.repo / 'hinted.txt').write_text(TOKEN)
        before = self.git('ls-files', '-v', '--', 'hinted.txt')
        result = self.scan('--current-only')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('hinted.txt', result.stdout)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)
        self.assertEqual(self.git('ls-files', '-v', '--', 'hinted.txt'), before)

    def test_assume_unchanged_does_not_hide_working_contents(self):
        self.check_index_hint('assume-unchanged')

    def test_skip_worktree_does_not_hide_present_working_contents(self):
        self.check_index_hint('skip-worktree')

    def test_invalid_pattern_is_rejected_with_no_tracked_files(self):
        self.git('rm', 'README.md')
        self.git('commit', '-qm', 'Empty current tree')
        self.assert_scan_error(self.scan('--current-only', pattern='['))

    def test_private_identifier_and_untracked_scope(self):
        (self.repo / 'untracked.txt').write_text(TOKEN)
        self.assertEqual(self.scan('--current-only').returncode, 0)
        self.commit_file('deployment.txt', b'private.example.invalid\n')
        for mode in ('--current-only', '--history-only'):
            with self.subTest(mode=mode):
                result = self.scan(mode, pattern=r'private\.example\.invalid')
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn('deployment.txt', result.stdout)
                self.assertNotIn('private.example.invalid', result.stdout)


if __name__ == '__main__':
    unittest.main()
