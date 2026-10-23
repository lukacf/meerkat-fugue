import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('fugue_install', Path(__file__).with_name('install.py'))
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def bundle(root, commit='a', invalid=None):
    identity = {'version': '0.1.0', 'commit': commit * 40, 'tree': 'c' * 40, 'dirty': False,
                'runtime_storage': 'meta_native_v1'}
    files = {f'bin/{name}': b'#!/bin/sh\necho rkat 0.9.4\n' for name in installer.BINARIES}
    for name in installer.BINARIES - {'rkat'}:
        files['bin/' + name] = ("#!/bin/sh\nprintf '%s\\n' '" + json.dumps(identity) + "'\n").encode()
    files.update({'LICENSE-MIT': b'license', 'LICENSE-APACHE': b'license'})
    manifest = {'schema': 1, 'fugue': identity, 'target': installer.target(), 'rkat': {'version': '0.9.4'},
                'files': {name: hashlib.sha256(value).hexdigest() for name, value in files.items()}}
    if invalid == 'target':
        manifest['target'] = 'wrong-platform'
    if invalid == 'checksum':
        manifest['files']['bin/rkat'] = '0' * 64
    if invalid in ('identity', 'rkat-version'):
        name = 'fugue-jira-mcp' if invalid == 'identity' else 'rkat'
        files['bin/' + name] = b'#!/bin/sh\necho "{}"\n'
        manifest['files']['bin/' + name] = hashlib.sha256(files['bin/' + name]).hexdigest()
    files['manifest.json'] = json.dumps(manifest).encode()
    if invalid == 'path':
        files['../../outside'] = b'bad'
    if invalid == 'missing':
        del files['bin/fugue-jira-mcp']
    path = root / f'{commit}-{invalid}.tar.gz'
    with tarfile.open(path, 'w:gz') as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            if invalid == 'link' and name == 'bin/rkat':
                info.type = tarfile.SYMTYPE
                info.linkname = '/etc/passwd'
            archive.addfile(info, io.BytesIO(data))
        if invalid == 'duplicate':
            info = tarfile.TarInfo('manifest.json')
            archive.addfile(info, io.BytesIO())
    return path, installer.digest(path)


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.prefix = self.root / 'install'

    def test_install_repeat_and_atomic_upgrade_retain_previous(self):
        first = bundle(self.root)
        installer.install(*first, self.prefix)
        before = (self.prefix / 'current').readlink()
        installer.install(*first, self.prefix)
        self.assertEqual(before, (self.prefix / 'current').readlink())
        installer.install(*bundle(self.root, 'b'), self.prefix)
        self.assertNotEqual(before, (self.prefix / 'current').readlink())
        self.assertEqual(before, (self.prefix / 'previous').readlink())
        manifest = json.loads((self.prefix / 'current/manifest.json').read_text())
        self.assertEqual(manifest['fugue']['runtime_storage'], 'meta_native_v1')
        for name in installer.BINARIES:
            self.assertTrue((self.prefix / 'bin' / name).is_file())

    def test_service_account_can_traverse_release_after_private_staging(self):
        first = bundle(self.root)
        installer.install(*first, self.prefix)
        release = (self.prefix / 'current').resolve()
        self.assertEqual(release.stat().st_mode & 0o777, 0o755)
        self.assertEqual((release / 'bin').stat().st_mode & 0o777, 0o755)
        # Repair the same verified release after an older installer left its
        # temporary directory mode in place.
        release.chmod(0o700)
        installer.install(*first, self.prefix)
        self.assertEqual(release.stat().st_mode & 0o777, 0o755)

    def test_rejected_bundles_preserve_running_release_and_private_state(self):
        installer.install(*bundle(self.root), self.prefix)
        before = (self.prefix / 'current').readlink()
        state = self.root / 'private-state'
        state.write_text('preserve credentials and sessions')
        for invalid in ('target', 'checksum', 'path', 'missing', 'link', 'duplicate'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                installer.install(*bundle(self.root, 'b', invalid), self.prefix)
            self.assertEqual(before, (self.prefix / 'current').readlink())
            self.assertEqual(state.read_text(), 'preserve credentials and sessions')
        self.assertFalse((self.root / 'outside').exists())

    def test_download_corruption_is_rejected_before_executing(self):
        archive, _ = bundle(self.root)
        with self.assertRaisesRegex(ValueError, 'Download checksum'):
            installer.install(archive, '0' * 64, self.prefix)
        self.assertFalse((self.prefix / 'current').exists())

    def test_existing_unmanaged_binary_is_never_overwritten(self):
        (self.prefix / 'bin').mkdir(parents=True)
        binary = self.prefix / 'bin/rkat'
        binary.write_text('user installation')
        with self.assertRaisesRegex(ValueError, 'unmanaged'):
            installer.install(*bundle(self.root), self.prefix)
        self.assertEqual(binary.read_text(), 'user installation')

    def test_modified_installed_release_is_not_silently_trusted_on_retry(self):
        first = bundle(self.root)
        installer.install(*first, self.prefix)
        (self.prefix / 'current/bin/rkat').write_text('modified')
        with self.assertRaisesRegex(ValueError, 'files have changed'):
            installer.install(*first, self.prefix)

    def test_unmanaged_release_links_are_rejected_before_candidate_execution(self):
        for link_name in ('current', 'previous'):
            for index, value in enumerate(('user file', '../outside', 'versions/..', 'versions/not-a-release')):
                with self.subTest(link=link_name, value=value):
                    prefix = self.root / f'layout-{link_name}-{index}'
                    installer.install(*bundle(self.root), prefix)
                    before = (prefix / 'current').readlink()
                    link = prefix / link_name
                    if link.is_symlink() or link.exists():
                        link.unlink()
                    if value == 'user file':
                        link.write_text('user-owned rollback notes')
                    else:
                        link.symlink_to(value)
                    with mock.patch.object(installer.subprocess, 'check_output',
                                           side_effect=AssertionError('candidate ran before layout validation')) as execute:
                        with self.assertRaisesRegex(ValueError, 'managed release link'):
                            installer.install(*bundle(self.root, 'b'), prefix)
                        execute.assert_not_called()
                    if value == 'user file':
                        self.assertEqual(link.read_text(), 'user-owned rollback notes')
                    else:
                        self.assertEqual(os.readlink(link), value)
                    if link_name == 'previous':
                        self.assertEqual((prefix / 'current').readlink(), before)

    def test_failed_activation_preserves_current_and_previous_then_retry_succeeds(self):
        installer.install(*bundle(self.root), self.prefix)
        installer.install(*bundle(self.root, 'b'), self.prefix)
        current = (self.prefix / 'current').readlink()
        previous = (self.prefix / 'previous').readlink()
        candidate = bundle(self.root, 'c')
        replace = os.replace

        def fail_current(source, destination):
            if destination == self.prefix / 'current':
                raise OSError('simulated activation failure')
            return replace(source, destination)

        with mock.patch.object(installer.os, 'replace', side_effect=fail_current):
            with self.assertRaisesRegex(OSError, 'simulated activation failure'):
                installer.install(*candidate, self.prefix)
        self.assertEqual((self.prefix / 'current').readlink(), current)
        self.assertEqual((self.prefix / 'previous').readlink(), previous)
        self.assertEqual(list(self.prefix.glob('*.pending')), [])
        self.assertEqual(list(self.prefix.glob('.unpack-*')), [])
        installer.install(*candidate, self.prefix)
        self.assertEqual((self.prefix / 'previous').readlink(), current)
        self.assertNotEqual((self.prefix / 'current').readlink(), current)

    def test_failed_first_upgrade_does_not_create_a_false_previous_release(self):
        installer.install(*bundle(self.root), self.prefix)
        current = (self.prefix / 'current').readlink()
        replace = os.replace

        def fail_current(source, destination):
            if destination == self.prefix / 'current':
                raise OSError('simulated activation failure')
            return replace(source, destination)

        with mock.patch.object(installer.os, 'replace', side_effect=fail_current):
            with self.assertRaises(OSError):
                installer.install(*bundle(self.root, 'b'), self.prefix)
        self.assertEqual((self.prefix / 'current').readlink(), current)
        self.assertFalse((self.prefix / 'previous').is_symlink())

    def test_candidate_identity_and_timeout_failures_leave_both_releases_untouched(self):
        installer.install(*bundle(self.root), self.prefix)
        installer.install(*bundle(self.root, 'b'), self.prefix)
        links = {name: (self.prefix / name).readlink() for name in ('current', 'previous')}
        for invalid in ('identity', 'rkat-version'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                installer.install(*bundle(self.root, 'c', invalid), self.prefix)
        with mock.patch.object(installer.subprocess, 'check_output',
                               side_effect=subprocess.TimeoutExpired('build-info', 30)):
            with self.assertRaises(subprocess.TimeoutExpired):
                installer.install(*bundle(self.root, 'c'), self.prefix)
        self.assertEqual({name: (self.prefix / name).readlink() for name in links}, links)
        self.assertEqual(list(self.prefix.glob('.unpack-*')), [])

    def test_concurrent_installer_refuses_before_executing_or_switching(self):
        installer.install(*bundle(self.root), self.prefix)
        before = (self.prefix / 'current').readlink()
        with (self.prefix / '.install.lock').open('a') as lock:
            installer.fcntl.flock(lock, installer.fcntl.LOCK_EX | installer.fcntl.LOCK_NB)
            with mock.patch.object(installer.subprocess, 'check_output') as execute:
                with self.assertRaises(BlockingIOError):
                    installer.install(*bundle(self.root, 'b'), self.prefix)
                execute.assert_not_called()
        self.assertEqual((self.prefix / 'current').readlink(), before)

    def test_cli_start_command_handles_spaces_and_opens_the_welcome_page(self):
        archive, checksum = bundle(self.root)
        prefix = self.root / 'my installation'
        result = subprocess.run([sys.executable, str(Path(installer.__file__)),
                                 '--archive', str(archive), '--sha256', checksum,
                                 '--prefix', str(prefix)], text=True, capture_output=True, check=True)
        command = next(line.removeprefix('Start: ') for line in result.stdout.splitlines()
                       if line.startswith('Start: '))
        self.assertEqual(shlex.split(command), [str(prefix / 'bin/meerkat-fugue'), 'serve'])
        self.assertIn('Then open http://127.0.0.1:8788/\n', result.stdout)


if __name__ == '__main__':
    unittest.main()
