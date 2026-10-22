import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('fugue_install', Path(__file__).with_name('install.py'))
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def bundle(root, commit='a', invalid=None):
    identity = {'version': '0.1.0', 'commit': commit * 40, 'tree': 'c' * 40, 'dirty': False}
    files = {f'bin/{name}': b'#!/bin/sh\necho rkat 0.9.3\n' for name in installer.BINARIES}
    for name in installer.BINARIES - {'rkat'}:
        files['bin/' + name] = ("#!/bin/sh\nprintf '%s\\n' '" + json.dumps(identity) + "'\n").encode()
    files.update({'LICENSE-MIT': b'license', 'LICENSE-APACHE': b'license'})
    manifest = {'schema': 1, 'fugue': identity, 'target': installer.target(), 'rkat': {'version': '0.9.3'},
                'files': {name: hashlib.sha256(value).hexdigest() for name, value in files.items()}}
    if invalid == 'target':
        manifest['target'] = 'wrong-platform'
    if invalid == 'checksum':
        manifest['files']['bin/rkat'] = '0' * 64
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


if __name__ == '__main__':
    unittest.main()
