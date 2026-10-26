#!/usr/bin/env python3
"""Install one verified Fugue bundle without touching credentials or repository state."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile

BINARIES = {'meerkat-fugue', 'fugue-linear-mcp', 'fugue-jira-mcp',
            'fugue-github-mcp', 'fugue-host-mcp', 'rkat'}
PAYLOAD_FILES = {'LICENSE-MIT', 'LICENSE-APACHE'} | {'bin/' + name for name in BINARIES}


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def target():
    machine = {'arm64': 'aarch64', 'aarch64': 'aarch64', 'x86_64': 'x86_64', 'AMD64': 'x86_64'}.get(platform.machine())
    system = {'Linux': 'unknown-linux-gnu', 'Darwin': 'apple-darwin'}.get(platform.system())
    if not machine or not system:
        raise ValueError('This release supports Linux and macOS on Intel/AMD or ARM64.')
    return f'{machine}-{system}'


def validate_manifest(manifest):
    """Validate incoming and installed metadata before trusting its fields."""
    if not isinstance(manifest, dict):
        raise ValueError('Release manifest must be a JSON object.')
    if type(manifest.get('schema')) is not int or manifest['schema'] != 1:
        raise ValueError('Unsupported release manifest schema.')
    if manifest.get('target') != target():
        raise ValueError('This release is not compatible with this computer.')
    identity = manifest.get('fugue')
    if (not isinstance(identity, dict) or identity.get('dirty') is not False
            or any(not isinstance(identity.get(field), str)
                   or not re.fullmatch('[0-9a-f]{40}', identity[field])
                   for field in ('commit', 'tree'))):
        raise ValueError('The release does not identify a clean source commit and tree.')
    for name in ('fugue', 'rkat'):
        metadata = manifest.get(name)
        if (not isinstance(metadata, dict) or not isinstance(metadata.get('version'), str)
                or not metadata['version'].strip()):
            raise ValueError(f'Release manifest is missing a valid {name} version.')
    files = manifest.get('files')
    if not isinstance(files, dict) or set(files) != PAYLOAD_FILES:
        raise ValueError('Release file manifest is incomplete.')
    if any(not isinstance(checksum, str) or not re.fullmatch('[0-9a-f]{64}', checksum)
           for checksum in files.values()):
        raise ValueError('Release file manifest contains an invalid checksum.')


def unpack(archive_path, expected, destination):
    if not re.fullmatch('[0-9a-f]{64}', expected) or digest(archive_path) != expected:
        raise ValueError('Download checksum mismatch. The current installation has not changed.')
    allowed = PAYLOAD_FILES | {'manifest.json'}
    with tarfile.open(archive_path) as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if set(names) != allowed or len(names) != len(allowed):
            raise ValueError('Unexpected, missing, or duplicate files in release archive.')
        if any(not member.isfile() or member.size > 1024 * 1024 * 1024 for member in members):
            raise ValueError('Release archive contains an unsafe member.')
        for member in members:
            path = destination / PurePosixPath(member.name)
            path.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, path.open('xb') as output:
                shutil.copyfileobj(source, output)
            path.chmod(0o755 if member.name.startswith('bin/') else 0o644)
    manifest = json.loads((destination / 'manifest.json').read_text())
    validate_manifest(manifest)
    for name, expected_file in manifest['files'].items():
        if digest(destination / name) != expected_file:
            raise ValueError('Release file checksum mismatch.')
    return manifest


def replace_link(link, value):
    pending = link.with_name(link.name + '.pending')
    if pending.exists() or pending.is_symlink():
        pending.unlink()
    pending.symlink_to(value)
    try:
        os.replace(pending, link)
    finally:
        pending.unlink(missing_ok=True)


def managed_link(link):
    if not link.is_symlink():
        if link.exists():
            raise ValueError(f'Existing {link.name} path is not a managed release link.')
        return None
    value = os.readlink(link)
    if not re.fullmatch(r'versions/[0-9a-f]{40}-[0-9a-f]{16}', value):
        raise ValueError(f'Existing {link.name} path is not a managed release link.')
    return value


def sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def finish_activation(prefix):
    (prefix / '.activation.json').unlink()
    sync_directory(prefix)


def recover_activation(prefix):
    """Restore an interrupted switch, or recognize its completed activation."""
    journal = prefix / '.activation.json'
    if journal.is_symlink():
        raise ValueError('Activation recovery record must not be a symbolic link.')
    if not journal.exists():
        return
    record = json.loads(journal.read_text())
    if (not isinstance(record, dict)
            or set(record) != {'schema', 'current', 'previous', 'candidate'}
            or type(record['schema']) is not int or record['schema'] != 1):
        raise ValueError('Activation recovery record is invalid; installation stopped.')
    for name in ('current', 'previous', 'candidate'):
        value = record[name]
        if name == 'previous' and value is None:
            continue
        if not isinstance(value, str) or not re.fullmatch(r'versions/[0-9a-f]{40}-[0-9a-f]{16}', value):
            raise ValueError('Activation recovery record is invalid; installation stopped.')
    current = managed_link(prefix / 'current')
    previous = managed_link(prefix / 'previous')
    if (record['current'] == record['candidate']
            or current not in (record['current'], record['candidate'])
            or previous not in (record['previous'], record['current'])
            or (current == record['candidate'] and previous != record['current'])):
        raise ValueError('Release selections differ from the interrupted activation; installation stopped.')
    if current == record['current'] and previous != record['previous']:
        if record['previous'] is None:
            (prefix / 'previous').unlink()
        else:
            replace_link(prefix / 'previous', record['previous'])
        sync_directory(prefix)
    finish_activation(prefix)


def begin_activation(prefix, current, previous, candidate):
    record = {'schema': 1, 'current': current, 'previous': previous, 'candidate': candidate}
    with tempfile.NamedTemporaryFile(mode='w', prefix='.activation-', dir=prefix, delete=False) as output:
        temporary = Path(output.name)
        try:
            json.dump(record, output)
            output.flush()
            os.fsync(output.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, prefix / '.activation.json')
        sync_directory(prefix)
    finally:
        temporary.unlink(missing_ok=True)


def verify_installed_release(prefix, relative):
    """Do not promote a damaged current release to the rollback target."""
    release = prefix / relative
    if release.is_symlink() or not release.is_dir() or (release / 'bin').is_symlink():
        raise ValueError('Existing release layout has changed; installation stopped.')
    manifest_path = release / 'manifest.json'
    if manifest_path.is_symlink():
        raise ValueError('Existing release manifest has changed; installation stopped.')
    try:
        manifest = json.loads(manifest_path.read_text())
        validate_manifest(manifest)
    except ValueError as error:
        raise ValueError('Existing release manifest is invalid; installation stopped. Restore the affected release before installing; the current and previous links are preserved.') from error
    if manifest['fugue']['commit'] != release.name.split('-')[0]:
        raise ValueError('Existing release manifest has changed; installation stopped.')
    for name, checksum in manifest['files'].items():
        path = release / name
        if path.is_symlink() or not path.is_file() or digest(path) != checksum:
            raise ValueError('Existing release files have changed; installation stopped. Restore the affected release before installing; the current and previous links are preserved.')
        if name.startswith('bin/') and ((path.stat().st_mode & 0o111) != 0o111
                                        or not os.access(path, os.R_OK | os.X_OK)):
            raise ValueError('Existing release binary is not executable; installation stopped. Restore its permissions before installing; the current and previous links are preserved.')


def install(archive, expected, prefix):
    if prefix.is_symlink():
        raise ValueError('The installation directory must not be a symbolic link.')
    prefix.mkdir(parents=True, exist_ok=True)
    with (prefix / '.install.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for name in ('versions', 'bin'):
            if (prefix / name).is_symlink():
                raise ValueError('Installation layout contains an unexpected symbolic link.')
        versions = prefix / 'versions'
        versions.mkdir(exist_ok=True)
        recover_activation(prefix)
        current = prefix / 'current'
        previous = prefix / 'previous'
        old = managed_link(current)
        prior = managed_link(previous)
        if old is not None:
            verify_installed_release(prefix, old)
        for name in BINARIES:
            link = prefix / 'bin' / name
            if link.exists() or link.is_symlink():
                if not link.is_symlink() or os.readlink(link) != f'../current/bin/{name}':
                    raise ValueError(f'Refusing to replace an unmanaged executable: {link}')
        with tempfile.TemporaryDirectory(prefix='.unpack-', dir=prefix) as temporary:
            stage = Path(temporary)
            manifest = unpack(archive, expected, stage)
            for binary in BINARIES - {'rkat'}:
                actual = json.loads(subprocess.check_output([str(stage / 'bin' / binary), 'build-info'], timeout=30))
                if actual != manifest['fugue']:
                    raise ValueError('Executable build identity differs from its release manifest.')
            rkat_version = subprocess.check_output([str(stage / 'bin/rkat'), '--version'], text=True, timeout=30).strip()
            if rkat_version not in (f"rkat {manifest['rkat']['version']}", f"meerkat {manifest['rkat']['version']}"):
                raise ValueError('The native rkat version differs from its release manifest.')
            name = manifest['fugue']['commit'] + '-' + expected[:16]
            release = versions / name
            if release.exists():
                if release.is_symlink() or (release / 'manifest.json').read_bytes() != (stage / 'manifest.json').read_bytes():
                    raise ValueError('Existing release directory does not match this bundle.')
                # Activate the retained files, not the freshly checked stage.
                # Recheck their layout and execute permissions as well as bytes.
                verify_installed_release(prefix, f'versions/{name}')
            else:
                os.replace(stage, release)
            # TemporaryDirectory is deliberately private during verification.
            # Published files are public executables; a different service user
            # must be able to traverse the verified release after root installs.
            release.chmod(0o755)
            (release / 'bin').chmod(0o755)
            (prefix / 'bin').mkdir(exist_ok=True)
            for binary in BINARIES:
                link = prefix / 'bin' / binary
                if not link.is_symlink():
                    link.symlink_to(f'../current/bin/{binary}')
            changing_previous = old is not None and old != f'versions/{name}'
            if changing_previous:
                begin_activation(prefix, old, prior, f'versions/{name}')
            try:
                if changing_previous:
                    replace_link(previous, old)
                    sync_directory(prefix)
                replace_link(current, f'versions/{name}')
                sync_directory(prefix)
            except OSError:
                if changing_previous:
                    recover_activation(prefix)
                raise
            if changing_previous:
                finish_activation(prefix)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True, type=Path)
    parser.add_argument('--sha256', required=True, help='Expected SHA-256 from the release receipt')
    parser.add_argument('--prefix', type=Path, default=Path.home() / '.local/share/meerkat-fugue/install')
    args = parser.parse_args()
    try:
        manifest = install(args.archive.resolve(), args.sha256, args.prefix.absolute())
    except (OSError, ValueError, KeyError, subprocess.SubprocessError, tarfile.TarError) as error:
        parser.exit(1, f'Installation stopped: {error}\n')
    print(f"Fugue {manifest['fugue']['version']} is installed at {args.prefix / 'bin'}")
    missing = [name for name in ('git', 'gh', 'node', 'npx') if not shutil.which(name)]
    if missing:
        print('Before connecting a repository, install: ' + ', '.join(missing))
    print('Start: ' + shlex.join([str(args.prefix / 'bin/meerkat-fugue'), 'serve']))
    print('Then open http://127.0.0.1:8788/')


if __name__ == '__main__':
    main()
