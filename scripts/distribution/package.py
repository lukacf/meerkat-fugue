#!/usr/bin/env python3
"""Build a checksummed release from clean, native Fugue binaries and pinned rkat."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
BINARIES = ('meerkat-fugue', 'fugue-linear-mcp', 'fugue-jira-mcp',
            'fugue-github-mcp', 'fugue-host-mcp')


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    identity = json.loads(subprocess.check_output([str(args.bin_dir / 'meerkat-fugue'), 'build-info']))
    if identity['dirty'] or not re.fullmatch('[0-9a-f]{40}', identity['commit']):
        raise SystemExit('Release binaries must come from a clean committed checkout.')
    head = subprocess.check_output(['git', '-c', 'core.fsmonitor=false', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    if head != identity['commit']:
        raise SystemExit('Binary identity does not match the packaging checkout.')
    if subprocess.check_output(['git', '-c', 'core.fsmonitor=false', 'status', '--porcelain'], cwd=ROOT):
        raise SystemExit('The packaging checkout must be clean.')
    for name in BINARIES:
        helper = json.loads(subprocess.check_output([str(args.bin_dir / name), 'build-info']))
        if helper != identity:
            raise SystemExit(f'{name} comes from a different build. Rebuild all binaries together.')
    pins = json.loads(Path(__file__).with_name('rkat.json').read_text())
    asset = f"rkat-{pins['version']}-{args.target}.tar.gz"
    expected = pins['assets'].get(asset)
    if not expected:
        raise SystemExit('No verified rkat artifact for this target.')
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='fugue-package-') as temporary:
        root = Path(temporary)
        stage = root / 'bundle'
        (stage / 'bin').mkdir(parents=True)
        for name in BINARIES:
            shutil.copy2(args.bin_dir / name, stage / 'bin' / name)
        download = root / asset
        url = f"https://github.com/{pins['repository']}/releases/download/v{pins['version']}/{asset}"
        with urllib.request.urlopen(url, timeout=120) as response, download.open('wb') as output:
            shutil.copyfileobj(response, output)
        if digest(download) != expected:
            raise SystemExit('rkat artifact checksum mismatch.')
        with tarfile.open(download) as archive:
            members = [member for member in archive.getmembers() if Path(member.name).name == 'rkat' and member.isfile()]
            if len(members) != 1:
                raise SystemExit('rkat archive must contain exactly one CLI executable.')
            with archive.extractfile(members[0]) as source, (stage / 'bin/rkat').open('wb') as output:
                shutil.copyfileobj(source, output)
            (stage / 'bin/rkat').chmod(0o755)
        for name in ('LICENSE-MIT', 'LICENSE-APACHE'):
            shutil.copy2(ROOT / name, stage / name)
        manifest = {'schema': 1, 'fugue': identity, 'target': args.target,
                    'rkat': {'version': pins['version'], 'archive_sha256': expected},
                    'files': {str(path.relative_to(stage)): digest(path) for path in sorted(stage.rglob('*')) if path.is_file()}}
        (stage / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        name = f"fugue-{identity['version']}-{args.target}.tar.gz"
        output = args.output / name
        with tarfile.open(output, 'w:gz') as archive:
            for path in sorted(stage.rglob('*')):
                if path.is_file():
                    archive.add(path, arcname=path.relative_to(stage), recursive=False)
        checksum = digest(output)
        (args.output / (name + '.sha256')).write_text(f'{checksum}  {name}\n')
        print(json.dumps({'archive': str(output), 'sha256': checksum, 'manifest': manifest}, indent=2))


if __name__ == '__main__':
    main()
