#!/usr/bin/env python3
"""Repeat native restart checks using one Cargo-produced test executable."""
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys


RETIREMENT_TEST = 'meta_runtime::tests::runtime_members_emit_canonical_structural_events'


def qualify_retirement(executable, report_dir, attempt):
    # Each trial is already a separate process. Bypass the nested isolation
    # wrapper so successful exits cannot discard the actual runtime diagnostics.
    env = dict(os.environ, FUGUE_ISOLATED_RUNTIME_TEST='runtime_members_emit_canonical_structural_events')
    try:
        result = subprocess.run([executable, RETIREMENT_TEST, '--exact', '--nocapture'],
                                env=env, capture_output=True, text=True, timeout=180)
        output = result.stdout + result.stderr
        passed = result.returncode == 0 and 'test result: ok. 1 passed;' in output
        conflict = ('failed to realize committed handoffs' in output
                    or 'rewrite rejected: previous transcript revision' in output)
        row = {'attempt': attempt, 'exit': result.returncode, 'one_test_passed': passed,
               'transcript_conflict': conflict, 'qualified': passed and not conflict}
    except subprocess.TimeoutExpired as error:
        def decode(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else (value or '')
        output = decode(error.stdout) + decode(error.stderr)
        row = {'attempt': attempt, 'timeout': True, 'qualified': False}
    (report_dir / f'attempt-{attempt:02}.log').write_text(output)
    (report_dir / f'attempt-{attempt:02}.json').write_text(json.dumps(row) + '\n')
    print(f'Native retirement qualification {attempt}/20: {json.dumps(row)}', flush=True)
    if not row['qualified']:
        print(output, flush=True)
    return row


def main():
    executables = set()
    for line in Path(sys.argv[1]).read_text().splitlines():
        message = json.loads(line)
        if (message.get('reason') == 'compiler-artifact'
                and message.get('target', {}).get('name') == 'meerkat_fugue'
                and message.get('profile', {}).get('test')
                and message.get('executable')):
            executables.add(message['executable'])
    if len(executables) != 1:
        raise SystemExit('Expected exactly one Fugue library test executable')
    executable = executables.pop()
    selection = 'native_flow_delivers_worker'
    listing = subprocess.check_output([executable, selection, '--list'], text=True)
    selected = {line.removesuffix(': test') for line in listing.splitlines()
                if line.endswith(': test')}
    required = {
        'ensemble::runtime::tests::native_flow_delivers_worker_final_without_peer_reply',
        'ensemble::runtime::tests::native_flow_delivers_worker_failure_without_peer_reply',
    }
    if not required <= selected:
        raise SystemExit('The executable is missing required native restart tests')
    for attempt in range(1, 6):
        print(f'Native restart qualification {attempt}/5', flush=True)
        subprocess.run([executable, selection], check=True)

    listing = subprocess.check_output([executable, RETIREMENT_TEST, '--exact', '--list'], text=True)
    if RETIREMENT_TEST + ': test' not in listing.splitlines():
        raise SystemExit('The executable is missing the native retirement qualification test')
    report_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(sys.argv[1]).with_suffix('.retirement')
    report_dir.mkdir(parents=True, exist_ok=False)
    print(f'Retaining all native retirement evidence in {report_dir}', flush=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda attempt: qualify_retirement(executable, report_dir, attempt), range(1, 21)))
    with Path(executable).open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    summary = {'executable_sha256': digest, 'concurrency': 4, 'trials': results,
               'qualified': all(row['qualified'] for row in results)}
    (report_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    if not summary['qualified']:
        raise SystemExit('Native retirement qualification failed; all trial logs were retained')


if __name__ == '__main__':
    main()
