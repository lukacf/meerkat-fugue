#!/usr/bin/env python3
"""Repeat native restart checks using one Cargo-produced test executable."""
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys


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

    retirement = 'meta_runtime::tests::runtime_members_emit_canonical_structural_events'
    listing = subprocess.check_output([executable, retirement, '--exact', '--list'], text=True)
    if retirement + ': test' not in listing.splitlines():
        raise SystemExit('The executable is missing the native retirement qualification test')

    def qualify_retirement(attempt):
        # These are separate processes, each with its own native state. Parallel
        # load exposed a retirement/transcript race that serial repeats missed.
        result = subprocess.run([executable, retirement, '--exact', '--nocapture'],
                                capture_output=True, text=True)
        print(f'Native retirement qualification {attempt}/20: exit {result.returncode}', flush=True)
        if result.returncode:
            print(result.stdout + result.stderr, flush=True)
        return result.returncode

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(qualify_retirement, range(1, 21)))
    if any(results):
        raise SystemExit('Native retirement qualification failed; no retries were discarded')


if __name__ == '__main__':
    main()
