#!/usr/bin/env python3
"""Repeat native restart checks using one Cargo-produced test executable."""
import json
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


if __name__ == '__main__':
    main()
