#!/usr/bin/env python3
"""Bound native qualification phases and retain evidence from process startup."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


RETIREMENT_TEST = 'meta_runtime::tests::runtime_members_emit_canonical_structural_events'


class Cancellation:
    # Signal handlers must not acquire threading locks. A flag is also visible
    # to the worker threads, which poll it between bounded waits.
    def __init__(self):
        self.requested = False

    def set(self):
        self.requested = True

    def is_set(self):
        return self.requested


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def record(path, row):
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(row, indent=2) + '\n')
    temporary.replace(path)


def child_exited(process):
    """Observe exit without reaping the leader that reserves the group ID."""
    if process.returncode is not None:
        raise RuntimeError('Process-group ownership was lost: leader already reaped')
    status = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    return status is not None and status.si_pid == process.pid


def signal_owned_group(process, signum):
    child_exited(process)  # Fail before signalling if another waiter reaped it.
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Darwin reports EPERM for a group containing only zombies. Confirm
        # that no live member remains instead of masking a real denied signal.
        # The leader is still waitable, so its group ID cannot be recycled.
        if sys.platform != 'darwin':
            raise
        result = subprocess.run(['/bin/ps', '-ax', '-o', 'pgid=,stat='],
                                capture_output=True, text=True, check=True, timeout=3)
        for line in result.stdout.splitlines():
            group, state = line.split()
            if int(group) == process.pid and not state.startswith('Z'):
                raise
        return False
    return True


def stop_process_group(process):
    """Finish all group signals before releasing the owned leader's PID."""
    try:
        if signal_owned_group(process, signal.SIGTERM):
            # The unreaped leader can keep the group observable after all its
            # descendants exit. Allow bounded grace, then finish cleanup.
            # Neither poll() nor wait() is safe before the last signal.
            time.sleep(3)
            signal_owned_group(process, signal.SIGKILL)
    finally:
        process.wait(timeout=3)


def run_phase(command, report_dir, name, timeout, cancelled, env=None):
    path = report_dir / f'{name}.json'
    row = {'phase': name, 'status': 'running', 'started_at': timestamp(), 'qualified': False}
    record(path, row)
    process = None
    # Connect stdout/stderr directly to disk. An outer cancellation must not
    # discard bytes buffered by a parent subprocess.run(capture_output=True).
    with (report_dir / f'{name}.log').open('wb', buffering=0) as output:
        try:
            if not cancelled.is_set():
                process = subprocess.Popen(command, env=env, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                row['pid'] = process.pid
                record(path, row)
                deadline = time.monotonic() + timeout
                while not cancelled.is_set() and not child_exited(process):
                    if time.monotonic() >= deadline:
                        row['timeout'] = True
                        break
                    time.sleep(0.05)
        except Exception as error:
            row['error'] = str(error)
        finally:
            if process is not None:
                try:
                    stop_process_group(process)
                except Exception as error:
                    row['cleanup_error'] = str(error)
                row['exit'] = process.returncode
    row['cancelled'] = cancelled.is_set()
    row['qualified'] = (row.get('exit') == 0 and not row.get('timeout')
                        and not row['cancelled'] and 'error' not in row and 'cleanup_error' not in row)
    row['status'] = 'cancelled' if row['cancelled'] else ('completed' if row['qualified'] else 'failed')
    row['finished_at'] = timestamp()
    record(path, row)
    return row


def qualify_retirement(executable, report_dir, attempt, cancelled=None, timeout=180):
    # Each trial is already a separate process. Bypass the nested isolation
    # wrapper so successful exits cannot discard the actual runtime diagnostics.
    env = dict(os.environ, FUGUE_ISOLATED_RUNTIME_TEST='runtime_members_emit_canonical_structural_events')
    name = f'attempt-{attempt:02}'
    row = run_phase([executable, RETIREMENT_TEST, '--exact', '--nocapture'], report_dir,
                    name, timeout, cancelled if cancelled is not None else Cancellation(), env)
    output = (report_dir / f'{name}.log').read_text(errors='replace')
    row.update(attempt=attempt, one_test_passed='test result: ok. 1 passed;' in output,
               transcript_conflict=('failed to realize committed handoffs' in output
                                    or 'rewrite rejected: previous transcript revision' in output))
    row['qualified'] = row['qualified'] and row['one_test_passed'] and not row['transcript_conflict']
    if not row['qualified'] and row['status'] == 'completed':
        row['status'] = 'failed'
    record(report_dir / f'{name}.json', row)
    print(f'Native retirement qualification {attempt}/20: {json.dumps(row)}', flush=True)
    if not row['qualified']:
        print(output, flush=True)
    return row


def main():
    if os.name != 'posix' or not hasattr(os, 'WNOWAIT') or not hasattr(os, 'waitid'):
        raise SystemExit('Native qualification requires POSIX waitid with WNOWAIT')
    report_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(sys.argv[1]).with_suffix('.retirement')
    report_dir.mkdir(parents=True, exist_ok=False)
    cancelled = Cancellation()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _signum, _frame: cancelled.set())
    summary = {'status': 'running', 'started_at': timestamp(), 'qualified': False,
               'concurrency': 4, 'phases': [], 'trials': []}
    record(report_dir / 'summary.json', summary)
    try:
        executables = set()
        for line in Path(sys.argv[1]).read_text().splitlines():
            message = json.loads(line)
            if (message.get('reason') == 'compiler-artifact'
                    and message.get('target', {}).get('name') == 'meerkat_fugue'
                    and message.get('profile', {}).get('test')
                    and message.get('executable')):
                executables.add(message['executable'])
        if len(executables) != 1:
            raise RuntimeError('Expected exactly one Fugue library test executable')
        executable = executables.pop()
        with Path(executable).open('rb') as stream:
            summary['executable_sha256'] = hashlib.file_digest(stream, 'sha256').hexdigest()
        record(report_dir / 'summary.json', summary)

        def phase(name, arguments, timeout):
            row = run_phase([executable, *arguments], report_dir, name, timeout, cancelled)
            summary['phases'].append(row)
            record(report_dir / 'summary.json', summary)
            if not row['qualified']:
                raise RuntimeError(f'Native phase {name} failed; see retained evidence')
            return (report_dir / f'{name}.log').read_text(errors='replace')

        selection = 'native_flow_delivers_worker'
        listing = phase('handoff-list', [selection, '--list'], 15)
        selected = {line.removesuffix(': test') for line in listing.splitlines() if line.endswith(': test')}
        required = {
            'ensemble::runtime::tests::native_flow_delivers_worker_final_without_peer_reply',
            'ensemble::runtime::tests::native_flow_delivers_worker_failure_without_peer_reply',
        }
        if not required <= selected:
            raise RuntimeError('The executable is missing required native restart tests')
        for attempt in range(1, 6):
            phase(f'handoff-{attempt:02}', [selection], 60)
        listing = phase('retirement-list', [RETIREMENT_TEST, '--exact', '--list'], 15)
        if RETIREMENT_TEST + ': test' not in listing.splitlines():
            raise RuntimeError('The executable is missing the native retirement qualification test')
        with ThreadPoolExecutor(max_workers=4) as pool:
            summary['trials'] = list(pool.map(
                lambda attempt: qualify_retirement(executable, report_dir, attempt, cancelled), range(1, 21)))
        summary['qualified'] = all(row['qualified'] for row in summary['trials']) and not cancelled.is_set()
    except Exception as error:
        summary['error'] = str(error)
    finally:
        summary['qualified'] = summary['qualified'] and not cancelled.is_set()
        summary['status'] = 'cancelled' if cancelled.is_set() else ('completed' if summary['qualified'] else 'failed')
        summary['finished_at'] = timestamp()
        record(report_dir / 'summary.json', summary)
    if not summary['qualified']:
        raise SystemExit('Native qualification failed; all phase and trial logs were retained')


if __name__ == '__main__':
    main()
