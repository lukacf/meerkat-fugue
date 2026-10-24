//! Bounded child-process execution for forge and deployment commands. Capturing into private
//! files avoids pipe backpressure and descendants keeping an output pipe open.

use std::fs::File;
use std::io::{self, Read, Seek};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

const OUTPUT_LIMIT: u64 = 16 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProcessError {
    #[error("could not prepare output capture: {0}")]
    Prepare(io::Error),
    #[error("could not start the process: {0}")]
    Spawn(io::Error),
    #[error(
        "timed out; the remote operation may have completed. Refresh its state before retrying"
    )]
    Timeout,
    #[error("output exceeded the capture limit; refresh remote state before retrying")]
    OutputLimit,
    #[error("could not observe process completion: {0}")]
    Observe(io::Error),
    #[error("could not read captured output: {0}")]
    Read(io::Error),
    #[error("could not terminate and reap the process: {0}")]
    Cleanup(io::Error),
}

impl ProcessError {
    pub(crate) fn may_have_executed(&self) -> bool {
        !matches!(self, Self::Prepare(_) | Self::Spawn(_))
    }
}

pub(crate) fn run(command: Command, timeout: Duration) -> Result<Output, ProcessError> {
    let mut capture = CapturedProcess::start(command)?;
    let result = loop {
        match capture.poll(timeout) {
            Ok(true) => break Ok(()),
            Ok(false) => std::thread::sleep(POLL_INTERVAL),
            Err(error) => break Err(error),
        }
    };
    capture.finish(result)
}

/// The future owns the child directly. Dropping it kills the owned process group
/// and reaps the leader synchronously before an enclosing operation releases its lease.
pub(crate) async fn run_async(command: Command, timeout: Duration) -> Result<Output, ProcessError> {
    let mut capture = CapturedProcess::start(command)?;
    let result = loop {
        match capture.poll(timeout) {
            Ok(true) => break Ok(()),
            Ok(false) => tokio::time::sleep(POLL_INTERVAL).await,
            Err(error) => break Err(error),
        }
    };
    capture.finish(result)
}

struct CapturedProcess {
    process: OwnedProcess,
    stdout: File,
    stderr: File,
    started: Instant,
}

impl CapturedProcess {
    fn start(mut command: Command) -> Result<Self, ProcessError> {
        let stdout = tempfile::tempfile().map_err(ProcessError::Prepare)?;
        let stderr = tempfile::tempfile().map_err(ProcessError::Prepare)?;
        command
            .stdin(Stdio::null())
            .stdout(stdout.try_clone().map_err(ProcessError::Prepare)?)
            .stderr(stderr.try_clone().map_err(ProcessError::Prepare)?)
            .env("GH_PROMPT_DISABLED", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GH_PAGER", "cat");
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let child = command.spawn().map_err(ProcessError::Spawn)?;
        Ok(Self {
            process: OwnedProcess { child: Some(child) },
            stdout,
            stderr,
            started: Instant::now(),
        })
    }

    fn poll(&mut self, timeout: Duration) -> Result<bool, ProcessError> {
        let captured = self.stdout.metadata().map_err(ProcessError::Observe)?.len()
            + self.stderr.metadata().map_err(ProcessError::Observe)?.len();
        if captured > OUTPUT_LIMIT {
            return Err(ProcessError::OutputLimit);
        }
        if self.process.has_exited().map_err(ProcessError::Observe)? {
            return Ok(true);
        }
        if self.started.elapsed() >= timeout {
            return Err(ProcessError::Timeout);
        }
        Ok(false)
    }

    fn finish(mut self, result: Result<(), ProcessError>) -> Result<Output, ProcessError> {
        let status = self
            .process
            .finish()
            .map_err(ProcessError::Cleanup)?
            .ok_or_else(|| ProcessError::Observe(io::Error::other("process is unavailable")))?;
        result?;
        let stdout = read_capture(&mut self.stdout)?;
        let stderr = read_capture(&mut self.stderr)?;
        if (stdout.len() + stderr.len()) as u64 > OUTPUT_LIMIT {
            return Err(ProcessError::OutputLimit);
        }
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}

fn read_capture(file: &mut File) -> Result<Vec<u8>, ProcessError> {
    file.rewind().map_err(ProcessError::Read)?;
    let mut bytes = Vec::new();
    file.take(OUTPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(ProcessError::Read)?;
    if bytes.len() as u64 > OUTPUT_LIMIT {
        return Err(ProcessError::OutputLimit);
    }
    Ok(bytes)
}

/// Observe a child without reaping it. Its PID cannot be recycled before the
/// group is signalled. libc is used because nix does not expose waitid on macOS.
#[cfg(unix)]
fn exited_without_reaping(child: &Child) -> io::Result<bool> {
    use nix::libc;
    loop {
        // SAFETY: zero is a valid initial siginfo_t; waitid receives a writable,
        // correctly aligned instance and a PID belonging to our unreaped child.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                child.id() as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            // SAFETY: a successful waitid initialized siginfo_t; si_pid is zero
            // when WNOHANG found no waitable event.
            return Ok(unsafe { info.si_pid() } != 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(target_os = "macos")]
fn group_has_no_other_members(pid: i32) -> io::Result<bool> {
    // Darwin reports EPERM for a group containing only an exited leader.
    // Do not waive EPERM if another member remains, including one that changed
    // credentials. A two-entry buffer is enough to refuse any additional member.
    let mut members = [0_i32; 2];
    nix::errno::Errno::clear();
    // SAFETY: the buffer holds exactly two aligned pid_t values and its supplied
    // byte size matches that allocation. The unreaped leader still owns pid.
    let count = unsafe {
        nix::libc::proc_listpgrppids(
            pid,
            members.as_mut_ptr().cast(),
            std::mem::size_of_val(&members) as i32,
        )
    };
    if count < 0 || (count == 0 && nix::errno::Errno::last_raw() != 0) {
        return Err(io::Error::last_os_error());
    }
    Ok(count == 0 || (count == 1 && members[0] == pid))
}

struct OwnedProcess {
    child: Option<Child>,
}

impl OwnedProcess {
    fn has_exited(&mut self) -> io::Result<bool> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("process is unavailable"))?;
        #[cfg(unix)]
        {
            exited_without_reaping(child)
        }
        #[cfg(not(unix))]
        {
            child.try_wait().map(|status| status.is_some())
        }
    }

    fn finish(&mut self) -> io::Result<Option<ExitStatus>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        #[cfg(unix)]
        {
            use nix::sys::signal::{Signal, killpg};
            use nix::unistd::Pid;
            let pid = i32::try_from(child.id()).map_err(io::Error::other)?;
            // Never reap the leader before this signal. Even a naturally exited
            // leader retains ownership of its PID until the following wait.
            match killpg(Pid::from_raw(pid), Signal::SIGKILL) {
                Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                #[cfg(target_os = "macos")]
                Err(nix::errno::Errno::EPERM)
                    if exited_without_reaping(child)? && group_has_no_other_members(pid)? => {}
                Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
        #[cfg(not(unix))]
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        let status = child.wait()?;
        self.child = None;
        Ok(Some(status))
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::error!(%error, "child process cleanup failed");
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn shell(script: &str) -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", script]);
        command
    }

    #[test]
    fn large_output_does_not_deadlock_or_lose_stderr() {
        let command = shell("head -c 262144 /dev/zero; head -c 262144 /dev/zero >&2");
        let output = run(command, Duration::from_secs(5)).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 262144);
        assert_eq!(output.stderr.len(), 262144);
    }

    #[test]
    fn excessive_output_is_not_returned_as_valid_evidence() {
        let command = shell("head -c 16777217 /dev/zero");
        match run(command, Duration::from_secs(5)) {
            Err(ProcessError::OutputLimit) => {}
            Err(error) => panic!("unexpected process failure: {error}"),
            Ok(output) => panic!(
                "unexpected completion: status={:?}, stdout_len={}, stderr={}",
                output.status.code(),
                output.stdout.len(),
                String::from_utf8_lossy(&output.stderr)
            ),
        }
    }

    #[test]
    fn process_is_noninteractive_and_preserves_nonzero_status() {
        let command = shell(
            "test \"$GH_PROMPT_DISABLED\" = 1 || exit 10; test \"$GIT_TERMINAL_PROMPT\" = 0 || exit 11; test \"$GH_PAGER\" = cat || exit 12; if read line; then exit 13; fi; printf evidence; printf pending >&2; exit 8",
        );
        let output = run(command, Duration::from_secs(5)).unwrap();
        assert_eq!(output.status.code(), Some(8));
        assert_eq!(output.stdout, b"evidence");
        assert_eq!(output.stderr, b"pending");
    }

    #[test]
    fn natural_exit_keeps_leader_waitable_until_group_cleanup() {
        let mut capture = CapturedProcess::start(shell("exit 7")).unwrap();
        let pid = capture.process.child.as_ref().unwrap().id() as i32;
        while !capture.poll(Duration::from_secs(5)).unwrap() {
            std::thread::sleep(POLL_INTERVAL);
        }
        assert!(
            exited_without_reaping(capture.process.child.as_ref().unwrap()).unwrap(),
            "completion observation must leave the leader waitable and its PID owned"
        );
        let output = capture.finish(Ok(())).unwrap();
        assert_eq!(output.status.code(), Some(7));
        assert_eq!(
            nix::sys::wait::waitpid(
                nix::unistd::Pid::from_raw(pid),
                Some(nix::sys::wait::WaitPidFlag::WNOHANG)
            ),
            Err(nix::errno::Errno::ECHILD)
        );
    }

    #[test]
    fn timeout_reaps_leader_and_kills_term_ignoring_descendants() {
        let temp = tempfile::tempdir().unwrap();
        let pid_path = temp.path().join("leader.pid");
        let effect = temp.path().join("late-effect");
        let mut command = shell(
            "printf '%s' \"$$\" > \"$1\"; sh -c 'trap \"\" TERM; sleep 1; printf leaked > \"$1\"' child \"$2\" & wait",
        );
        command.arg("leader").arg(&pid_path).arg(&effect);
        let result = run(command, Duration::from_millis(150));
        assert!(matches!(result, Err(ProcessError::Timeout)), "{result:?}");
        let pid: i32 = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
        assert_eq!(
            nix::sys::wait::waitpid(
                nix::unistd::Pid::from_raw(pid),
                Some(nix::sys::wait::WaitPidFlag::WNOHANG)
            ),
            Err(nix::errno::Errno::ECHILD),
            "the owned leader must be reaped before returning"
        );
        std::thread::sleep(Duration::from_millis(1100));
        assert!(
            !effect.exists(),
            "an abandoned helper performed a late mutation"
        );
    }

    #[test]
    fn successful_leader_cannot_leave_a_background_helper_running() {
        let temp = tempfile::tempdir().unwrap();
        let effect = temp.path().join("late-effect");
        let mut command =
            shell("sh -c 'sleep 1; printf leaked > \"$1\"' child \"$1\" & printf complete");
        command.arg("leader").arg(&effect);
        let started = Instant::now();
        let output = run(command, Duration::from_secs(5)).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"complete");
        assert!(started.elapsed() < Duration::from_millis(850));
        std::thread::sleep(Duration::from_millis(1100));
        assert!(
            !effect.exists(),
            "a helper outlived the completed forge command"
        );
    }

    #[test]
    fn failed_spawn_never_claims_an_uncertain_remote_effect() {
        let error = run(
            Command::new("/nonexistent/fugue-test-command"),
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(error, ProcessError::Spawn(_)));
        assert!(!error.may_have_executed());
    }
}
