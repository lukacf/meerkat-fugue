//! Bounded execution for the synchronous forge adapter. Capturing into private
//! files avoids pipe backpressure and descendants keeping an output pipe open.

use std::fs::File;
use std::io::{self, Read, Seek};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const OUTPUT_LIMIT: u64 = 16 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessError {
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
    pub(super) fn may_have_executed(&self) -> bool {
        !matches!(self, Self::Prepare(_) | Self::Spawn(_))
    }
}

pub(super) fn run(mut command: Command, timeout: Duration) -> Result<Output, ProcessError> {
    let mut stdout = tempfile::tempfile().map_err(ProcessError::Prepare)?;
    let mut stderr = tempfile::tempfile().map_err(ProcessError::Prepare)?;
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
    let mut process = OwnedProcess { child: Some(child) };
    let result = (|| {
        let started = Instant::now();
        loop {
            let captured = stdout.metadata().map_err(ProcessError::Observe)?.len()
                + stderr.metadata().map_err(ProcessError::Observe)?.len();
            if captured > OUTPUT_LIMIT {
                return Err(ProcessError::OutputLimit);
            }
            if let Some(status) = process
                .child
                .as_mut()
                .ok_or_else(|| ProcessError::Observe(io::Error::other("process is unavailable")))?
                .try_wait()
                .map_err(ProcessError::Observe)?
            {
                return Ok(status);
            }
            if started.elapsed() >= timeout {
                return Err(ProcessError::Timeout);
            }
            std::thread::sleep(POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed())));
        }
    })();
    // Always terminate remaining group members, including a helper that outlived
    // a successful leader. Reap our direct child before releasing the review lock.
    process.finish().map_err(ProcessError::Cleanup)?;
    let status = result?;
    let stdout = read_capture(&mut stdout)?;
    let stderr = read_capture(&mut stderr)?;
    if (stdout.len() + stderr.len()) as u64 > OUTPUT_LIMIT {
        return Err(ProcessError::OutputLimit);
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
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

struct OwnedProcess {
    child: Option<Child>,
}

impl OwnedProcess {
    fn finish(&mut self) -> io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        // The output limit can stop observation before the ordinary try_wait.
        // Reap an already exited leader before signalling its remaining group.
        let status = child.try_wait()?;
        #[cfg(unix)]
        {
            let _ = status;
            use nix::sys::signal::{Signal, killpg};
            use nix::unistd::Pid;
            let pid = i32::try_from(child.id()).map_err(io::Error::other)?;
            match killpg(Pid::from_raw(pid), Signal::SIGKILL) {
                Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
        #[cfg(not(unix))]
        if status.is_none() {
            child.kill()?;
        }
        child.wait()?;
        self.child = None;
        Ok(())
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::error!(%error, "forge process cleanup failed");
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
