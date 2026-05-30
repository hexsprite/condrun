use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::process::{ChildHandle, CommandSpec, Spawner};
use crate::signal::SignalKind;

/// Real Tokio-backed process spawner. Creates each child in its own session
/// (via `setsid`) so that signals can be delivered to the entire process
/// group with `killpg`.
pub struct TokioSpawner;

#[async_trait]
impl Spawner for TokioSpawner {
    async fn spawn(&self, cmd: &CommandSpec) -> Result<Box<dyn ChildHandle>> {
        let mut command = ::tokio::process::Command::new(&cmd.program);
        command.args(&cmd.args);
        command.stdin(Stdio::null());
        command.stdout(Stdio::inherit());
        command.stderr(Stdio::inherit());
        // Defense in depth: if condrun panics or unwinds without going
        // through the normal shutdown path, drop on the Child handle sends
        // SIGKILL. Doesn't help against SIGKILL on condrun itself (kernel
        // can't forward), but does prevent leaks on panic.
        command.kill_on_drop(true);

        // SAFETY: `pre_exec` runs after fork() but before exec(). Calling
        // `setsid` here puts the child into a new session/process group so
        // that `killpg(pid, ...)` can target the entire group later.
        unsafe {
            command.pre_exec(|| {
                nix::unistd::setsid()
                    .map(|_| ())
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))
            });
        }

        let child = command.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| anyhow!("no PID for spawned child"))?;
        Ok(Box::new(TokioChildHandle { child, pid }))
    }
}

/// Handle to a spawned child plus the cached PID (which is also the pgid
/// after `setsid`).
pub struct TokioChildHandle {
    child: ::tokio::process::Child,
    pid: u32,
}

impl TokioChildHandle {
    fn pgid(&self) -> nix::unistd::Pid {
        nix::unistd::Pid::from_raw(self.pid as i32)
    }
}

#[async_trait]
impl ChildHandle for TokioChildHandle {
    async fn wait(&mut self) -> ExitStatus {
        self.child.wait().await.expect("wait failed")
    }

    async fn terminate(&mut self, grace: Duration) -> Result<()> {
        let pgid = self.pgid();
        // Best-effort SIGTERM to the process group. Ignore errors (e.g. ESRCH
        // if the child has already exited).
        let _ = nix::sys::signal::killpg(pgid, nix::sys::signal::Signal::SIGTERM);

        ::tokio::select! {
            _ = self.child.wait() => Ok(()),
            _ = ::tokio::time::sleep(grace) => {
                let _ = nix::sys::signal::killpg(pgid, nix::sys::signal::Signal::SIGKILL);
                let _ = self.child.wait().await;
                Ok(())
            }
        }
    }

    async fn signal(&mut self, kind: SignalKind) -> Result<()> {
        let sig = match kind {
            SignalKind::Interrupt => nix::sys::signal::Signal::SIGINT,
            SignalKind::Terminate => nix::sys::signal::Signal::SIGTERM,
            SignalKind::Quit => nix::sys::signal::Signal::SIGQUIT,
            SignalKind::Hup => nix::sys::signal::Signal::SIGHUP,
        };
        let _ = nix::sys::signal::killpg(self.pgid(), sig);
        Ok(())
    }

    fn pid(&self) -> u32 {
        self.pid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn spec(program: &str, args: &[&str]) -> CommandSpec {
        CommandSpec {
            program: program.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn spawn_echo() {
        let spawner = TokioSpawner;
        let mut child = spawner.spawn(&spec("echo", &["hello"])).await.unwrap();
        let status = child.wait().await;
        assert!(status.success(), "echo should exit 0, got {:?}", status);
    }

    #[tokio::test]
    async fn terminate_kills_child() {
        let spawner = TokioSpawner;
        let mut child = spawner.spawn(&spec("sleep", &["999"])).await.unwrap();
        // Give the child a beat to actually begin executing sleep before we
        // try to terminate it; otherwise the SIGTERM can race with exec().
        ::tokio::time::sleep(Duration::from_millis(50)).await;
        let start = Instant::now();
        child.terminate(Duration::from_secs(1)).await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "expected quick SIGTERM exit, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn sigkill_after_grace() {
        let spawner = TokioSpawner;
        let mut child = spawner
            .spawn(&spec("sh", &["-c", "trap '' TERM; sleep 999"]))
            .await
            .unwrap();
        ::tokio::time::sleep(Duration::from_millis(100)).await;
        let start = Instant::now();
        child.terminate(Duration::from_secs(1)).await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(1),
            "should have waited grace before SIGKILL, took {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "SIGKILL should have ended things quickly after grace, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn signal_forwarding_no_escalation() {
        let spawner = TokioSpawner;
        let mut child = spawner
            .spawn(&spec(
                "sh",
                &["-c", "trap 'echo got-int; exit 130' INT; sleep 999"],
            ))
            .await
            .unwrap();
        // Give the shell time to install the trap.
        ::tokio::time::sleep(Duration::from_millis(150)).await;
        let start = Instant::now();
        child.signal(SignalKind::Interrupt).await.unwrap();
        let status = child.wait().await;
        let elapsed = start.elapsed();
        assert_eq!(status.code(), Some(130), "expected exit 130 from INT trap");
        assert!(
            elapsed < Duration::from_secs(1),
            "raw signal forwarding should not escalate or wait grace, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn child_in_new_session() {
        let spawner = TokioSpawner;
        let mut child = spawner.spawn(&spec("sleep", &["30"])).await.unwrap();
        let pid = child.pid();
        // After setsid, the child's session id should equal its own PID
        // (it is the session leader of a new session).
        let sid = nix::unistd::getsid(Some(nix::unistd::Pid::from_raw(pid as i32)))
            .expect("getsid should succeed for a live child");
        assert_eq!(
            sid.as_raw() as u32,
            pid,
            "child should be its own session leader (sid == pid)"
        );
        // Also confirm the child's session differs from condrun's session.
        let our_sid = nix::unistd::getsid(None).unwrap();
        assert_ne!(
            sid.as_raw(),
            our_sid.as_raw(),
            "child session should differ from parent session"
        );
        child.terminate(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn nonexistent_command() {
        let spawner = TokioSpawner;
        let result = spawner
            .spawn(&spec("nonexistent-binary-xyz-condrun-test", &[]))
            .await;
        assert!(result.is_err(), "expected error for missing binary");
    }

    #[tokio::test]
    async fn exit_code_propagation() {
        let spawner = TokioSpawner;
        let mut child = spawner
            .spawn(&spec("sh", &["-c", "exit 42"]))
            .await
            .unwrap();
        let status = child.wait().await;
        assert_eq!(status.code(), Some(42), "should propagate exit 42");
    }
}
