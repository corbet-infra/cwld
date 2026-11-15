//! A guest agent that reaches the guest through a command.
//!
//! The macOS build clone is reached today by `vm-dev <clone> pipe -- <command>`
//! (one ssh session). This adapter runs `<transport...> <program> <args...>` as a
//! child process: the request's standard input is written once and closed (the
//! registration travels there, never in an argument list), output chunks are
//! pushed to the sink as they arrive, and a [`ControlFlow::Break`] from the sink
//! kills the session. It implements only what a CI job needs; the rest of
//! [`GuestAgent`] answers that the transport has no such channel.

use std::io::{Read, Write};
use std::ops::ControlFlow;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Instant;

use cdrv::{
    ExecChunk, ExecOutcome, ExecResult, ExecStream, GuestAgent, GuestExec, GuestHealth,
    ShutdownMode,
};

/// The guest reached through a command.
#[derive(Debug, Clone)]
pub struct CommandGuest {
    /// The transport command and its fixed arguments (`["vm-dev", "mac-0", "pipe", "--"]`).
    pub transport: Vec<String>,
}

fn pump(mut stream: impl Read + Send + 'static, kind: ExecStream, tx: mpsc::Sender<ExecChunk>) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        while let Ok(n) = stream.read(&mut buf) {
            if n == 0
                || tx
                    .send(ExecChunk {
                        stream: kind,
                        bytes: buf[..n].to_vec(),
                    })
                    .is_err()
            {
                break;
            }
        }
    });
}

impl GuestAgent for CommandGuest {
    fn health(&self) -> cdrv::Result<GuestHealth> {
        Err("a command transport has no health channel".into())
    }

    fn shutdown(&self, _mode: ShutdownMode) -> cdrv::Result<()> {
        Err("a command transport has no shutdown channel".into())
    }

    fn exec(&self, req: &GuestExec) -> cdrv::Result<ExecResult> {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let outcome = self.exec_stream(req, &mut |chunk| {
            match chunk.stream {
                ExecStream::Stdout => stdout.extend_from_slice(&chunk.bytes),
                ExecStream::Stderr => stderr.extend_from_slice(&chunk.bytes),
            }
            ControlFlow::Continue(())
        })?;
        match outcome {
            ExecOutcome::Exited(status) => Ok(ExecResult {
                status,
                stdout,
                stderr,
            }),
            other => Err(format!("the command did not finish: {other:?}").into()),
        }
    }

    fn exec_stream(
        &self,
        req: &GuestExec,
        sink: &mut dyn FnMut(ExecChunk) -> ControlFlow<()>,
    ) -> cdrv::Result<ExecOutcome> {
        let (first, rest) = self
            .transport
            .split_first()
            .map_or((req.program.as_str(), &[][..]), |(f, r)| (f.as_str(), r));
        let mut cmd = Command::new(first);
        cmd.args(rest);
        if !self.transport.is_empty() {
            cmd.arg(&req.program);
        }
        let mut child = cmd
            .args(&req.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let (tx, rx) = mpsc::channel();
        if let Some(out) = child.stdout.take() {
            pump(out, ExecStream::Stdout, tx.clone());
        }
        if let Some(err) = child.stderr.take() {
            pump(err, ExecStream::Stderr, tx.clone());
        }
        drop(tx);
        // written once, then closed; a thread, so a full pipe never blocks the output loop
        if let Some(mut stdin) = child.stdin.take() {
            let bytes = req.stdin.clone().unwrap_or_default();
            std::thread::spawn(move || {
                let _ = stdin.write_all(&bytes);
            });
        }
        let deadline = Instant::now() + req.timeout;
        loop {
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(chunk) => {
                    if sink(chunk).is_break() {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Ok(ExecOutcome::Cancelled);
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Ok(ExecOutcome::TimedOut);
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        let status = child.wait()?;
        Ok(ExecOutcome::Exited(status.code().unwrap_or(-1)))
    }

    fn free_page_reporting(&self, _enable: bool) -> cdrv::Result<()> {
        Err("a command transport has no free-page-reporting channel".into())
    }
}
