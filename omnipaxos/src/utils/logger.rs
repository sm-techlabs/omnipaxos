use crate::util::NodeId;
use slog::{Drain, Key, OwnedKVList, Record, Serializer, KV};
use slog_term::timestamp_local;
use std::{
    fs::OpenOptions,
    io::{self, Write},
    sync::{Arc, Mutex},
};

/// Shared handle that callers write to whenever their `(Role, Phase)` changes.
/// The `PrefixedDrain` reads it on every log line to keep the prefix current.
pub type StateLabel = Arc<Mutex<String>>;

// ANSI color codes assigned per node id so each replica's log lines are
// visually distinct.  Colors cycle through 6 standard foreground codes.
fn node_color(pid: NodeId) -> u8 {
    // 31=red 32=green 33=yellow 34=blue 35=magenta 36=cyan
    31 + ((pid as u8).wrapping_sub(1) % 6)
}

// Serialises key=value pairs into a flat byte buffer, skipping "node".
struct KvBuf(Vec<u8>);

impl Serializer for KvBuf {
    fn emit_arguments(&mut self, key: Key, val: &std::fmt::Arguments) -> slog::Result {
        if key != "node" {
            write!(self.0, " {}={}", key, val).ok();
        }
        Ok(())
    }
}

/// A drain that writes `[P<pid> <state>] <timestamp> <LEVEL> <message> <key=val…>`
/// to `W`.  The entire `[P<pid> state]` bracket is rendered in one ANSI color.
struct PrefixedDrain<W: Write + Send + 'static> {
    writer: Mutex<W>,
    pid: NodeId,
    state: StateLabel,
    color: u8,
}

impl<W: Write + Send + 'static> Drain for PrefixedDrain<W> {
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<(), slog::Never> {
        let mut buf: Vec<u8> = Vec::with_capacity(160);

        // Colored [P<pid> <state>] prefix — one escape sequence wraps everything
        let state = self
            .state
            .lock()
            .map(|g| g.clone())
            .unwrap_or_else(|_| "???".into());
        write!(buf, "\x1b[{}m[P{} {}]\x1b[0m ", self.color, self.pid, state).ok();

        // Timestamp via slog_term's local-time formatter
        timestamp_local(&mut buf).ok();

        // Level (5 chars, uppercase)
        write!(buf, " {:5} ", record.level().as_short_str().to_uppercase()).ok();

        // Message
        write!(buf, "{}", record.msg()).ok();

        // Key-value pairs from call-site and logger context
        let mut kv = KvBuf(Vec::new());
        record.kv().serialize(record, &mut kv).ok();
        values.serialize(record, &mut kv).ok();
        buf.extend_from_slice(&kv.0);

        buf.push(b'\n');

        if let Ok(mut w) = self.writer.lock() {
            let _ = w.write_all(&buf);
        }
        Ok(())
    }
}

/// Creates an asynchronous logger whose every line starts with a colored
/// `[P<pid> <role>/<phase>]` tag.
///
/// Returns `(logger, state_label)`.  The caller should write a short
/// `"Rol/Pha"` string into `state_label` whenever its `(Role, Phase)` changes
/// so that subsequent log lines reflect the new state automatically.
pub fn create_logger(file_path: &str, pid: NodeId) -> (slog::Logger, StateLabel) {
    let path = std::path::Path::new(file_path);
    let prefix = path.parent().unwrap();
    std::fs::create_dir_all(prefix).unwrap();

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .unwrap();

    let color = node_color(pid);
    let state: StateLabel = Arc::new(Mutex::new("---/---".to_string()));

    let term_drain = PrefixedDrain {
        writer: Mutex::new(io::stderr()),
        pid,
        state: Arc::clone(&state),
        color,
    };
    let file_drain = PrefixedDrain {
        writer: Mutex::new(file),
        pid,
        state: Arc::clone(&state),
        color,
    };

    let both = slog::Duplicate::new(term_drain, file_drain).fuse();
    let both = slog_async::Async::new(both).build().fuse();

    let logger = slog::Logger::root(both, slog::o!("node" => pid.to_string()));
    (logger, state)
}
