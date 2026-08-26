use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, ErrorCode, OptionalExtension, TransactionBehavior};

use crate::api::schema::{AgentQueueReceipt, AgentQueueState};

const SCHEMA_VERSION: i64 = 1;
const RECEIPT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreErrorKind {
    Busy,
    Full,
    Corrupt,
    Unavailable,
}

impl StoreErrorKind {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Busy => "store_busy",
            Self::Full => "store_full",
            Self::Corrupt => "store_corrupt",
            Self::Unavailable => "store_unavailable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreError {
    pub(crate) kind: StoreErrorKind,
    pub(crate) message: String,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StoreError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueueError {
    Store(StoreError),
    NotFound,
    IdempotencyConflict(AgentQueueReceipt),
    AlreadySubmitted(AgentQueueReceipt),
    InvalidTransition(AgentQueueReceipt),
}

impl From<StoreError> for QueueError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnqueueOutcome {
    pub(crate) receipt: AgentQueueReceipt,
    pub(crate) duplicate: bool,
}

#[derive(Debug)]
pub(crate) enum QueueRuntime {
    Available(QueueStore),
    Unavailable(StoreError),
}

impl QueueRuntime {
    pub(crate) fn open(path: &Path) -> Self {
        match QueueStore::open(path) {
            Ok(store) => Self::Available(store),
            Err(err) => {
                tracing::error!(
                    code = err.kind.code(),
                    error = %err,
                    path = %path.display(),
                    "durable agent queue unavailable"
                );
                Self::Unavailable(err)
            }
        }
    }

    pub(crate) fn in_memory() -> Self {
        match QueueStore::open_in_memory() {
            Ok(store) => Self::Available(store),
            Err(err) => Self::Unavailable(err),
        }
    }

    pub(crate) fn store_mut(&mut self) -> Result<&mut QueueStore, StoreError> {
        match self {
            Self::Available(store) => Ok(store),
            Self::Unavailable(err) => Err(err.clone()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct QueueStore {
    connection: Connection,
    #[allow(dead_code)]
    path: Option<PathBuf>,
}

impl QueueStore {
    pub(crate) fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| StoreError {
                kind: if err.raw_os_error() == Some(28) {
                    StoreErrorKind::Full
                } else {
                    StoreErrorKind::Unavailable
                },
                message: format!("failed to create durable queue directory: {err}"),
            })?;
        }
        let connection = Connection::open(path).map_err(classify_sqlite_error)?;
        let mut store = Self {
            connection,
            path: Some(path.to_path_buf()),
        };
        store.configure(true)?;
        store.initialize()?;
        store.integrity_check()?;
        store.recover_interrupted_attempts()?;
        Ok(store)
    }

    fn open_in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory().map_err(classify_sqlite_error)?;
        let mut store = Self {
            connection,
            path: None,
        };
        store.configure(false)?;
        store.initialize()?;
        Ok(store)
    }

    fn configure(&self, require_wal: bool) -> Result<(), StoreError> {
        self.connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(classify_sqlite_error)?;
        self.connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(classify_sqlite_error)?;
        let journal_mode = if require_wal {
            self.connection
                .query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))
                .map_err(classify_sqlite_error)?
        } else {
            self.connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(classify_sqlite_error)?
        };
        if require_wal && !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError {
                kind: StoreErrorKind::Unavailable,
                message: format!("durable queue refused non-WAL journal mode {journal_mode}"),
            });
        }
        self.connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(classify_sqlite_error)?;
        let synchronous: i64 = self
            .connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .map_err(classify_sqlite_error)?;
        if synchronous < 2 {
            return Err(StoreError {
                kind: StoreErrorKind::Unavailable,
                message: "durable queue refused synchronous mode below FULL".into(),
            });
        }
        Ok(())
    }

    fn initialize(&mut self) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_sqlite_error)?;
        transaction
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS agent_queue (
                    enqueue_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_id TEXT UNIQUE NOT NULL,
                    instance_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    text TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN (
                        'queued','blocked','attempting','submitted','ambiguous',
                        'acknowledged','canceled','orphaned'
                    )),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                    block_reason TEXT,
                    last_error TEXT,
                    pty_epoch TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    UNIQUE(instance_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS agent_queue_fifo
                    ON agent_queue(instance_id, enqueue_seq);
                CREATE UNIQUE INDEX IF NOT EXISTS agent_queue_one_inflight
                    ON agent_queue(instance_id)
                    WHERE state IN ('attempting','submitted','ambiguous');
                CREATE TABLE IF NOT EXISTS agent_queue_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_id TEXT NOT NULL,
                    instance_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(queue_id) REFERENCES agent_queue(queue_id)
                );",
            )
            .map_err(classify_sqlite_error)?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(classify_sqlite_error)?;
        transaction.commit().map_err(classify_sqlite_error)
    }

    fn integrity_check(&self) -> Result<(), StoreError> {
        let result: String = self
            .connection
            .pragma_query_value(None, "quick_check", |row| row.get(0))
            .map_err(classify_sqlite_error)?;
        if result == "ok" {
            Ok(())
        } else {
            Err(StoreError {
                kind: StoreErrorKind::Corrupt,
                message: format!("durable queue integrity check failed: {result}"),
            })
        }
    }

    fn recover_interrupted_attempts(&mut self) -> Result<(), StoreError> {
        let now = now_ms();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_sqlite_error)?;
        let mut statement = transaction
            .prepare("SELECT queue_id, instance_id FROM agent_queue WHERE state = 'attempting'")
            .map_err(classify_sqlite_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(classify_sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(classify_sqlite_error)?;
        drop(statement);
        for (queue_id, instance_id) in rows {
            transaction
                .execute(
                    "UPDATE agent_queue SET state='ambiguous', last_error='server_restarted_during_submission', updated_at_ms=?1 WHERE queue_id=?2",
                    params![now, queue_id],
                )
                .map_err(classify_sqlite_error)?;
            insert_event(
                &transaction,
                &queue_id,
                &instance_id,
                AgentQueueState::Ambiguous,
                now,
            )?;
        }
        transaction.commit().map_err(classify_sqlite_error)
    }

    pub(crate) fn enqueue(
        &mut self,
        instance_id: &str,
        idempotency_key: &str,
        text: &str,
    ) -> Result<EnqueueOutcome, QueueError> {
        let now = now_ms();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_sqlite_error)?;
        let existing = query_by_key(&transaction, instance_id, idempotency_key)?;
        if let Some((receipt, existing_text)) = existing {
            if existing_text != text {
                return Err(QueueError::IdempotencyConflict(receipt));
            }
            return Ok(EnqueueOutcome {
                receipt,
                duplicate: true,
            });
        }
        let queue_id = uuid::Uuid::new_v4().to_string();
        transaction
            .execute(
                "INSERT INTO agent_queue (
                    queue_id, instance_id, idempotency_key, text, state,
                    attempts, created_at_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, 'queued', 0, ?5, ?5)",
                params![queue_id, instance_id, idempotency_key, text, now],
            )
            .map_err(classify_sqlite_error)?;
        insert_event(
            &transaction,
            &queue_id,
            instance_id,
            AgentQueueState::Queued,
            now,
        )?;
        let receipt = query_by_id(&transaction, &queue_id)?.ok_or(QueueError::NotFound)?;
        transaction.commit().map_err(classify_sqlite_error)?;
        Ok(EnqueueOutcome {
            receipt,
            duplicate: false,
        })
    }

    pub(crate) fn get(&self, queue_id: &str) -> Result<AgentQueueReceipt, QueueError> {
        query_by_id(&self.connection, queue_id)?.ok_or(QueueError::NotFound)
    }

    pub(crate) fn list(
        &self,
        instance_id: Option<&str>,
        state: Option<AgentQueueState>,
    ) -> Result<Vec<AgentQueueReceipt>, QueueError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT queue_id, instance_id, idempotency_key, state, attempts,
                        block_reason, last_error, pty_epoch, created_at_ms, updated_at_ms
                 FROM agent_queue
                 WHERE (?1 IS NULL OR instance_id = ?1)
                   AND (?2 IS NULL OR state = ?2)
                 ORDER BY enqueue_seq",
            )
            .map_err(classify_sqlite_error)?;
        let rows = statement
            .query_map(
                params![instance_id, state.map(AgentQueueState::as_str)],
                receipt_from_row,
            )
            .map_err(classify_sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| QueueError::Store(classify_sqlite_error(err)))?;
        Ok(rows)
    }

    pub(crate) fn cancel(&mut self, queue_id: &str) -> Result<AgentQueueReceipt, QueueError> {
        self.transition_terminal(queue_id, AgentQueueState::Canceled, |state| {
            matches!(state, AgentQueueState::Queued | AgentQueueState::Blocked)
        })
    }

    pub(crate) fn acknowledge(&mut self, queue_id: &str) -> Result<AgentQueueReceipt, QueueError> {
        let current = self.get(queue_id)?;
        if current.state == AgentQueueState::Acknowledged {
            return Ok(current);
        }
        if matches!(
            current.state,
            AgentQueueState::Queued | AgentQueueState::Blocked
        ) {
            return Err(QueueError::InvalidTransition(current));
        }
        if matches!(
            current.state,
            AgentQueueState::Canceled | AgentQueueState::Orphaned
        ) {
            return Err(QueueError::InvalidTransition(current));
        }
        self.transition_terminal(queue_id, AgentQueueState::Acknowledged, |state| {
            matches!(
                state,
                AgentQueueState::Attempting
                    | AgentQueueState::Submitted
                    | AgentQueueState::Ambiguous
            )
        })
    }

    fn transition_terminal<F>(
        &mut self,
        queue_id: &str,
        target: AgentQueueState,
        allowed: F,
    ) -> Result<AgentQueueReceipt, QueueError>
    where
        F: FnOnce(AgentQueueState) -> bool,
    {
        let now = now_ms();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_sqlite_error)?;
        let current = query_by_id(&transaction, queue_id)?.ok_or(QueueError::NotFound)?;
        if current.state == target {
            return Ok(current);
        }
        if !allowed(current.state) {
            return if matches!(
                current.state,
                AgentQueueState::Attempting
                    | AgentQueueState::Submitted
                    | AgentQueueState::Ambiguous
                    | AgentQueueState::Acknowledged
            ) {
                Err(QueueError::AlreadySubmitted(current))
            } else {
                Err(QueueError::InvalidTransition(current))
            };
        }
        transaction
            .execute(
                "UPDATE agent_queue SET state=?1, block_reason=NULL, updated_at_ms=?2 WHERE queue_id=?3",
                params![target.as_str(), now, queue_id],
            )
            .map_err(classify_sqlite_error)?;
        insert_event(&transaction, queue_id, &current.instance_id, target, now)?;
        let receipt = query_by_id(&transaction, queue_id)?.ok_or(QueueError::NotFound)?;
        transaction.commit().map_err(classify_sqlite_error)?;
        Ok(receipt)
    }

    pub(crate) fn set_blocked(
        &mut self,
        queue_id: &str,
        reason: &str,
    ) -> Result<AgentQueueReceipt, QueueError> {
        let current = self.get(queue_id)?;
        if !matches!(
            current.state,
            AgentQueueState::Queued | AgentQueueState::Blocked
        ) {
            return Err(QueueError::InvalidTransition(current));
        }
        if current.state == AgentQueueState::Blocked
            && current.block_reason.as_deref() == Some(reason)
        {
            return Ok(current);
        }
        self.update_state(
            queue_id,
            AgentQueueState::Blocked,
            Some(reason),
            None,
            None,
            false,
        )
    }

    pub(crate) fn claim_next(
        &mut self,
        instance_id: &str,
    ) -> Result<Option<(AgentQueueReceipt, String)>, QueueError> {
        let now = now_ms();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_sqlite_error)?;
        let inflight: Option<String> = transaction
            .query_row(
                "SELECT state FROM agent_queue WHERE instance_id=?1 AND state IN ('attempting','submitted') LIMIT 1",
                [instance_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(classify_sqlite_error)?;
        if inflight.is_some() {
            return Ok(None);
        }
        let next: Option<(String, String)> = transaction
            .query_row(
                "SELECT queue_id, text FROM agent_queue
                 WHERE instance_id=?1 AND state IN ('queued','blocked','ambiguous')
                 ORDER BY enqueue_seq LIMIT 1",
                [instance_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(classify_sqlite_error)?;
        let Some((queue_id, text)) = next else {
            return Ok(None);
        };
        transaction
            .execute(
                "UPDATE agent_queue SET state='attempting', attempts=attempts+1,
                        block_reason=NULL, last_error=NULL, updated_at_ms=?1
                 WHERE queue_id=?2",
                params![now, queue_id],
            )
            .map_err(classify_sqlite_error)?;
        insert_event(
            &transaction,
            &queue_id,
            instance_id,
            AgentQueueState::Attempting,
            now,
        )?;
        let receipt = query_by_id(&transaction, &queue_id)?.ok_or(QueueError::NotFound)?;
        transaction.commit().map_err(classify_sqlite_error)?;
        Ok(Some((receipt, text)))
    }

    pub(crate) fn mark_submitted(
        &mut self,
        queue_id: &str,
        pty_epoch: &str,
    ) -> Result<AgentQueueReceipt, QueueError> {
        self.update_state(
            queue_id,
            AgentQueueState::Submitted,
            None,
            None,
            Some(pty_epoch),
            false,
        )
    }

    pub(crate) fn mark_ambiguous(
        &mut self,
        queue_id: &str,
        error: &str,
        pty_epoch: Option<&str>,
    ) -> Result<AgentQueueReceipt, QueueError> {
        self.update_state(
            queue_id,
            AgentQueueState::Ambiguous,
            None,
            Some(error),
            pty_epoch,
            false,
        )
    }

    fn update_state(
        &mut self,
        queue_id: &str,
        target: AgentQueueState,
        block_reason: Option<&str>,
        last_error: Option<&str>,
        pty_epoch: Option<&str>,
        increment_attempts: bool,
    ) -> Result<AgentQueueReceipt, QueueError> {
        let now = now_ms();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_sqlite_error)?;
        let current = query_by_id(&transaction, queue_id)?.ok_or(QueueError::NotFound)?;
        let allowed = match target {
            AgentQueueState::Blocked => {
                matches!(
                    current.state,
                    AgentQueueState::Queued | AgentQueueState::Blocked
                )
            }
            AgentQueueState::Submitted | AgentQueueState::Ambiguous => {
                current.state == AgentQueueState::Attempting
            }
            _ => false,
        };
        if !allowed {
            return Err(QueueError::InvalidTransition(current));
        }
        transaction
            .execute(
                "UPDATE agent_queue
                 SET state=?1, block_reason=?2, last_error=?3,
                     pty_epoch=COALESCE(?4, pty_epoch),
                     attempts=attempts + ?5, updated_at_ms=?6
                 WHERE queue_id=?7",
                params![
                    target.as_str(),
                    block_reason,
                    last_error,
                    pty_epoch,
                    i64::from(increment_attempts),
                    now,
                    queue_id
                ],
            )
            .map_err(classify_sqlite_error)?;
        insert_event(&transaction, queue_id, &current.instance_id, target, now)?;
        let receipt = query_by_id(&transaction, queue_id)?.ok_or(QueueError::NotFound)?;
        transaction.commit().map_err(classify_sqlite_error)?;
        Ok(receipt)
    }

    pub(crate) fn orphan_missing_instances(
        &mut self,
        known_instances: &HashSet<String>,
    ) -> Result<Vec<AgentQueueReceipt>, QueueError> {
        let candidates = self.list(None, None)?;
        let mut orphaned = Vec::new();
        for receipt in candidates.into_iter().filter(|receipt| {
            !receipt.state.is_terminal() && !known_instances.contains(&receipt.instance_id)
        }) {
            let now = now_ms();
            let transaction = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(classify_sqlite_error)?;
            transaction
                .execute(
                    "UPDATE agent_queue SET state='orphaned', block_reason=NULL,
                            last_error='agent_instance_replaced_or_closed', updated_at_ms=?1
                     WHERE queue_id=?2 AND state NOT IN ('acknowledged','canceled','orphaned')",
                    params![now, receipt.queue_id],
                )
                .map_err(classify_sqlite_error)?;
            insert_event(
                &transaction,
                &receipt.queue_id,
                &receipt.instance_id,
                AgentQueueState::Orphaned,
                now,
            )?;
            let updated =
                query_by_id(&transaction, &receipt.queue_id)?.ok_or(QueueError::NotFound)?;
            transaction.commit().map_err(classify_sqlite_error)?;
            orphaned.push(updated);
        }
        Ok(orphaned)
    }

    pub(crate) fn event_count(&self) -> Result<u64, StoreError> {
        self.connection
            .query_row("SELECT COUNT(*) FROM agent_queue_events", [], |row| {
                row.get(0)
            })
            .map_err(classify_sqlite_error)
    }
}

fn query_by_id(
    connection: &Connection,
    queue_id: &str,
) -> Result<Option<AgentQueueReceipt>, QueueError> {
    connection
        .query_row(
            "SELECT queue_id, instance_id, idempotency_key, state, attempts,
                    block_reason, last_error, pty_epoch, created_at_ms, updated_at_ms
             FROM agent_queue WHERE queue_id=?1",
            [queue_id],
            receipt_from_row,
        )
        .optional()
        .map_err(|err| QueueError::Store(classify_sqlite_error(err)))
}

fn query_by_key(
    connection: &Connection,
    instance_id: &str,
    idempotency_key: &str,
) -> Result<Option<(AgentQueueReceipt, String)>, QueueError> {
    connection
        .query_row(
            "SELECT queue_id, instance_id, idempotency_key, state, attempts,
                    block_reason, last_error, pty_epoch, created_at_ms, updated_at_ms, text
             FROM agent_queue WHERE instance_id=?1 AND idempotency_key=?2",
            params![instance_id, idempotency_key],
            |row| Ok((receipt_from_row(row)?, row.get(10)?)),
        )
        .optional()
        .map_err(|err| QueueError::Store(classify_sqlite_error(err)))
}

fn receipt_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentQueueReceipt> {
    let state: String = row.get(3)?;
    let state = AgentQueueState::parse(&state).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid queue state {state}"),
            )),
        )
    })?;
    let attempts: i64 = row.get(4)?;
    Ok(AgentQueueReceipt {
        version: RECEIPT_VERSION,
        queue_id: row.get(0)?,
        instance_id: row.get(1)?,
        idempotency_key: row.get(2)?,
        state,
        attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
        block_reason: row.get(5)?,
        last_error: row.get(6)?,
        pty_epoch: row.get(7)?,
        created_at: format_timestamp(row.get(8)?),
        updated_at: format_timestamp(row.get(9)?),
    })
}

fn insert_event(
    transaction: &rusqlite::Transaction<'_>,
    queue_id: &str,
    instance_id: &str,
    state: AgentQueueState,
    created_at_ms: i64,
) -> Result<(), StoreError> {
    transaction
        .execute(
            "INSERT INTO agent_queue_events(queue_id, instance_id, state, created_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![queue_id, instance_id, state.as_str(), created_at_ms],
        )
        .map(|_| ())
        .map_err(classify_sqlite_error)
}

fn now_ms() -> i64 {
    let millis = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn format_timestamp(timestamp_ms: i64) -> String {
    let nanos = i128::from(timestamp_ms) * 1_000_000;
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|timestamp| {
            timestamp
                .format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn classify_sqlite_error(error: rusqlite::Error) -> StoreError {
    let kind = match &error {
        rusqlite::Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => StoreErrorKind::Busy,
            ErrorCode::DiskFull => StoreErrorKind::Full,
            ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase => StoreErrorKind::Corrupt,
            _ => StoreErrorKind::Unavailable,
        },
        _ => StoreErrorKind::Unavailable,
    };
    StoreError {
        kind,
        message: format!("durable queue database error: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("herdr-queue-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn recipient_scoped_idempotency_and_text_conflict() {
        let mut store = QueueStore::open_in_memory().unwrap();
        let first = store
            .enqueue("instance-a", "event-1", "pointer one")
            .unwrap();
        let duplicate = store
            .enqueue("instance-a", "event-1", "pointer one")
            .unwrap();
        let other = store
            .enqueue("instance-b", "event-1", "pointer one")
            .unwrap();

        assert!(!first.duplicate);
        assert!(duplicate.duplicate);
        assert_eq!(first.receipt.queue_id, duplicate.receipt.queue_id);
        assert_ne!(first.receipt.queue_id, other.receipt.queue_id);
        assert!(matches!(
            store.enqueue("instance-a", "event-1", "different text"),
            Err(QueueError::IdempotencyConflict(receipt)) if receipt.queue_id == first.receipt.queue_id
        ));
    }

    #[test]
    fn fifo_and_one_inflight_are_per_instance() {
        let mut store = QueueStore::open_in_memory().unwrap();
        let a1 = store.enqueue("a", "1", "a1").unwrap().receipt;
        let a2 = store.enqueue("a", "2", "a2").unwrap().receipt;
        let b1 = store.enqueue("b", "1", "b1").unwrap().receipt;

        let (claimed_a, text_a) = store.claim_next("a").unwrap().unwrap();
        let (claimed_b, text_b) = store.claim_next("b").unwrap().unwrap();
        assert_eq!(
            (claimed_a.queue_id.as_str(), text_a.as_str()),
            (a1.queue_id.as_str(), "a1")
        );
        assert_eq!(
            (claimed_b.queue_id.as_str(), text_b.as_str()),
            (b1.queue_id.as_str(), "b1")
        );
        assert!(store.claim_next("a").unwrap().is_none());

        store.mark_submitted(&a1.queue_id, "epoch-a").unwrap();
        assert!(store.claim_next("a").unwrap().is_none());
        store.acknowledge(&a1.queue_id).unwrap();
        assert_eq!(
            store.claim_next("a").unwrap().unwrap().0.queue_id,
            a2.queue_id
        );
    }

    #[test]
    fn cancel_is_guaranteed_only_before_submission() {
        let mut store = QueueStore::open_in_memory().unwrap();
        let queued = store.enqueue("a", "1", "a1").unwrap().receipt;
        assert_eq!(
            store.cancel(&queued.queue_id).unwrap().state,
            AgentQueueState::Canceled
        );

        let attempting = store.enqueue("a", "2", "a2").unwrap().receipt;
        store.claim_next("a").unwrap().unwrap();
        assert!(matches!(
            store.cancel(&attempting.queue_id),
            Err(QueueError::AlreadySubmitted(_))
        ));
    }

    #[test]
    fn interrupted_attempt_is_ambiguous_after_reopen() {
        let directory = TestDirectory::new();
        let path = directory.path().join("queue.sqlite3");
        let queue_id = {
            let mut store = QueueStore::open(&path).unwrap();
            let receipt = store.enqueue("a", "1", "pointer").unwrap().receipt;
            store.claim_next("a").unwrap().unwrap();
            receipt.queue_id
        };

        let store = QueueStore::open(&path).unwrap();
        let recovered = store.get(&queue_id).unwrap();
        assert_eq!(recovered.state, AgentQueueState::Ambiguous);
        assert_eq!(recovered.attempts, 1);
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("server_restarted_during_submission")
        );
    }

    #[test]
    fn ten_thousand_rows_across_twenty_instances_are_complete_and_ordered() {
        let mut store = QueueStore::open_in_memory().unwrap();
        for sequence in 0..10_000_u32 {
            let instance = format!("instance-{}", sequence % 20);
            store
                .enqueue(
                    &instance,
                    &format!("key-{sequence}"),
                    &format!("pointer-{sequence}"),
                )
                .unwrap();
        }

        let all = store.list(None, None).unwrap();
        assert_eq!(all.len(), 10_000);
        for instance in 0..20 {
            let rows = store
                .list(Some(&format!("instance-{instance}")), None)
                .unwrap();
            assert_eq!(rows.len(), 500);
            let sequence_numbers = rows
                .iter()
                .map(|row| {
                    row.idempotency_key
                        .strip_prefix("key-")
                        .unwrap()
                        .parse::<u32>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert!(sequence_numbers.windows(2).all(|pair| pair[0] < pair[1]));
        }
        assert_eq!(store.event_count().unwrap(), 10_000);
    }

    #[test]
    fn corrupt_database_is_refused_without_replacement() {
        let directory = TestDirectory::new();
        let path = directory.path().join("queue.sqlite3");
        std::fs::write(&path, b"not a sqlite database").unwrap();
        let original = std::fs::read(&path).unwrap();

        let error = QueueStore::open(&path).unwrap_err();
        assert_eq!(error.kind, StoreErrorKind::Corrupt);
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }

    #[test]
    fn orphaning_preserves_terminal_tombstones() {
        let mut store = QueueStore::open_in_memory().unwrap();
        let missing = store.enqueue("missing", "1", "pointer").unwrap().receipt;
        let kept = store.enqueue("kept", "1", "pointer").unwrap().receipt;
        let known = HashSet::from(["kept".to_string()]);

        let orphaned = store.orphan_missing_instances(&known).unwrap();
        assert_eq!(orphaned.len(), 1);
        assert_eq!(orphaned[0].queue_id, missing.queue_id);
        assert_eq!(orphaned[0].state, AgentQueueState::Orphaned);
        assert_eq!(
            store.get(&kept.queue_id).unwrap().state,
            AgentQueueState::Queued
        );
        assert_eq!(
            store
                .enqueue("missing", "1", "pointer")
                .unwrap()
                .receipt
                .queue_id,
            missing.queue_id
        );
    }
}
