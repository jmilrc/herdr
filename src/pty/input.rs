use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InputProvenance {
    Manual,
    PaneApi,
    Queue(String),
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputKind {
    Content,
    TopLevelEnter,
    Control,
}

#[derive(Debug, Clone)]
pub(crate) struct PtyInput {
    pub(crate) bytes: Bytes,
    pub(crate) provenance: InputProvenance,
    pub(crate) kind: InputKind,
}

impl PtyInput {
    pub(crate) fn new(bytes: Bytes, provenance: InputProvenance, kind: InputKind) -> Self {
        Self {
            bytes,
            provenance,
            kind,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComposerCleanliness {
    Clean,
    Dirty,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InputTrackerSnapshot {
    pub(crate) pty_epoch: String,
    pub(crate) cleanliness: ComposerCleanliness,
    pub(crate) content_sequence: u64,
    pub(crate) manual_enter_sequence: Option<u64>,
    pub(crate) manual_enter_reached_working: bool,
}

impl InputTrackerSnapshot {
    pub(crate) fn fresh() -> Self {
        Self {
            pty_epoch: uuid::Uuid::new_v4().to_string(),
            cleanliness: ComposerCleanliness::Clean,
            content_sequence: 0,
            manual_enter_sequence: None,
            manual_enter_reached_working: false,
        }
    }

    pub(crate) fn cold_restore() -> Self {
        Self {
            cleanliness: ComposerCleanliness::Unknown,
            ..Self::fresh()
        }
    }

    pub(crate) fn observe_accepted_input(&mut self, input: &PtyInput) {
        if input.bytes.is_empty() {
            return;
        }
        match (&input.provenance, input.kind) {
            (InputProvenance::Manual, InputKind::Content) => {
                self.record_content_byte();
            }
            (InputProvenance::Manual, InputKind::TopLevelEnter) => {
                self.content_sequence = self.content_sequence.saturating_add(1);
                self.cleanliness = ComposerCleanliness::Dirty;
                self.manual_enter_sequence = Some(self.content_sequence);
                self.manual_enter_reached_working = false;
            }
            (InputProvenance::PaneApi, InputKind::Content | InputKind::TopLevelEnter) => {
                self.record_content_byte();
            }
            (InputProvenance::Manual | InputProvenance::PaneApi, InputKind::Control)
            | (InputProvenance::Queue(_), _)
            | (InputProvenance::System, _) => {}
        }
    }

    fn record_content_byte(&mut self) {
        self.content_sequence = self.content_sequence.saturating_add(1);
        self.cleanliness = ComposerCleanliness::Dirty;
        self.manual_enter_sequence = None;
        self.manual_enter_reached_working = false;
    }

    pub(crate) fn observe_agent_state(&mut self, state: crate::detect::AgentState) {
        if self.manual_enter_sequence != Some(self.content_sequence) {
            return;
        }
        if state == crate::detect::AgentState::Working {
            self.manual_enter_reached_working = true;
            return;
        }
        if self.manual_enter_reached_working && state == crate::detect::AgentState::Idle {
            self.cleanliness = ComposerCleanliness::Clean;
            self.manual_enter_sequence = None;
            self.manual_enter_reached_working = false;
        }
    }

    pub(crate) fn prove_fresh_resume_with_zero_input(&mut self) {
        if self.content_sequence == 0 {
            self.cleanliness = ComposerCleanliness::Clean;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(provenance: InputProvenance, kind: InputKind) -> PtyInput {
        PtyInput::new(Bytes::from_static(b"x"), provenance, kind)
    }

    #[test]
    fn manual_content_dirties_and_only_enter_working_idle_clears() {
        let mut tracker = InputTrackerSnapshot::fresh();
        tracker.observe_accepted_input(&input(InputProvenance::Manual, InputKind::Content));
        assert_eq!(tracker.cleanliness, ComposerCleanliness::Dirty);
        tracker.observe_agent_state(crate::detect::AgentState::Working);
        tracker.observe_agent_state(crate::detect::AgentState::Idle);
        assert_eq!(tracker.cleanliness, ComposerCleanliness::Dirty);

        tracker.observe_accepted_input(&input(InputProvenance::Manual, InputKind::TopLevelEnter));
        tracker.observe_agent_state(crate::detect::AgentState::Idle);
        assert_eq!(tracker.cleanliness, ComposerCleanliness::Dirty);
        tracker.observe_agent_state(crate::detect::AgentState::Working);
        tracker.observe_agent_state(crate::detect::AgentState::Idle);
        assert_eq!(tracker.cleanliness, ComposerCleanliness::Clean);
    }

    #[test]
    fn later_manual_content_invalidates_enter_cycle() {
        let mut tracker = InputTrackerSnapshot::fresh();
        tracker.observe_accepted_input(&input(InputProvenance::Manual, InputKind::TopLevelEnter));
        tracker.observe_agent_state(crate::detect::AgentState::Working);
        tracker.observe_accepted_input(&input(InputProvenance::Manual, InputKind::Content));
        tracker.observe_agent_state(crate::detect::AgentState::Idle);
        assert_eq!(tracker.cleanliness, ComposerCleanliness::Dirty);
    }

    #[test]
    fn queue_and_system_input_never_dirty_composer() {
        let mut tracker = InputTrackerSnapshot::fresh();
        tracker.observe_accepted_input(&input(
            InputProvenance::Queue("queue-id".into()),
            InputKind::Content,
        ));
        tracker.observe_accepted_input(&input(InputProvenance::System, InputKind::Content));
        assert_eq!(tracker.cleanliness, ComposerCleanliness::Clean);
        assert_eq!(tracker.content_sequence, 0);
    }

    #[test]
    fn cold_restore_requires_zero_input_resume_proof() {
        let mut clean = InputTrackerSnapshot::cold_restore();
        clean.prove_fresh_resume_with_zero_input();
        assert_eq!(clean.cleanliness, ComposerCleanliness::Clean);

        let mut dirty = InputTrackerSnapshot::cold_restore();
        dirty.observe_accepted_input(&input(InputProvenance::PaneApi, InputKind::Content));
        dirty.prove_fresh_resume_with_zero_input();
        assert_eq!(dirty.cleanliness, ComposerCleanliness::Dirty);
    }
}
