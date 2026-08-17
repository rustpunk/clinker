use std::sync::{Arc, Mutex};

use clinker_exec::executor::capabilities::{
    AdmittedActivationGroup, AdmittedRunCapabilities, AdmittedSourceOpener, CapabilityOpenError,
    CapabilityOpener, CapabilityReservationError, CapabilitySession, GroupCapacityLease,
    GroupCapacityReservation, RunCapabilityErrorKind,
};
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
use clinker_plan::config::{CompileContext, PipelineConfig, parse_config};
use clinker_plan::plan::execution::{
    CompiledSourceInstanceId, SourceActivationCapacity, SourceActivationGroup,
};

fn compile(yaml: &str) -> clinker_plan::plan::CompiledPlan {
    let config: PipelineConfig = parse_config(yaml).expect("fixture parses");
    config
        .compile(&CompileContext::default())
        .expect("fixture compiles")
}

fn one_source_plan() -> clinker_plan::plan::CompiledPlan {
    compile(
        r#"
pipeline: { name: admitted_capability }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema: [{ name: id, type: string }]
"#,
    )
}

fn two_source_plan() -> clinker_plan::plan::CompiledPlan {
    compile(
        r#"
pipeline: { name: admitted_capability_cleanup }
nodes:
  - type: source
    name: first
    config:
      name: first
      type: csv
      path: first.csv
      schema: [{ name: id, type: string }]
  - type: source
    name: second
    config:
      name: second
      type: csv
      path: second.csv
      schema: [{ name: id, type: string }]
"#,
    )
}

#[derive(Clone)]
struct Events(Arc<Mutex<Vec<String>>>);

impl Events {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn push(&self, event: impl Into<String>) {
        self.0.lock().expect("event mutex").push(event.into());
    }

    fn snapshot(&self) -> Vec<String> {
        self.0.lock().expect("event mutex").clone()
    }
}

struct FixtureReservation {
    label: &'static str,
    events: Events,
    fail: bool,
}

impl GroupCapacityReservation for FixtureReservation {
    fn reserve(self: Box<Self>) -> Result<Box<dyn GroupCapacityLease>, CapabilityReservationError> {
        self.events.push(format!("reserve:{}", self.label));
        if self.fail {
            Err(CapabilityReservationError::Unavailable)
        } else {
            Ok(Box::new(FixtureLease {
                label: self.label,
                events: self.events.clone(),
            }))
        }
    }
}

struct FixtureLease {
    label: &'static str,
    events: Events,
}

impl GroupCapacityLease for FixtureLease {}

impl Drop for FixtureLease {
    fn drop(&mut self) {
        self.events.push(format!("lease_drop:{}", self.label));
    }
}

struct FixtureOpener {
    label: &'static str,
    events: Events,
    fail: bool,
    secret: Box<str>,
}

impl CapabilityOpener for FixtureOpener {
    fn open(self: Box<Self>) -> Result<Box<dyn CapabilitySession>, CapabilityOpenError> {
        self.events.push(format!("open:{}", self.label));
        if self.fail {
            Err(CapabilityOpenError::Unavailable)
        } else {
            Ok(Box::new(FixtureSession {
                label: self.label,
                events: self.events.clone(),
                _secret: self.secret,
            }))
        }
    }
}

struct FixtureSession {
    label: &'static str,
    events: Events,
    _secret: Box<str>,
}

impl CapabilitySession for FixtureSession {}

impl Drop for FixtureSession {
    fn drop(&mut self) {
        self.events.push(format!("session_drop:{}", self.label));
    }
}

fn source_opener(
    member: CompiledSourceInstanceId,
    label: &'static str,
    events: &Events,
    fail: bool,
) -> AdmittedSourceOpener {
    AdmittedSourceOpener::new(
        member,
        Box::new(FixtureOpener {
            label,
            events: events.clone(),
            fail,
            secret: "opener-secret-must-not-escape".into(),
        }),
    )
}

fn group_request(
    group: &SourceActivationGroup,
    label: &'static str,
    events: &Events,
    capacity: SourceActivationCapacity,
    fail_reservation: bool,
    fail_open: bool,
) -> AdmittedActivationGroup {
    AdmittedActivationGroup::new(
        group.id(),
        capacity,
        group
            .members()
            .iter()
            .copied()
            .map(|member| source_opener(member, label, events, fail_open))
            .collect(),
        Box::new(FixtureReservation {
            label,
            events: events.clone(),
            fail: fail_reservation,
        }),
    )
}

#[test]
fn exact_contract_reserves_then_transfers_each_group_once() {
    let plan = one_source_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];
    let member = group.members()[0];
    let events = Events::new();

    let mut admitted = AdmittedRunCapabilities::admit(
        activation,
        vec![group_request(
            group,
            "orders",
            &events,
            group.capacity(),
            false,
            false,
        )],
    )
    .expect("exact contract admits");
    assert_eq!(admitted.group_count(), 1);
    assert_eq!(admitted.remaining_group_count(), 1);

    let mut active = admitted
        .take_group(group.id())
        .expect("group transfers once");
    assert_eq!(active.capacity(), group.capacity());
    assert_eq!(admitted.remaining_group_count(), 0);
    assert_eq!(
        admitted.take_group(group.id()).unwrap_err().kind(),
        RunCapabilityErrorKind::GroupUnavailable
    );

    active.open(member).expect("source opener runs once");
    assert_eq!(
        active.open(member).unwrap_err().kind(),
        RunCapabilityErrorKind::SourceUnavailable
    );
    drop(active);
    drop(admitted);

    assert_eq!(
        events.snapshot(),
        [
            "reserve:orders",
            "open:orders",
            "session_drop:orders",
            "lease_drop:orders",
        ]
    );
}

#[test]
fn required_plus_one_capacity_is_rejected_before_reservation() {
    let plan = one_source_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];
    let events = Events::new();
    let request = group_request(
        group,
        "orders",
        &events,
        SourceActivationCapacity::new(
            group.capacity().resource_units() + 1,
            group.capacity().opener_units(),
            group.capacity().credential_handle_units(),
        ),
        false,
        false,
    );

    let error = AdmittedRunCapabilities::admit(activation, vec![request]).unwrap_err();
    assert_eq!(error.kind(), RunCapabilityErrorKind::CapacityMismatch);
    assert!(events.snapshot().is_empty());
}

#[test]
fn partial_reservation_failure_releases_prior_groups_in_reverse_order() {
    let plan = two_source_plan();
    let activation = plan.dag().source_activation();
    assert_eq!(activation.groups().len(), 2, "fixture needs two groups");
    let events = Events::new();
    let requests = vec![
        group_request(
            &activation.groups()[0],
            "first",
            &events,
            activation.groups()[0].capacity(),
            false,
            false,
        ),
        group_request(
            &activation.groups()[1],
            "second",
            &events,
            activation.groups()[1].capacity(),
            true,
            false,
        ),
    ];

    let error = AdmittedRunCapabilities::admit(activation, requests).unwrap_err();
    assert_eq!(error.kind(), RunCapabilityErrorKind::ReservationFailed);
    assert_eq!(
        events.snapshot(),
        ["reserve:first", "reserve:second", "lease_drop:first"]
    );
}

#[test]
fn dropping_unconsumed_bundle_releases_group_leases_in_reverse_order() {
    let plan = two_source_plan();
    let activation = plan.dag().source_activation();
    assert_eq!(activation.groups().len(), 2, "fixture needs two groups");
    let events = Events::new();
    let admitted = AdmittedRunCapabilities::admit(
        activation,
        vec![
            group_request(
                &activation.groups()[0],
                "first",
                &events,
                activation.groups()[0].capacity(),
                false,
                false,
            ),
            group_request(
                &activation.groups()[1],
                "second",
                &events,
                activation.groups()[1].capacity(),
                false,
                false,
            ),
        ],
    )
    .expect("all reservations succeed");

    drop(admitted);
    assert_eq!(
        events.snapshot(),
        [
            "reserve:first",
            "reserve:second",
            "lease_drop:second",
            "lease_drop:first",
        ]
    );
}

#[test]
fn opener_failure_and_downstream_error_release_active_group() {
    let plan = one_source_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];
    let member = group.members()[0];
    let events = Events::new();
    let mut admitted = AdmittedRunCapabilities::admit(
        activation,
        vec![group_request(
            group,
            "orders",
            &events,
            group.capacity(),
            false,
            true,
        )],
    )
    .expect("group reserves");

    let result: Result<(), RunCapabilityErrorKind> = (|| {
        let mut active = admitted
            .take_group(group.id())
            .map_err(|error| error.kind())?;
        active.open(member).map_err(|error| error.kind())?;
        Err(RunCapabilityErrorKind::SourceUnavailable)
    })();

    assert_eq!(result, Err(RunCapabilityErrorKind::OpenFailed));
    assert_eq!(
        events.snapshot(),
        ["reserve:orders", "open:orders", "lease_drop:orders"]
    );
}

#[test]
fn executor_error_releases_the_unconsumed_bundle() {
    let plan = one_source_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];
    let events = Events::new();
    let admitted = AdmittedRunCapabilities::admit(
        activation,
        vec![group_request(
            group,
            "orders",
            &events,
            group.capacity(),
            false,
            false,
        )],
    )
    .expect("group admits");

    let result = PipelineExecutor::run_admitted_plan_with_readers_writers(
        &plan,
        admitted,
        std::collections::HashMap::new(),
        WriterRegistry::default(),
        &PipelineRunParams::default(),
    );

    assert!(result.is_err(), "missing Source reader must fail the run");
    assert_eq!(events.snapshot(), ["reserve:orders", "lease_drop:orders"]);
}

#[test]
fn interruption_drop_releases_opened_session_before_group_capacity() {
    let plan = one_source_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];
    let member = group.members()[0];
    let events = Events::new();
    let mut admitted = AdmittedRunCapabilities::admit(
        activation,
        vec![group_request(
            group,
            "orders",
            &events,
            group.capacity(),
            false,
            false,
        )],
    )
    .expect("group admits");
    let mut active = admitted.take_group(group.id()).expect("group transfers");
    active.open(member).expect("session opens");

    drop(active);
    assert_eq!(
        events.snapshot(),
        [
            "reserve:orders",
            "open:orders",
            "session_drop:orders",
            "lease_drop:orders",
        ]
    );
}

#[test]
fn debug_and_errors_never_render_opaque_payloads() {
    let plan = one_source_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];
    let events = Events::new();
    let admitted = AdmittedRunCapabilities::admit(
        activation,
        vec![group_request(
            group,
            "orders",
            &events,
            group.capacity(),
            false,
            false,
        )],
    )
    .expect("group admits");

    let debug = format!("{admitted:?}");
    assert!(debug.contains("AdmittedRunCapabilities"));
    assert!(!debug.contains("opener-secret-must-not-escape"));

    let mismatched = group_request(
        group,
        "orders",
        &events,
        SourceActivationCapacity::default(),
        false,
        false,
    );
    let error = AdmittedRunCapabilities::admit(activation, vec![mismatched]).unwrap_err();
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("orders"));
    assert!(!rendered.contains("opener-secret-must-not-escape"));
}
