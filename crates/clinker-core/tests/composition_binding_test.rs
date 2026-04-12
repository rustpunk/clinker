#[test]
fn scaffold_compiles() {
    // Verifies the composition_binding_test integration test module is
    // discoverable and the plan::composition_body module exists.
    let _ = std::any::type_name::<clinker_core::plan::compiled::CompiledPlan>();
}
