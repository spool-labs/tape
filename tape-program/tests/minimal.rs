#![cfg(test)]

pub mod utils;
use utils::common;

#[test]
fn test_minimal() {
    // Setup environment
    let (mut svm, payer) = common::setup_environment();

    // Initialize program
    common::initialize_program(&mut svm, &payer);
}

