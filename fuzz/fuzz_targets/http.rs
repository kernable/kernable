//! Any bytes as an HTTP request to the UI and the metrics endpoint: an answer, never a crash.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kernable_kernel::fuzzing::http(data);
});
