//! Any bytes as a membership (`kernable apply`) or a worker's registration: refused or
//! accepted by the kernel's checks, never a crash.
#![no_main]

use kernable_core::proto::{Membership, Register};
use kernable_kernel::fuzzing;
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = Membership::decode(data) {
        fuzzing::membership(&m);
    }
    if let Ok(reg) = Register::decode(data) {
        fuzzing::register(&reg);
    }
});
