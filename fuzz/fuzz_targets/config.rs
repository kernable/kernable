//! Any arguments and environment for `kernable start`, and any text where the kernel reads a
//! time, a name or a type pattern: refused or read, never a crash.
#![no_main]

use kernable_kernel::{Config, config};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    // A root or key "path" is read from disk: keep the fuzzer off devices that never end.
    if text.contains("/dev") || text.contains("/proc") {
        return;
    }
    let args: Vec<&str> = text.split('\0').collect();
    let env = |var: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(var)?.strip_prefix('=').map(str::to_owned))
    };
    let _ = Config::from_args(&args, env);
    for part in &args {
        if let Some(ms) = config::utc_ms(part) {
            assert_eq!(config::utc_ms(&config::utc(ms)), Some(ms), "{part}");
        }
        let _ = config::utc(part.len() as i64 * 86_400_000 - 1);
        let _ = (config::valid_domain(part), config::valid_name(part));
        let _ = (config::valid_type(part), config::valid_qualified(part));
        let _ = config::valid_pattern(part);
        for other in &args {
            let _ = config::covers(part, other);
        }
    }
});
