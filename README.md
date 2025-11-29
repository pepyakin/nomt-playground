a minimal deterministic repro is:

```
RUST_BACKTRACE=1 RUST_LOG=info ./target/release/rollup_emulator --number-of-blocks=20 --fast-sequencers=1 --sleepy-sequencers=0 --seed 26870
```
