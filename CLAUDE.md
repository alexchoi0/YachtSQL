# Project Instructions for Claude Code

1. Don't simply put `#[ignore]` on failing tests.
   When a test fails because a feature is not yet implemented, implement the missing feature rather than adding
   `#[ignore]` to skip the test.

2. Use `debug_eprintln!` instead of `eprintln!`. Use headers such as `debug_eprintln!("[executor::dml::insert] ..");` on
   executor/dml/insert.rs.

3. Do not write comments unless instructed. Simply write code.

4. Match all enum variants explicitly and exhaustively.

5. Avoid nested if statements.

6. Use `match` on enum and tuple types. Use `if` on boolean type.

7. Always use panic! where the invariants fail for easy debugging.