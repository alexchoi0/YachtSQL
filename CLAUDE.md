# Project Instructions for Claude Code

1. Don't simply put `#[ignore]` on failing tests.
   When a test fails because a feature is not yet implemented, implement the missing feature rather than adding
   `#[ignore]` to skip the test.

2. Use `debug_eprintln!` instead of `eprintln!`. Use headers such as `debug_eprintln!("[executor::dml::insert] ..");` on
   executor/dml/insert.rs.

3. Do not write comments unless instructed. Simply write code.

4. Match all enum variants explicitly and exhaustively.

5. Avoid nested if statements.

6. Prefer `match` pattern matching on enum and tuple types.

7. Always use panic! where the invariants fail for easy debugging.

8. Avoid writing nested if/else beyond two layers deep.

9. Use assert_eq!(result, batch![ .. ]) for asserting against a result of query execution.

example:

```rust
 assert_batch_eq!(
    result,
    [
       [1, 1.0, "New York", null, [{"Laptop", datetime(2025, 10, 1), numeric("2.13")}]],
       [2, 1.0, "Los Angeles", null, [{"Keyboard", datetime(2025, 10, 1), numeric("4.11")}]],
       [3, 1.0, "New York", null, [{"Laptop", datetime(2025, 10, 1), numeric("5.00")}]],
    ]
);
```

10. Don't include
    `Generated with [Claude Code](https://claude.com/claude-code) Co-Authored-By: Claude <noreply@anthropic.com>` when
    making a commit.
