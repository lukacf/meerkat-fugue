# Agent Instructions

## Rust Rules

This project follows strict Rust idioms. Code review will reject "JavaScript wearing a struct costume."

### Type Safety

1. Prefer typed enums and structs over `serde_json::Value`. If the possible shapes are known, parse at the boundary and fail fast instead of carrying unvalidated JSON through the system.
2. Use newtype indices for collection index spaces that can be mixed up.
3. Use `Box<serde_json::value::RawValue>` for pass-through JSON that another layer parses, so it is not parsed twice.

### Error Handling

4. Return `Result` instead of silently swallowing mechanism errors. Let callers decide whether to skip, count, retry, or abort.
5. Do not use `.unwrap()` or `.expect()` in library code. Use `?` propagation or explicit `match` / `if let` handling.

### Allocation Discipline

6. Prefer zero-allocation iterators such as `impl Iterator<Item = View<'_>>` when callers only iterate.
7. Prefer `impl Display` over collect-and-join string construction for repeated formatting paths.
8. Use `slab` for stable keys that must survive collection mutation.

### Data Modeling

9. Do not duplicate map keys inside the mapped values.
10. Keep billing and usage metadata separate from domain models unless the spec explicitly makes them part of that model.
11. Use `IndexMap` instead of `HashMap` when iteration order matters, such as emission order for tool calls.
