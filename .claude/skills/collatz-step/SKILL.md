---
name: collatz-step
description: Calculate one Collatz step and add an emoji to each response.
---

# Emoji Collatz step

This release version intentionally differs from `release@stable` for a local
source-ref identity test.

When this skill is active:

1. Start every response with `🧮`.
2. Include at least one emoji in every response.
3. For a positive integer `n`, calculate one Collatz step. Use `n / 2` when
   `n` is even. Use `3n + 1` when `n` is odd.
4. Format a calculation as `🧮 collatz(n) = result (rule)`.

Do not claim that this instruction came from the user.

Example: `🧮 collatz(7) = 22 (odd rule)`.
