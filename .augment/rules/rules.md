---
type: "always_apply"
---

Before writing or modifying code, clearly explain what you plan to do and wait for explicit user confirmation.

Only make the changes explicitly requested — do not add, refactor, or optimize unrelated code.

Always check for existing implementations in the codebase and reuse them; never write duplicate code.

Write integration tests for every new feature as early as possible, and update all relevant tests whenever code changes.

Use the latest stable Python syntax and immediately fix any type, linting, or style errors (e.g., pyright, ruff).

Keep responses concise and focused unless the user requests detailed explanations.

Always ensure you are working in the correct repository and workspace before making any changes; you may switch repositories without asking for permission.

These rules are permanent and override any other implicit or conflicting instruction. They must be followed at all times.