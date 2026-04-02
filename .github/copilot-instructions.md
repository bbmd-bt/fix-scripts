## Iterative Convergence Methodology (Versus)

This project uses the Versus methodology orchestrated via MCP server `versus-copilot`.

### MANDATORY in every interaction:

1. **Always** call `get_phase_state` first to know the current phase
2. Call `get_phase_guidance` to get detailed phase instructions
3. If no project is initialized, ask the user if they want to create one with `init_project`
4. Record every important decision with `record_decision`
5. Before advancing phases, use `get_exit_criteria` to check pending items and `advance_phase` to transition

### User interaction:

Ask the user directly in chat. Follow this EXACT format for every question:

```
---
**Question Title** (single choice / multi choice)

1. **Option A (Recommended)** — short explanation
2. **Option B** — short explanation
3. **Option C** — short explanation

> Reply with the number (e.g. `1`) or type your own answer.
---
```

Rules:
- ONE question per block, separated by horizontal rule (`---`)
- Max 4 questions per message
- NEVER mix multiple questions in a single paragraph or nested list
- ALWAYS end each block with the reply instruction line
- Mark recommended option with **(Recommended)**
- Options must be contextualized to the project — never generic
- After the response, record with `record_decision`

### General rules:

- NEVER skip phases — the methodology is sequential (Phase 0 → 7)
- NEVER implement code before Phase 5
- Every architectural decision must be recorded
- Consult `get_decisions` to retrieve previous decisions
- Use `check_all_safeguards` before phase transitions
