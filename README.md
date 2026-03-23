# Workspace Layout

This repository is organized as a uv workspace with a shared root virtual environment.

Each subfolder under `workspaces/` is its own uv project with its own dependency set, but they all share the workspace venv at the repository root:

- `workspaces/castleshield_rinit_measurements`
- `workspaces/old_MAD_data_migration`

Run a tool by its exposed script name from the repository root, for example:

```powershell
uv run castleshield-rinit-measurements
uv run ttk2json-batch --help
```

The shared virtual environment lives at the repository root; the `workspaces/` folders stay as the separate source projects.

