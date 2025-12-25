# Contributing to dbt-doctor

Thank you for your interest in contributing to `dbt-doctor`! This project aims to make working with dbt models inside MCP (Cursor, Cline, Windsurf) seamless. Since the project is young, all contributions are highly appreciated!

## Getting Started

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/dbt-doctor.git
   cd dbt-doctor
   ```
3. **Set up a virtual environment** and install dev dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e ".[dev]"
   ```

## Development Workflow

### Adding New Tools
If you want to add a new MCP tool to analyze the dbt project:
1. Create a core analyzer class in `src/dbt_doctor/analyzers/`. All logic should live here, independent of MCP.
2. Add comprehensive tests in `tests/` using the `pytest` framework and our included `duckdb` fixture if database interactions are required.
3. Expose the analyzer logic as an MCP tool inside `src/dbt_doctor/server.py`.

### Code Standards
- We use `ruff` for all formatting and linting.
- Simply run `ruff check .` and `ruff format .` before opening a pull request.
- Ensure all tests pass by running `pytest tests/`.

## Submitting Pull Requests
- Please make sure your PR description clearly states the problem you're solving or the feature you're adding.
- If your PR is fixing a specific issue, reference the issue number (e.g., `Fixes #42`).
- We try to review PRs within 48 hours.

We value feedback and code contributions equally! If you've found a bug or have a feature idea, feel free to open an Issue first to discuss it.
