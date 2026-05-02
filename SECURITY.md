# Security Notes

**Snapshot date:** 2025-12-25

- Run `dbt-doctor` locally against projects and warehouses you control.
- Use read-only database credentials for profiling and schema drift checks.
- Treat `update_model_yaml` as a source-code write operation; review diffs before committing.
- Do not use production warehouse credentials until SQL sandboxing is strengthened beyond lightweight checks.
- Report security issues privately by opening a minimal GitHub issue asking for a disclosure contact, without posting credentials or exploit details.

