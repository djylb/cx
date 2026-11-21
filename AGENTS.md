# Repository agent instructions

- Read the existing call path before changing behavior. In the development checkout, also consult `CX-DESIGN.md`. Use the Go controller and node as the source for current CLI behavior and exit codes.
- Reuse the existing transport, job state, snapshot, and result contracts. Keep dispatch and observation mechanical. The caller decides policy, retries, and when to apply changes.
- Verify changes through the normal CLI with bounded commands, private temporary state, and disposable fixtures. Keep live services and real deployment credentials outside tests and repository files.
- For day-to-day cx usage, follow [docs/agent-guide.md](docs/agent-guide.md). For deployment, follow [docs/agent-install.md](docs/agent-install.md).
