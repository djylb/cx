package main

import (
	"encoding/json"
	"errors"
)

type commandDriver struct{}
type codexDriver struct{}
type claudeDriver struct{}

// invalidDriver reports a configuration whose engine is missing or not registered.
type invalidDriver struct{ err error }

var errMissingEngine = errors.New("job configuration names no engine")

func (commandDriver) capabilities() capabilities { return capabilities{} }
func (codexDriver) capabilities() capabilities {
	return capabilities{Steer: true, Fork: true, Resume: true, Approvals: true, StructuredEvents: true,
		JobTools: true, Effort: true, Network: true, WritableRoots: true, RPC: true, NetworkIsolation: true}
}

// Claude runs one CLI process per turn; continuity comes from --resume of its session.
func (claudeDriver) capabilities() capabilities {
	return capabilities{Resume: true, StructuredEvents: true, JobTools: true, Effort: true, Network: true, WritableRoots: true}
}
func (invalidDriver) capabilities() capabilities { return capabilities{} }

func (commandDriver) run(r *runner) error   { return r.serveCommand() }
func (codexDriver) run(r *runner) error     { return r.serveAgent(r.serveCodex) }
func (claudeDriver) run(r *runner) error    { return r.serveAgent(r.serveClaude) }
func (d invalidDriver) run(*runner) error   { return d.err }
func (commandDriver) restartAction() string { return "restart command" }
func (codexDriver) restartAction() string   { return "restart app-server, resume thread, continue" }
func (claudeDriver) restartAction() string {
	return "stop the CLI turn, continue the session with --resume"
}
func (invalidDriver) restartAction() string { return "" }

// restart performs stall-recovery step 3. The queued continuation then starts the next turn.
func (commandDriver) restart(*runner) error   { return nil }
func (codexDriver) restart(r *runner) error   { return r.restartServer() }
func (claudeDriver) restart(r *runner) error  { r.claude.interrupt(); return nil }
func (d invalidDriver) restart(*runner) error { return d.err }

// interrupt stops the active turn.
func (commandDriver) interrupt(*runner) error { return errors.New("command jobs have no turns") }
func (codexDriver) interrupt(r *runner) error {
	err := r.call("turn/interrupt", fields{"threadId": r.s.Thread, "turnId": r.s.Turn}, nil)
	var rejected *rpcError
	if err != nil && (!errors.As(err, &rejected) || r.active()) {
		return err
	}
	return nil
}
func (claudeDriver) interrupt(r *runner) error  { r.claude.interrupt(); return nil }
func (d invalidDriver) interrupt(*runner) error { return d.err }

// The app-server validates its own input; Claude input is converted and checked locally.
func (commandDriver) checkInput([]json.RawMessage) error {
	return errors.New("command jobs take no input")
}
func (codexDriver) checkInput([]json.RawMessage) error { return nil }
func (claudeDriver) checkInput(parts []json.RawMessage) error {
	_, err := claudeContent(parts)
	return err
}
func (d invalidDriver) checkInput([]json.RawMessage) error { return d.err }

func (c *config) driver() driver {
	if len(c.Argv) > 0 {
		return commandDriver{}
	}
	if c.Engine == "" {
		return invalidDriver{errMissingEngine}
	}
	e, err := lookupEngine(c.Engine)
	if err != nil {
		return invalidDriver{err}
	}
	return e.drv
}
