package main

type capabilities struct {
	Steer, Fork, Resume, Approvals, StructuredEvents bool
}

type driver interface {
	capabilities() capabilities
	run(*runner) error
}

type commandDriver struct{}
type codexDriver struct{}
type claudeDriver struct{}

func (commandDriver) capabilities() capabilities { return capabilities{} }
func (codexDriver) capabilities() capabilities {
	return capabilities{Steer: true, Fork: true, Resume: true, Approvals: true, StructuredEvents: true}
}
func (claudeDriver) capabilities() capabilities {
	return capabilities{Resume: true, StructuredEvents: true}
}
func (commandDriver) run(r *runner) error { return r.serveCommand() }
func (codexDriver) run(r *runner) error   { return r.serveAgent(r.serveCodex) }
func (claudeDriver) run(r *runner) error  { return r.serveAgent(r.serveClaude) }
func (c *config) driver() driver {
	if len(c.Argv) > 0 {
		return commandDriver{}
	}
	if c.Engine == "claude" {
		return claudeDriver{}
	}
	return codexDriver{}
}
