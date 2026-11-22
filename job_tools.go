package main

import (
	"os"
	"path/filepath"
	"time"
)

// jobToolTimeout bounds one call to the job-scoped messaging tools. Engines receive it in
// their own configuration form; agent_ask waits stay below it.
const jobToolTimeout = 150 * time.Second

// jobToolServerName is the MCP server name engines see for the job-scoped tools.
const jobToolServerName = "cxrun"

// jobToolServer is the stdio MCP server command that exposes the job-scoped tools.
func jobToolServer(c config, root, dir string) (string, []string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", nil, err
	}
	id := filepath.Base(dir)
	if c.Project != "" {
		id = c.Project + "/" + id
	}
	return exe, []string{"node", "mcp-tools", "--state", root, "--job", id, "--expect-run", c.RunID}, nil
}
