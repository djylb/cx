package main

import "time"

// Ceilings for long controller operations, so a lost peer cannot hold a caller forever.
// Each can be overridden in seconds through its environment variable; 0 is rejected.
var (
	// A submission covers snapshot publication and node registration.
	submitLimit = operationLimit("CX_SUBMIT_LIMIT", 90*time.Minute)
	// Transfers, deployment and submission queues carry whole snapshots and binaries.
	transferLimit = operationLimit("CX_TRANSFER_LIMIT", 6*time.Hour)
	// Result collection downloads one sealed turn.
	collectLimit = operationLimit("CX_COLLECT_LIMIT", 30*time.Minute)
	// An engine RPC waits for the runner's receipt and reply.
	rpcLimit = operationLimit("CX_RPC_LIMIT", 3*time.Minute)
	// Installing a compiler on the build center.
	compilerInstallLimit = operationLimit("CX_COMPILER_INSTALL_LIMIT", 6*time.Minute)
)

// controlCallIdle is the silence allowed on a control action. A control action can wait for a
// job-scoped tool call in progress, so it outlasts jobToolTimeout.
const controlCallIdle = jobToolTimeout + 35*time.Second

func operationLimit(key string, fallback time.Duration) time.Duration {
	d, err := transferSetting(key, fallback, false)
	if err != nil {
		diagnosticf("cx: %v; using %s\n", err, fallback)
		return fallback
	}
	return d
}
