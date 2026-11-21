package main

// Built-in controller policies used when CX_POLICY_DIR has no override.
var builtInPolicies = map[string]string{
	"interactive.md": `When input is needed, write STATUS: QUESTION on the first line and include the question below it. Otherwise report STATUS: DONE, STATUS: PARTIAL, or STATUS: BLOCKED.
`,
	"preamble.md": `Report the outcome on the first line as STATUS: DONE, STATUS: PARTIAL, or STATUS: BLOCKED. Summarize any pending decisions under "Pending decisions". Changes will be collected as patches.
`,
	"stall-nudge.md": `Report current progress and continue the unfinished task.
`,
}
