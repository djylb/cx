package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func controllerPolicy(name string) (string, error) {
	if d := os.Getenv("CX_POLICY_DIR"); d != "" {
		b, e := os.ReadFile(filepath.Join(d, name))
		if !os.IsNotExist(e) {
			return string(b), e
		}
	}
	text, ok := builtInPolicies[name]
	if !ok {
		return "", fmt.Errorf("unknown policy %q", name)
	}
	return text, nil
}

type jobSubmission struct {
	Sources                                         map[string]string  `json:",omitempty"`
	Branches                                        map[string]*string `json:",omitempty"`
	ReuseKey                                        string
	ID, Host, Role, Parent, Session, Source, Prompt string
	Snapshot                                        SnapshotID
	Options, Command                                []string
	Context                                         map[string][]byte
	Preamble, Nudge                                 string
	Images                                          []attachment
}

func defaultSubmitHost() string {
	if v := os.Getenv("CX_DEFAULT_HOST"); v != "" {
		return v
	}
	return "local"
}
func submitSnapshot(ctx context.Context, src string, o ctlOptions, def projectDefinition) (SnapshotID, error) {
	if o.snapshot == "" {
		sid, _, e := Freeze(ctx, src, FreezeOptions{History: o.history, Repositories: def.Repositories})
		return sid, e
	}
	if o.history != "" || !snapHex(o.snapshot, 64) {
		return "", errors.New("invalid pinned snapshot or incompatible history")
	}
	return SnapshotID(o.snapshot), nil
}
func loadSnapshot(sid SnapshotID) (Manifest, error) {
	var m Manifest
	store, e := snapStore()
	if e != nil {
		return m, e
	}
	if !snapHex(string(sid), 64) {
		return m, errors.New("invalid snapshot id")
	}
	m, _, e = snapManifest(store, string(sid))
	return m, e
}
func ctlSubmit(ctx context.Context, hs endpoints, op string, o ctlOptions) error {
	return ctlSubmitWithRebase(ctx, hs, op, o, rebaseInput{})
}

type rebaseInput struct {
	files map[string][]byte
	key   string
}

func ctlSubmitWithRebase(ctx context.Context, hs endpoints, op string, o ctlOptions, rebase rebaseInput) error {
	if op == "snapshot" {
		if len(o.pos) != 1 {
			return errors.New("snapshot requires DIR")
		}
		if o.history == "" {
			o.snapshot = os.Getenv("LAN_SNAPSHOT_ID")
		}
		_, def, e := resolveProject(ctx, o.pos[0], o.project)
		if e != nil {
			return e
		}
		id, e := submitSnapshot(ctx, o.pos[0], o, def)
		if e != nil {
			return e
		}
		h, e := hs.get(defaultSubmitHost())
		if e != nil {
			return e
		}
		if e = Publish(ctx, h, id, nil); e != nil {
			return e
		}
		fmt.Println(id)
		return nil
	}
	session, sessionErr := o.submissionSession()
	if o.previous == "" && !o.reuse && sessionErr != nil {
		return ctlUsage(op, sessionErr)
	}
	var host, src, tag, prompt string
	if op == "agent" {
		if len(o.pos) != 1 {
			return errors.New("agent requires a prompt")
		}
		prompt = o.pos[0]
		var e error
		if prompt == "-" {
			b, err := io.ReadAll(os.Stdin)
			prompt, e = string(b), err
		} else if strings.HasPrefix(prompt, "@") {
			prompt, e = readInputText(prompt[1:])
		}
		if e != nil {
			return e
		}
		if o.previous != "" {
			if o.project != "" {
				return errors.New("--project applies to new jobs")
			}
			name, id, continueErr := resolveJob(ctx, o.previous)
			if continueErr != nil {
				return continueErr
			}
			h, continueErr := hs.get(name)
			if continueErr != nil {
				return continueErr
			}
			if o.queue != "" {
				return errors.New("--queue-timeout applies to the original submission")
			}
			shown := id
			if strings.Contains(o.previous, ":") {
				shown = o.previous
			}
			diagnosticf("JOB=%s\n", shown)
			if continueErr = forgetResult(name, id); continueErr != nil {
				return continueErr
			}
			argv := []string{"continue-job", name, id, prompt}
			if o.profile != "" {
				argv = append(argv, o.profile)
			}
			r, continueErr := h.invoke(ctx, controlCallIdle, false, argv...)
			if continueErr != nil {
				return continueErr
			}
			if r.code != 0 {
				return r.output()
			}
			return agentReport(ctx, name, h, id, o.full)
		}
		src = o.dir
		if src == "" {
			src = "."
		}
		tag = o.tag
		if tag == "" {
			var nonce [4]byte
			if _, e = rand.Read(nonce[:]); e != nil {
				return e
			}
			tag = fmt.Sprintf("%s-%x", time.Now().Format("20060102-150405"), nonce)
		}
		host = o.host
	} else {
		if len(o.pos) < 2 {
			return errors.New("run HOST DIR TAG PROMPTFILE")
		}
		host, src = o.pos[0], o.pos[1]
		if o.batch != "" {
			return submitBatch(ctx, hs, o)
		}
		if o.command {
			if len(o.pos) != 3 || len(o.argv) == 0 {
				return errors.New("run HOST DIR TAG --cmd -- ARGV")
			}
		} else {
			if len(o.pos) != 4 || len(o.argv) != 0 {
				return errors.New("run HOST DIR TAG PROMPTFILE")
			}
			var e error
			prompt, e = readInputText(o.pos[3])
			if e != nil {
				return e
			}
		}
		tag = o.pos[2]
	}
	src, e := filepath.Abs(src)
	if e != nil {
		return e
	}
	p, j, e := submissionTag(tag, o.project)
	if e != nil {
		return e
	}
	p, def, e := resolveProject(ctx, src, p)
	if e != nil {
		return e
	}
	if op == "agent" && (host == "auto" || host == "") {
		host = defaultSubmitHost()
		if os.Getenv("CX_DEFAULT_HOST") == "" && def.Host != "" {
			host = def.Host
		}
	}
	h, e := hs.get(host)
	if e != nil {
		return e
	}
	id := p + "/" + j
	if _, e = jobDir("", id); e != nil {
		return e
	}
	req := jobSubmission{ID: id, Host: host, Role: o.role, Parent: o.parent, Session: session, Command: o.argv, Prompt: prompt}
	if req.Role == "" {
		req.Role = "worker"
	}
	if req.Parent == "" {
		req.Parent = os.Getenv("CX_PARENT")
		if req.Parent == "" {
			req.Parent = req.Session
		}
	}
	if o.inplace {
		if host != "local" || o.snapshot != "" || o.history != "" || o.cleanBase {
			return errors.New("inplace requires local without a snapshot")
		}
		req.Source = src
	}
	if o.reuse && !o.command {
		return errors.New("--reuse requires a command job")
	}
	if o.command && (o.engine != "" || o.interactive || o.web || o.input != "" || len(o.images) > 0) {
		return errors.New("agent options on command job")
	}
	if !o.command && strings.TrimSpace(prompt) == "" && o.input == "" && len(o.images) == 0 {
		return errors.New("empty prompt")
	}
	if !o.command && o.quiet != "" {
		return errors.New("quiet timeout requires --cmd")
	}
	if o.sandbox == "" {
		o.sandbox = defaultSandbox
	}
	if o.network && o.noNetwork {
		return errors.New("conflicting network options")
	}
	network := o.network || o.sandbox == "workspace-write" && !o.noNetwork
	if network {
		req.Options = append(req.Options, "--network")
	} else {
		req.Options = append(req.Options, "--no-network")
	}
	if o.hard && o.effort == "" {
		o.effort = "high"
	}
	jobTools := false
	if !o.command {
		// Check the engine's capabilities here so an unsupported option fails before any upload.
		eng, engineErr := lookupEngine(o.engine)
		if engineErr != nil {
			return engineErr
		}
		o.engine = eng.name
		c := config{Effort: o.effort, Network: network, Sandbox: o.sandbox, JobTools: !o.noTools && eng.caps().JobTools}
		if engineErr = c.checkEngineOptions(eng, options{input: o.input, images: o.images}, o.approval != ""); engineErr != nil {
			return engineErr
		}
		jobTools = c.JobTools
	}
	for _, kv := range [][2]string{{"profile", o.profile}, {"sandbox", o.sandbox}, {"model", o.model}, {"effort", o.effort}, {"engine", o.engine}, {"idle-exit", o.idle}, {"turn-timeout", o.turnLimit}, {"stall", o.stall}, {"approval-timeout", o.approval}, {"queue-timeout", o.queue}, {"timeout", o.timeout}, {"quiet-timeout", o.quiet}} {
		if kv[1] != "" {
			req.Options = append(req.Options, "--"+kv[0], kv[1])
		}
	}
	if jobTools {
		req.Options = append(req.Options, "--job-tools")
	}
	if o.web {
		req.Options = append(req.Options, "--web-search")
	}
	if o.interactive {
		req.Options = append(req.Options, "--interactive")
	}
	if o.input != "" {
		b, inputErr := rawArg(o.input)
		if inputErr != nil {
			return inputErr
		}
		warnInputCredentials(b)
		req.Options = append(req.Options, "--input-json", string(b))
	}
	for _, image := range o.images {
		data, readErr := os.ReadFile(image)
		if readErr != nil {
			return readErr
		}
		req.Images = append(req.Images, attachment{Ext: filepath.Ext(image), Data: data})
	}
	if o.profile != "" {
		// Check before snapshot preparation; the runner validates again at registration.
		if h.SSH != "" {
			if e = deploy(ctx, hs, host); e != nil {
				return e
			}
			h = hs[host]
		}
		r, profileErr := h.call(ctx, 20*time.Second, "check-profile", o.profile)
		if profileErr != nil {
			return profileErr
		}
		if r.code != 0 {
			return r.output()
		}
	}
	if e = attachContext(&req, p, o.interactive, o.context); e != nil {
		return e
	}
	for name, data := range rebase.files {
		if _, exists := req.Context[name]; exists {
			return fmt.Errorf("submission context already contains %q", name)
		}
		req.Context[name] = data
	}

	if jobTools && req.Preamble != "" {
		req.Preamble = strings.TrimRight(req.Preamble, "\n") + "\n" + jobToolsNote
	}
	// Without a preamble the caller submits its input verbatim.
	if o.sandbox != dangerFullAccess && req.Preamble != "" {
		req.Preamble = strings.TrimRight(req.Preamble, "\n") + "\n" + sandboxNote
	}
	if !o.inplace {
		req.Snapshot, e = submitSnapshot(ctx, src, o, def)
		if e != nil {
			return e
		}
	}
	if req.Snapshot != "" {
		if e = verifySourceBase(ctx, src, &req, o); e != nil {
			return e
		}
	}
	if o.command {
		req.ReuseKey = runReuseKey(req.Snapshot, src, req.Command)
		if o.reuse {
			found, reuseErr := lookupReuse(ctx, h, reuseQuery{ID: id, Host: host, Key: req.ReuseKey, Profile: o.profile})
			if reuseErr != nil || found {
				return reuseErr
			}
		}
	}
	if sessionErr != nil {
		return ctlUsage(op, sessionErr)
	}
	if h.Manager {
		if e = pushContext(ctx, h, host, false); e != nil {
			return fmt.Errorf("refresh context copy on %s: %w", host, e)
		}
	}
	canonical, e := filepath.EvalSymlinks(src)
	if e != nil {
		return e
	}
	origin := submissionOrigin{Source: canonical, Snapshot: req.Snapshot, JobOptions: req.Options, Role: req.Role, Parent: req.Parent, Command: o.command, History: o.history, CleanBase: o.cleanBase}
	if e = forgetResult(host, id); e != nil {
		return e
	}
	if e = queueJob(ctx, queuedJob{Request: req, Origin: origin, Rebase: rebase.key}, o.sync || o.inplace || op == "agent"); e != nil {
		return e
	}
	if op == "agent" {
		diagnosticf("JOB=%s\n", id)
		return agentReport(ctx, host, h, id, o.full)
	}
	if rebase.key == "" {
		fmt.Println(id)
	}
	return nil
}

func submissionTag(tag, project string) (string, string, error) {
	if p, j, ok := strings.Cut(tag, "/"); ok {
		if p == "" || project != "" && p != project {
			return "", "", errors.New("job project does not match --project")
		}
		return p, j, nil
	}
	return project, tag, nil
}

func submitBatch(ctx context.Context, hs endpoints, o ctlOptions) error {
	if len(o.pos) != 2 || o.command || o.inplace {
		return errors.New("batch needs HOST DIR and snapshot jobs")
	}
	b, e := os.ReadFile(o.batch)
	if e != nil {
		return e
	}
	var jobs [][]string
	var defaultName string
	definitions := map[string]projectDefinition{}
	seen := map[string]bool{}
	for line := range strings.SplitSeq(strings.TrimSuffix(string(b), "\n"), "\n") {
		parts := strings.Split(strings.TrimSuffix(line, "\r"), "\t")
		if len(parts) != 2 {
			return errors.New("batch rows require TAG and PROMPTFILE")
		}
		project, tag, tagErr := submissionTag(parts[0], o.project)
		if tagErr != nil {
			return tagErr
		}
		if project == "" {
			project = defaultName
		}
		if _, ok := definitions[project]; !ok {
			name, def, projectErr := resolveProject(ctx, o.pos[1], project)
			if projectErr != nil {
				return projectErr
			}
			if project == "" {
				defaultName = name
			}
			definitions[name] = def
			project = name
		}
		parts[0] = project + "/" + tag
		if _, e = jobDir("", parts[0]); e != nil {
			return e
		}
		p, readErr := readInputText(parts[1])
		if readErr != nil {
			return readErr
		}
		if seen[parts[0]] || strings.TrimSpace(p) == "" {
			return errors.New("duplicate tag or empty prompt")
		}
		seen[parts[0]] = true
		jobs = append(jobs, append(append([]string{}, o.pos...), parts...))
	}
	snapshots := map[string]SnapshotID{}
	for _, row := range jobs {
		project, _, _ := strings.Cut(row[2], "/")
		if _, ok := snapshots[project]; !ok {
			sid, snapshotErr := submitSnapshot(ctx, o.pos[1], o, definitions[project])
			if snapshotErr != nil {
				return snapshotErr
			}
			snapshots[project] = sid
		}
	}
	o.batch, o.history = "", ""
	var first error
	for _, row := range jobs {
		project, _, _ := strings.Cut(row[2], "/")
		o.snapshot = string(snapshots[project])
		o.pos = row
		if e = ctlSubmit(ctx, hs, "run", o); first == nil {
			first = e
		}
	}
	return first
}

// attachContext ships context files with the job: the shared layer next to the
// project definitions, then the project layer, whose files replace same-named shared
// ones, and the target host's notes under host/. All layers live only on the controller.
func attachContext(req *jobSubmission, p string, interactive bool, layers string) error {
	use, e := contextLayers(layers)
	if e != nil {
		return e
	}
	base, e := projectDirectory()
	if e != nil {
		return e
	}
	dir := os.Getenv("CX_CONTEXT_DIR")
	if dir == "" {
		dir = filepath.Join(base, p, "context")
	}
	req.Context = map[string][]byte{}
	config := filepath.Dir(base)
	for _, layer := range []struct{ name, root string }{{"shared", filepath.Join(config, "context")}, {"project", dir}} {
		if !use[layer.name] {
			continue
		}
		if e = readContextLayer(layer.root, req.Context); e != nil {
			return e
		}
	}
	host := map[string][]byte{}
	if use["host"] {
		if _, e = jobPath("", req.Host); e != nil {
			return e
		}
		if e = readContextLayer(filepath.Join(config, "hosts", req.Host, "context"), host); e != nil {
			return e
		}
		for k, b := range host {
			req.Context["host/"+k] = b
		}
	}
	if len(req.Command) > 0 {
		return nil
	}
	template := "preamble.md"
	if interactive {
		template = "interactive.md"
	}
	req.Preamble, e = controllerPolicy(template)
	if e != nil {
		return e
	}
	req.Nudge, e = controllerPolicy("stall-nudge.md")
	if e != nil {
		return e
	}
	if b, ok := req.Context[template]; ok {
		req.Preamble = string(b)
	}
	if len(req.Context) > 0 {
		req.Preamble += "\nProject context is available in $CX_CONTEXT.\n"
	}
	if _, ok := req.Context[contextIndex]; ok {
		req.Preamble += "Read $CX_CONTEXT/" + contextIndex + " before starting; open the files it points to only when relevant.\n"
	}
	if _, ok := host[contextIndex]; ok {
		req.Preamble += "Read $CX_CONTEXT/host/" + contextIndex + " for this host's environment.\n"
	}
	return nil
}

// contextLayers parses --context: a comma list of shared, project and host, or none.
// The default sends all three.
func contextLayers(v string) (map[string]bool, error) {
	use := map[string]bool{"shared": true, "project": true, "host": true}
	if v == "" {
		return use, nil
	}
	use = map[string]bool{}
	if v == "none" {
		return use, nil
	}
	for name := range strings.SplitSeq(v, ",") {
		switch name {
		case "shared", "project", "host":
			use[name] = true
		default:
			return nil, fmt.Errorf("invalid --context layer %q (shared, project, host or none)", name)
		}
	}
	return use, nil
}

// jobToolsNote tells an agent what the job tools reach and how to wait for a
// manager that may be offline: a blocking ask is bounded by the tool timeout.
const jobToolsNote = "Job tools: ask (the parent), send (parent, PROJECT/JOB or #CHANNEL; the manager relays channel posts) and inbox. " +
	"A blocking ask waits only a few minutes. To wait longer for an answer, end the turn with STATUS: QUESTION and the question; the job keeps its state and continues when answered. " +
	"$CX_CONTEXT is a read-only copy: to add or change shared memory (for example host tools or environment you discovered), send the proposed text to the parent, which decides.\n"

// sandboxNote tells a sandboxed agent how to ask for host privileges.
const sandboxNote = "If the task needs host privileges the sandbox denies (network namespaces, TUN devices, host process tables, local sockets), do not work around the sandbox: report STATUS: BLOCKED and say it needs --sandbox danger-full-access.\n"

// contextIndex is the context file agents read first when it exists.
const contextIndex = "MEMORY.md"

func readContextLayer(dir string, into map[string][]byte) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
		if path == dir && os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("context requires regular files: %s", path)
		}
		k, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		b, e := readSharedFile(path)
		into[filepath.ToSlash(k)] = b
		return e
	})
}

func legacyGate(ctx context.Context, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, submitLimit)
	defer cancel()
	mode := os.Getenv("LAN_SOURCE_MODE")
	if mode == "" {
		mode = "snapshot"
	}
	env := append(os.Environ(), "LAN_SOURCE_MODE="+mode)
	var cmd *exec.Cmd
	if args[0] == "test" {
		tool := os.Getenv("CX_TEST_COMMAND")
		if tool == "" {
			return errors.New("set CX_TEST_COMMAND to the test executable")
		}
		cmd = exec.CommandContext(ctx, tool, args[1:]...)
	} else {
		if len(args) < 2 {
			return errors.New("gate requires DIR")
		}
		src, e := filepath.Abs(args[1])
		if e != nil {
			return e
		}
		if mode == "snapshot" {
			hs, e := loadEndpoints()
			if e != nil {
				return e
			}
			h, e := hs.get(defaultSubmitHost())
			if e != nil {
				return e
			}
			_, def, e := resolveProject(ctx, src, "")
			if e != nil {
				return e
			}
			sid, e := submitSnapshot(ctx, src, ctlOptions{snapshot: os.Getenv("LAN_SNAPSHOT_ID")}, def)
			if e != nil {
				return e
			}
			if e = Publish(ctx, h, sid, nil); e != nil {
				return e
			}
			env = append(env, "LAN_SNAPSHOT_ID="+string(sid))
		}
		cmd = exec.CommandContext(ctx, filepath.Join(src, "scripts", "gate.sh"), args[2:]...)
	}
	cmd.Env = env
	var e error
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if e = cmd.Run(); e != nil {
		if ex, ok := errors.AsType[*exec.ExitError](e); ok {
			return ctlExit(ex.ExitCode())
		}
	}
	return e
}

func verifySourceBase(ctx context.Context, src string, req *jobSubmission, o ctlOptions) error {
	m, e := loadSnapshot(req.Snapshot)
	if e != nil {
		return e
	}
	root, e := m.sourceRoot(ctx, src)
	if e != nil {
		return e
	}
	req.Sources = map[string]string{}
	req.Branches = map[string]*string{}
	for _, r := range m.Repositories {
		req.Sources[r.Path] = filepath.Join(root, r.Path)
		if r.Head == nil {
			if o.cleanBase {
				return fmt.Errorf("missing base commit: %s", r.Path)
			}
			continue
		}
		g := snapGit{ctx: ctx, dir: filepath.Join(root, r.Path), env: []string{"GIT_OPTIONAL_LOCKS=0"}}
		head, e := g.get("rev-parse", "HEAD")
		if e != nil {
			return e
		}
		if head == *r.Head {
			branch := probe(ctx, "git", "-C", g.dir, "symbolic-ref", "--quiet", "--short", "HEAD")
			if branch != "" {
				req.Branches[r.Path] = &branch
			}
		}
		if o.snapshot == "" && head != *r.Head {
			return fmt.Errorf("HEAD moved: %s", r.Path)
		}
		dirty, e := g.get(r.excludeIncludes([]string{"status", "--porcelain", "-z", "--untracked-files=all", "--", "."})...)
		if e != nil {
			return e
		}
		if dirty == "" && r.Base != "" {
			store, e := snapStore()
			if e != nil {
				return e
			}
			g.dir = r.bare(store)
			dirty, e = g.get(r.excludeIncludes([]string{"diff", "--name-only", r.Base, r.OID, "--"})...)
			if e != nil {
				return e
			}
		}
		if dirty != "" {
			if o.cleanBase {
				return fmt.Errorf("dirty source: %s", r.Path)
			}
			diagnosticf("cx: uncommitted changes in %s; using frozen worktree\n", r.Path)
		}
	}
	return nil
}
