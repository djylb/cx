package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/djylb/timeshim"
)

const acceptanceFilename = "acceptance.json"

type turnAcceptance struct {
	Status string             `json:"status"`
	Checks []acceptanceResult `json:"checks,omitempty"`
}

func (r *runner) acceptSealedTurn(folder string) error {
	identity, _, _, err := captureJobIdentity(r.dir)
	if err != nil {
		return err
	}
	// The acceptance goroutine must not share maps the runner keeps mutating.
	c := r.c
	c.Env = maps.Clone(r.c.Env)
	if r.c.Work != nil {
		work := *r.c.Work
		c.Work = &work
	}
	dir, number := r.dir, r.s.Finished
	ctx := r.auxiliaryCtx
	r.auxiliary.Go(func() {
		result := runTurnAcceptance(ctx, dir, folder, c)
		publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlCallIdle)
		defer cancel()
		if err := publishAcceptance(publishCtx, dir, identity, folder, number, result); err != nil {
			diagnosticf("cx: acceptance for turn %d: %v\n", number, err)
		}
	})
	return nil
}

func runTurnAcceptance(ctx context.Context, dir, folder string, c config) turnAcceptance {
	result := turnAcceptance{Status: "unknown"}
	var meta turnResult
	if err := readJSON(filepath.Join(folder, "result.json"), &meta); err != nil {
		return result
	}
	setup, cancel := context.WithTimeout(ctx, moduleSetupTimeout)
	defer cancel()
	scratch, err := prepareAuxiliaryInput(setup, dir, meta.Baseline.Snapshot, folder, &meta)
	if err != nil {
		diagnosticf("cx: prepare acceptance for turn %d: %v\n", meta.Turn, err)
		if ctx.Err() != nil {
			result.Status = "interrupted"
		}
		return result
	}
	defer removeTreeBestEffort(scratch.dir)
	result.Status = "passed"
	timeout := c.Work.AcceptTimeout
	if timeout <= 0 {
		timeout = defaultAcceptTimeout
	}
	run := func(module string, argv []string, shell bool, cwd string) {
		if ctx.Err() != nil {
			result.Status = "interrupted"
			return
		}
		var tail acceptanceTail
		started := time.Now()
		code, runErr := runModuleCommand(ctx, nil, &c, dir, argv, shell, cwd, scratch.env, timeout, nil, &tail, nil)
		text := ""
		truncated := errors.Is(runErr, errOutputTruncated)
		if !truncated {
			if runErr != nil {
				_, _ = fmt.Fprintf(&tail, "\n%s", c.secret.text(runErr.Error()))
			}
			text, truncated = string(tail.commandTail), tail.truncated
		}
		command := make([]string, len(argv))
		for i, arg := range argv {
			command[i] = c.secret.text(arg)
		}
		result.Checks = append(result.Checks, acceptanceResult{Module: module, Cmd: command, Exit: code,
			Duration: time.Since(started).Milliseconds(), Tail: text, Truncated: truncated})
		if ctx.Err() != nil {
			result.Status = "interrupted"
		} else if runErr != nil || code != 0 {
			result.Status = "failed"
		}
	}
	for _, module := range c.Work.Definitions {
		for _, argv := range module.Accept {
			run(module.Name, argv, false, c.Work.repoDir(scratch.cwd))
		}
	}
	for _, script := range c.Work.AcceptScripts {
		run("", []string{script}, true, scratch.cwd)
	}
	return result
}

type acceptanceTail struct {
	commandTail
	truncated bool
}

func (t *acceptanceTail) Write(p []byte) (int, error) {
	t.truncated = t.truncated || len(t.commandTail)+len(p) > outputTailBytes
	return t.commandTail.Write(p)
}

func readAcceptance(folder string) (turnAcceptance, error) {
	var result turnAcceptance
	err := readJSON(filepath.Join(folder, acceptanceFilename), &result)
	return result, err
}

func publishAcceptance(ctx context.Context, dir string, identity jobIdentity, folder string, number int64, answer turnAcceptance) error {
	if err := identity.checkDir(dir); err != nil {
		return err
	}
	data, err := json.Marshal(answer)
	if err != nil {
		return err
	}
	if err = publishImmutable(filepath.Join(folder, acceptanceFilename), data, folder); err != nil {
		return err
	}
	return publishTurnEvent(ctx, dir, identity, number, &agentMail{
		Type: "acceptance", To: "parent", Text: fmt.Sprintf("Acceptance for turn %d: %s", number, answer.Status),
		Acceptance: answer.Status, Turn: number,
	})
}

func publishTurnEvent(ctx context.Context, dir string, identity jobIdentity, number int64, event *agentMail) error {
	op := event.Type + "-" + snapSum([]byte(identity.run+"/"+strconv.FormatInt(number, 10)))
	return appendMail(ctx, dir, identity, op, event)
}

func (r *runner) recoverAuxiliaryWorkspace() error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.baseContext()), moduleSetupTimeout)
	defer cancel()
	staged, err := filepath.Glob(filepath.Join(r.dir, jobTempDir, "cx-aux-*"))
	if err != nil {
		return err
	}
	work, err := workDir(r.dir, r.s)
	if err != nil {
		return err
	}
	contextStages, err := filepath.Glob(filepath.Join(work, "context", "modules", ".stage-*"))
	if err != nil {
		return err
	}
	staged = append(staged, contextStages...)
	if len(staged) != 0 {
		if err = r.reclaim(); err != nil {
			return err
		}
	}
	for _, path := range staged {
		if removeErr := os.RemoveAll(path); removeErr != nil {
			return removeErr
		}
	}
	identity, _, _, err := captureJobIdentity(r.dir)
	if err != nil {
		return err
	}
	return replayTurnEvents(ctx, r.dir, r.s, identity, true)
}

func replayTurnEvents(ctx context.Context, dir string, s status, identity jobIdentity, recover bool) error {
	records, err := completionCheckpoints(dir)
	if err != nil {
		return err
	}
	work, err := workDir(dir, s)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		var completed turnCompletion
		if err := json.Unmarshal(record.Value, &completed); err != nil {
			return err
		}
		details, err := readTurnMetadata(dir, completed.TurnID)
		if err != nil {
			diagnosticf("cx: read turn %d observations: %v\n", completed.Number, err)
		}
		if warning := details.ContextWarning; warning != nil {
			if eventErr := publishTurnEvent(ctx, dir, identity, completed.Number, &agentMail{Type: "context_warning", To: "parent",
				Text: warning.text(), ContextWarning: warning}); eventErr != nil {
				return eventErr
			}
		}
		folder := filepath.Join(work, "result", fmt.Sprintf("turn-%d", completed.Number))
		result, err := readAcceptance(folder)
		if os.IsNotExist(err) {
			if recover && details.AcceptanceStatus == "pending" {
				if _, statErr := os.Stat(filepath.Join(folder, "result.json")); statErr == nil {
					if err = publishAcceptance(ctx, dir, identity, folder, completed.Number, turnAcceptance{Status: "unknown"}); err != nil {
						return err
					}
				}
			}
			continue
		}
		if err != nil {
			return err
		}
		if err := publishTurnEvent(ctx, dir, identity, completed.Number, &agentMail{
			Type: "acceptance", To: "parent", Text: fmt.Sprintf("Acceptance for turn %d: %s", completed.Number, result.Status),
			Acceptance: result.Status, Turn: completed.Number,
		}); err != nil {
			return err
		}
	}
	return nil
}

func nodeResultAcceptance(ctx context.Context, args []string) error {
	if len(args) != 3 {
		return errors.New("invalid acceptance selection")
	}
	n, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || n < 0 {
		return errors.New("invalid acceptance turn")
	}
	root, err := stateRoot()
	if err != nil {
		return err
	}
	dir, err := jobDir(root, args[0])
	if err != nil {
		return err
	}
	identity, s, _, err := captureJobIdentity(dir)
	if err != nil {
		return err
	}
	if identity.run != args[1] {
		return errJobReplaced
	}
	work, err := workDir(dir, s)
	if err != nil {
		return err
	}
	folder := filepath.Join(work, "result", fmt.Sprintf("turn-%d", n))
	var meta turnResult
	if err = readJSON(filepath.Join(folder, "result.json"), &meta); err != nil {
		return err
	}
	answer, err := readAcceptance(folder)
	if os.IsNotExist(err) {
		answer.Status, err = meta.AcceptanceStatus, nil
		if answer.Status == "pending" {
			// Ownership is also the acceptance lifetime. Recovery must
			// never replace a receipt while a current owner can publish it.
			owner, lockErr := openJobOwner(ctx, dir, jobFileLockExisting)
			if lockErr != nil {
				return lockErr
			}
			if owner != nil {
				defer closeBestEffort(owner)
				if err = identity.checkDir(dir); err != nil {
					return err
				}
				answer, err = readAcceptance(folder)
				if os.IsNotExist(err) {
					r := runner{ctx: ctx, root: root, dir: dir, s: s, c: config{RunID: s.RunID}}
					if err = r.recoverAuxiliaryWorkspace(); err != nil {
						return err
					}
					answer, err = readAcceptance(folder)
				}
			}
		}
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(answer)
}

func collectTurnAcceptance(ctx context.Context, h endpoint, id, dest string, target observedResult) error {
	if _, err := readAcceptance(dest); !os.IsNotExist(err) {
		return err
	}
	var meta turnResult
	if err := readJSON(filepath.Join(dest, "result.json"), &meta); err != nil {
		return err
	}
	if meta.AcceptanceStatus != "pending" {
		return nil
	}
	bounded, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), controlCallIdle)
	defer cancel()
	response, err := h.callIdle(bounded, "collect acceptance receipt", "result-acceptance", id, target.RunID, strconv.FormatInt(target.Number, 10))
	if err != nil {
		return err
	}
	if response.code != 0 {
		return ctlExit(response.code)
	}
	var answer turnAcceptance
	if err = json.Unmarshal(response.data, &answer); err != nil || answer.Status == "pending" {
		return err
	}
	data, err := json.Marshal(answer)
	if err != nil {
		return err
	}
	return snapImmutable(filepath.Join(dest, acceptanceFilename), data)
}

func addAcceptanceToResult(dir string, meta *turnResult) {
	if answer, err := readAcceptance(dir); err == nil {
		meta.AcceptanceStatus, meta.Acceptance = answer.Status, answer.Checks
	}
	if meta.AcceptanceStatus == "" {
		meta.AcceptanceStatus = "none"
	}
}

func replayOutboxTurnEvents(ctx context.Context, dir string) error {
	identity, s, _, err := captureJobIdentity(dir)
	if err != nil {
		return err
	}
	return replayTurnEvents(ctx, dir, s, identity, false)
}
