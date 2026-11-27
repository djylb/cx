package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
)

const refreshKind = "refresh"

func refreshInstallPath(work string) string {
	return filepath.Join(work, "input", "refresh-install.json")
}

func checkRefreshInstall(work string, base jobBaseline) error {
	var request string
	err := readJSON(refreshInstallPath(work), &request)
	if os.IsNotExist(err) || err == nil && request == base.Request {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("baseline refresh installation was interrupted; inspect the checkout before resuming")
}

func nodeRefresh(ctx context.Context, args []string) error {
	if len(args) != 4 || args[2] == "" {
		return errors.New("invalid refresh request")
	}
	var req refreshRequest
	if err := json.Unmarshal([]byte(args[3]), &req); err != nil {
		return err
	}
	if !snapHex(string(req.Snapshot), 64) || req.Source == "" {
		return errors.New("invalid refresh snapshot or source")
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
	if s.RunID != args[1] {
		return errJobReplaced
	}
	record, err := newRecord(refreshKind, req)
	if err != nil {
		return err
	}
	record.ID = args[2]
	_, err = appendInbox(ctx, dir, &identity, record, func(_ *stateRecord, previous stateRecord, current status) error {
		if previous.Seq != 0 {
			return nil
		}
		_, engineErr := liveAgentEngine(current)
		return engineErr
	})
	return err
}

func (r *runner) refreshBeforeTurn() (bool, error) {
	if err := r.refreshInbox(); err != nil {
		return false, err
	}
	last := -1
	for i, record := range slices.Backward(r.inbox.queue) {
		if record.Kind == refreshKind {
			last = i
			break
		}
	}
	if last < 0 {
		return false, nil
	}
	for i, record := range r.inbox.queue[:last+1] {
		if record.Kind != refreshKind {
			continue
		}
		if err := r.syncInboxThrough(record.position); err != nil {
			return false, err
		}
		if i != last {
			if err := r.ackInbox(record, "withdrawn"); err != nil {
				return false, err
			}
			continue
		}
		question, err := r.installRefresh(record)
		if err != nil {
			if r.ctx.Err() != nil || errors.Is(err, errApplyRollback) {
				return false, err
			}
			question = "Baseline refresh failed.\n\n" + err.Error()
		}
		if question != "" {
			id := "refresh-" + record.ID
			if r.s.LastCompleted != id {
				if refreshErr := r.withdrawRefreshInputs(); refreshErr != nil {
					return false, refreshErr
				}
				if finishErr := r.finish(turn{ID: id, Status: "completed", Items: []item{{
					Type: "agentMessage", Text: "STATUS: QUESTION\n\n" + question,
				}}}); finishErr != nil {
					return false, finishErr
				}
			}
		}
		outcome := "delivered"
		if err != nil {
			outcome = "failed"
		}
		if ackErr := r.ackInbox(record, outcome); ackErr != nil {
			return false, ackErr
		}
		if question != "" {
			return true, nil
		}
	}
	return false, nil
}

// A question requires fresh instructions from the managing session. Earlier
// follow-ups were queued before the manager could see the merge outcome.
func (r *runner) withdrawRefreshInputs() error {
	if err := r.refreshInbox(); err != nil {
		return err
	}
	for _, record := range r.inbox.queue {
		if record.Kind == "msg" {
			if err := r.ackInbox(record, "withdrawn"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *runner) installRefresh(record stateRecord) (string, error) {
	var req refreshRequest
	if err := json.Unmarshal(record.Value, &req); err != nil {
		return "", err
	}
	work, err := workDir(r.dir, r.s)
	if err != nil {
		return "", err
	}
	prep, found, err := readPreparedSnapshot(r.ctx, work)
	if err != nil {
		return "", err
	}
	if !found || prep.Source != r.c.Dir {
		return "", errors.New("refresh requires an owned snapshot checkout")
	}
	base, err := readJobBaseline(work)
	if err != nil {
		return "", err
	}
	if base.Request == record.ID {
		return base.Question, nil
	}
	if base.Snapshot == req.Snapshot && base.Source == req.Source {
		return "", nil
	}
	if r.s.Finished > 0 && r.s.LastCompleted != "" {
		if _, err = sealTurn(r.ctx, r.dir, r.s, r.s.Finished); err != nil {
			return "", err
		}
	}
	next := jobBaseline{refreshRequest: req, FromTurn: r.s.Finished + 1, Time: time.Now().UTC(), Request: record.ID}
	return refreshCheckout(r.ctx, work, next)
}

func refreshCheckout(ctx context.Context, work string, next jobBaseline) (string, error) {
	old, err := readCheckoutManifest(work)
	if err != nil {
		return "", err
	}
	newManifest, err := loadSnapshot(next.Snapshot)
	if err != nil {
		return "", err
	}
	if old.Main != newManifest.Main || !reflect.DeepEqual(old.Workspace, newManifest.Workspace) ||
		len(old.Repositories) != len(newManifest.Repositories) {
		return "", errors.New("refresh changes snapshot workspace layout")
	}
	store, err := snapStore()
	if err != nil {
		return "", err
	}
	for i, previous := range old.Repositories {
		repo := newManifest.Repositories[i]
		if repo.Path != previous.Path || repo.RepoID != previous.RepoID || repo.key() != previous.key() || !slices.Equal(repo.Include, previous.Include) {
			return "", fmt.Errorf("refresh changes snapshot repository at %s", previous.Path)
		}
	}
	if err = validateSnapshotRepos(ctx, store, newManifest); err != nil {
		return "", err
	}
	tree, err := filepath.EvalSymlinks(filepath.Join(work, "work", "tree"))
	if err != nil {
		return "", err
	}
	for _, repo := range old.Repositories {
		if err = checkRefreshRepo(ctx, tree, store, repo); err != nil {
			return "", err
		}
	}
	result, err := Collect(ctx, work)
	if err != nil {
		return "", err
	}
	defer removeTreeBestEffort(result.Dir)
	patches := make(map[string]SnapshotPatch)
	for _, patch := range result.Repositories {
		patches[patch.Path] = patch
	}
	var plans []applyPlan
	var conflicts []string
	for i, previous := range old.Repositories {
		repo := newManifest.Repositories[i]
		src := filepath.Join(tree, repo.Path)
		scratch := filepath.Join(result.Dir, fmt.Sprint(i))
		plan, planErr := planRefresh(ctx, src, scratch, result.Dir, previous, repo, patches[repo.Path])
		if planErr != nil {
			return "", fmt.Errorf("%s: %w", repo.Path, planErr)
		}
		for _, path := range plan.conflicts {
			conflicts = append(conflicts, filepath.ToSlash(filepath.Join(repo.Path, path)))
		}
		plans = append(plans, plan)
	}
	if len(conflicts) > 0 {
		var report strings.Builder
		report.WriteString("Baseline refresh has conflicts. How should the job resolve these files?\n")
		for _, path := range conflicts {
			fmt.Fprintf(&report, "\n- %q", path)
		}
		next.Question = report.String()
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	// Let installation and its rollback finish when the runner is canceled.
	commitCtx := context.WithoutCancel(ctx)
	for i := range plans {
		plans[i].git.ctx, plans[i].work.ctx = commitCtx, commitCtx
	}
	marker := refreshInstallPath(work)
	if err = writeJSON(marker, next.Request); err != nil {
		return "", err
	}
	err = installPlans(plans, func() error {
		return commitRefresh(commitCtx, work, next, plans, newManifest)
	})
	if !errors.Is(err, errApplyRollback) {
		if removeErr := os.Remove(marker); removeErr != nil && err != nil {
			return "", errors.Join(err, errApplyRollback, removeErr)
		}
	}
	return next.Question, err
}

func checkRefreshRepo(ctx context.Context, tree, store string, repo SnapshotRepo) error {
	src := filepath.Join(tree, repo.Path)
	physical, err := filepath.EvalSymlinks(src)
	if err != nil {
		return err
	}
	if src != physical {
		return fmt.Errorf("snapshot repository root changed: %s", repo.Path)
	}
	g := snapGit{ctx: ctx, dir: src}
	common, err := g.get("rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return err
	}
	bare, err := filepath.EvalSymlinks(repo.bare(store))
	if err != nil {
		return err
	}
	if filepath.Clean(common) != bare {
		return fmt.Errorf("snapshot repository metadata changed: %s", repo.Path)
	}
	return nil
}

func planRefresh(ctx context.Context, src, scratch, result string, old, next SnapshotRepo, patch SnapshotPatch) (applyPlan, error) {
	g := snapGit{ctx: ctx, dir: src}
	p := applyPlan{src: src, git: g}
	var err error
	p.index, err = g.get("rev-parse", "--path-format=absolute", "--git-path", "index")
	if err != nil {
		return p, err
	}
	p.head, err = g.get("rev-parse", "--verify", "HEAD")
	if err != nil {
		return p, err
	}
	if symbolic, _ := g.get("symbolic-ref", "-q", "HEAD"); symbolic != "" {
		return p, fmt.Errorf("refresh requires a detached HEAD (currently %s)", symbolic)
	}
	p.original, err = os.ReadFile(p.index)
	if err != nil {
		return p, err
	}
	if err = os.Mkdir(scratch, 0700); err != nil {
		return p, err
	}
	p.work = g
	p.work.env = []string{"GIT_INDEX_FILE=" + filepath.Join(scratch, "work-index")}
	// Check the real index before reconstructing the cumulative worktree.
	if _, err = g.get("write-tree"); err != nil {
		return p, err
	}
	g.env = []string{"GIT_INDEX_FILE=" + filepath.Join(scratch, "index")}
	ours, err := resultTree(g, old.OID, result, turnPatch{SnapshotPatch: patch})
	if err != nil {
		return p, err
	}
	ancestor, err := g.get("rev-parse", old.OID+"^{tree}")
	if err != nil {
		return p, err
	}
	theirs, err := g.get("rev-parse", next.OID+"^{tree}")
	if err != nil {
		return p, err
	}
	merged, stages, err := mergeApplyTrees(g, ancestor, theirs, ours)
	if err != nil {
		return p, err
	}
	if _, err = p.work.get("read-tree", ours); err != nil {
		return p, err
	}
	if _, err = g.get("read-tree", merged); err != nil {
		return p, err
	}
	p.delta, err = g.input(nil, append(append([]string{}, patchDiff...), ours)...)
	if err != nil {
		return p, err
	}
	if len(p.delta) > 0 {
		if _, err = p.work.get("update-index", "--refresh"); err != nil {
			return p, err
		}
		if _, err = p.work.input(p.delta, "apply", "--check", "--index", "-"); err != nil {
			return p, err
		}
	}
	if _, err = g.get("read-tree", next.OID); err != nil {
		return p, err
	}
	var info bytes.Buffer
	for _, stage := range stages {
		_, name, _ := strings.Cut(stage, "\t")
		if !slices.Contains(p.conflicts, name) {
			p.conflicts = append(p.conflicts, name)
			fmt.Fprintf(&info, "0 %s\t%s%c", strings.Repeat("0", len(next.OID)), name, 0)
		}
		info.WriteString(stage)
		info.WriteByte(0)
	}
	if info.Len() != 0 {
		if _, err = g.input(info.Bytes(), "update-index", "-z", "--index-info"); err != nil {
			return p, err
		}
	}
	p.final, err = os.ReadFile(filepath.Join(scratch, "index"))
	return p, err
}

func commitRefresh(ctx context.Context, work string, next jobBaseline, plans []applyPlan, manifest Manifest) (err error) {
	changed := 0
	defer func() {
		if err == nil {
			return
		}
		recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		for i := changed - 1; i >= 0; i-- {
			g := plans[i].git
			g.ctx = recovery
			_, rollbackErr := g.get("update-ref", "--no-deref", "HEAD", plans[i].head, manifest.Repositories[i].OID)
			if rollbackErr != nil {
				err = errors.Join(err, errApplyRollback, rollbackErr)
			}
		}
	}()
	for i, plan := range plans {
		if _, err = plan.git.get("update-ref", "--no-deref", "HEAD", manifest.Repositories[i].OID, plan.head); err != nil {
			return err
		}
		changed++
	}
	err = writeJSON(filepath.Join(work, "input", "baseline.json"), next)
	if err != nil {
		// A directory sync failure can follow a successful rename.
		if published, readErr := readJobBaseline(work); readErr == nil && published.Request == next.Request {
			return nil
		}
	}
	return err
}
