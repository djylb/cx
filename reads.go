package main

import (
	"encoding/json"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

type readObservation struct {
	Path   string `json:"path"`
	Source string `json:"source"`
}

type turnReads struct {
	Status string            `json:"status"`
	Paths  []readObservation `json:"paths"`
	Turn   int64             `json:"turn"`
}

type readTracker struct {
	turn  string
	roots []string
	paths []readObservation
	seen  map[string]int
}

func (r *runner) recordReadPaths(turnID, cwd string, paths []string, source string) {
	if turnID == "" || len(paths) == 0 {
		return
	}
	tracker := &r.reads
	if tracker.turn != turnID {
		tracker.turn, tracker.paths, tracker.seen = turnID, nil, make(map[string]int)
	}
	if tracker.roots == nil {
		tracker.roots, _ = snapshotMemberRoots(r.root, r.c.Dir)
		if len(tracker.roots) == 0 {
			tracker.roots = []string{r.c.Dir}
		}
	}
	if cwd == "" {
		cwd = r.c.Dir
	} else if !filepath.IsAbs(cwd) {
		cwd = filepath.Join(r.c.Dir, cwd)
	}
	for _, name := range paths {
		if name == "" || name == "-" || strings.ContainsAny(name, "\x00\r\n") {
			continue
		}
		if !filepath.IsAbs(name) {
			name = filepath.Join(cwd, name)
		}
		if !slices.ContainsFunc(tracker.roots, func(root string) bool { return snapWithin(root, name) }) {
			continue
		}
		rel, err := filepath.Rel(r.c.Dir, name)
		if err != nil {
			continue
		}
		rel = r.c.secret.text(filepath.ToSlash(rel))
		key := rel
		if foldTargetPaths(runtime.GOOS) {
			key = strings.ToLower(key)
		}
		if index, exists := tracker.seen[key]; exists {
			if source == "structured" {
				tracker.paths[index].Source = source
			}
		} else {
			tracker.seen[key] = len(tracker.paths)
			tracker.paths = append(tracker.paths, readObservation{Path: rel, Source: source})
		}
	}
}

type toolRead struct {
	paths  []string
	source string
}

func claudeReadPaths(tool string, raw json.RawMessage) toolRead {
	var input struct {
		Command  string `json:"command"`
		File     string `json:"file_path"`
		Notebook string `json:"notebook_path"`
		Path     string `json:"path"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return toolRead{}
	}
	switch tool {
	case "Read":
		return toolRead{[]string{input.File}, "structured"}
	case "NotebookRead":
		return toolRead{[]string{input.Notebook}, "structured"}
	case "Grep", "Glob":
		return toolRead{[]string{input.Path}, "structured"}
	case "Bash":
		return toolRead{commandReadPaths(input.Command, false), "parsed"}
	case "PowerShell":
		return toolRead{commandReadPaths(input.Command, true), "parsed"}
	default:
		return toolRead{}
	}
}

type commandReadAction struct {
	Type, Path string
}

func codexReadPaths(i item) (paths []string, source string) {
	switch i.Type {
	case "fileRead", "file_read":
		return []string{i.Path}, "structured"
	case "commandExecution", "command_execution":
		if i.ExitCode == nil || *i.ExitCode != 0 {
			return nil, ""
		}
		for _, action := range i.CommandActions {
			if action.Type == "read" && action.Path != "" {
				paths = append(paths, action.Path)
			}
		}
		if len(paths) != 0 {
			return paths, "structured"
		}
		if runtime.GOOS == "windows" {
			return nil, "" // The host OS does not identify the shell.
		}
		return commandReadPaths(i.Command, false), "parsed"
	default:
		return nil, ""
	}
}

func commandReadPaths(command string, windows bool) []string {
	words, ok := literalShellWords(command, windows)
	if !ok || len(words) == 0 {
		return nil
	}
	switch path.Base(strings.ReplaceAll(words[0], "\\", "/")) {
	case "bash", "sh", "zsh":
		if len(words) == 3 && slices.Contains([]string{"-c", "-lc", "-cl"}, words[1]) {
			words, ok = literalShellWords(words[2], false)
			if !ok || len(words) == 0 {
				return nil
			}
		}
	}
	return slices.DeleteFunc(readOperands(words), func(operand string) bool {
		return strings.ContainsAny(operand, "*?[]{}~$%")
	})
}

// Deliberately accepts a single literal command. Expansions, redirections,
// pipelines and compound syntax require an interpreter and are left out.
func literalShellWords(command string, windows bool) ([]string, bool) {
	var (
		words   []string
		word    strings.Builder
		quote   byte
		started bool
	)
	for i := 0; i < len(command); i++ {
		c := command[i]
		if quote != 0 {
			switch {
			case c == quote:
				quote = 0
			case quote == '"' && (strings.ContainsRune("$`", rune(c)) || !windows && c == '\\'):
				return nil, false
			default:
				word.WriteByte(c)
			}
			continue
		}
		switch c {
		case ' ', '\t':
			if started {
				words = append(words, word.String())
				word.Reset()
				started = false
			}
		case '\'', '"':
			started, quote = true, c
		case '\\':
			if windows {
				word.WriteByte(c)
				started = true
				continue
			}
			if i+1 == len(command) || strings.ContainsRune("\r\n", rune(command[i+1])) {
				return nil, false
			}
			i++
			word.WriteByte(command[i])
			started = true
		case '|', '&', ';', '<', '>', '$', '`', '(', ')', '{', '}', '\n', '\r', '~', '#':
			return nil, false
		default:
			word.WriteByte(c)
			started = true
		}
	}
	if quote != 0 {
		return nil, false
	}
	if started {
		words = append(words, word.String())
	}
	return words, true
}

type readOptions struct {
	values, flags           string
	shortValues, shortFlags string
	pattern                 bool
}

var readCommands = map[string]readOptions{
	"cat":  {flags: "number number-nonblank show-all show-ends show-nonprinting show-tabs squeeze-blank", shortFlags: "AbEnstTuv"},
	"less": {shortValues: "x", shortFlags: "EFIMNnRSXs"},
	"nl":   {values: "body-numbering footer-numbering header-numbering line-increment join-blank-lines number-format number-separator number-width section-delimiter starting-line-number", flags: "no-renumber", shortValues: "bdfhilnsvw", shortFlags: "p"},
	"head": {values: "bytes lines", flags: "quiet silent verbose zero-terminated", shortValues: "cn", shortFlags: "qvz"},
	"tail": {values: "bytes lines max-unchanged-stats pid sleep-interval", flags: "follow quiet silent verbose retry zero-terminated", shortValues: "cns", shortFlags: "fFqvz"},
	"sed":  {values: "expression file", flags: "quiet silent regexp-extended unbuffered zero-terminated in-place", shortValues: "ef", shortFlags: "inErusz", pattern: true},
	"rg":   {values: "regexp file glob iglob type type-not max-count max-depth max-filesize context after-context before-context color colors encoding threads sort sortr replace", flags: "files hidden no-ignore no-heading heading line-number column with-filename no-filename fixed-strings ignore-case smart-case invert-match word-regexp count count-matches quiet no-messages files-with-matches files-without-match follow unrestricted glob-case-insensitive multiline multiline-dotall binary text json pretty stats only-matching", shortValues: "efgtTmABCjMrE", shortFlags: "aFHhiIlLnoNqSsuUVvwxbcz", pattern: true},
	"grep": {values: "regexp file include exclude exclude-dir exclude-from max-count context after-context before-context color binary-files label directories devices", flags: "recursive dereference-recursive line-number with-filename no-filename fixed-strings extended-regexp ignore-case invert-match word-regexp count quiet silent no-messages files-with-matches files-without-match only-matching text", shortValues: "efmABCdD", shortFlags: "aEFlLHiIhnoPqRrsvwxzZbc", pattern: true},
}

func readOperands(words []string) []string {
	name := strings.TrimSuffix(path.Base(strings.ReplaceAll(words[0], "\\", "/")), ".exe")
	opts, ok := readCommands[name]
	if !ok {
		return nil
	}
	var paths []string
	pattern, end := opts.pattern, false
	for i := 1; i < len(words); i++ {
		arg := words[i]
		if !end && arg == "--" {
			end = true
			continue
		}
		if !end && strings.HasPrefix(arg, "--") {
			key, value, assigned := strings.Cut(arg[2:], "=")
			switch {
			case strings.Contains(" "+opts.values+" ", " "+key+" "):
				if !assigned {
					i++
					if i == len(words) {
						return nil
					}
					value = words[i]
				}
				if key == "file" {
					paths = append(paths, value)
				}
				if key == "regexp" || key == "expression" || key == "file" {
					pattern = false
				}
			case strings.Contains(" "+opts.flags+" ", " "+key+" "):
				pattern = pattern && key != "files"
			default:
				return nil
			}
			continue
		}
		if !end && len(arg) > 1 && arg[0] == '-' {
			flags := arg[1:]
			if (name == "head" || name == "tail") && strings.Trim(flags, "0123456789") == "" {
				continue
			}
			for j := 0; j < len(flags); j++ {
				flag := flags[j]
				if strings.ContainsRune(opts.shortFlags, rune(flag)) {
					continue
				}
				if !strings.ContainsRune(opts.shortValues, rune(flag)) {
					return nil
				}
				value := flags[j+1:]
				if value == "" {
					i++
					if i == len(words) {
						return nil
					}
					value = words[i]
				}
				if opts.pattern && flag == 'f' {
					paths = append(paths, value)
				}
				if opts.pattern && (flag == 'e' || flag == 'f') {
					pattern = false
				}
				break
			}
			continue
		}
		if pattern {
			pattern = false
		} else {
			paths = append(paths, arg)
		}
	}
	return slices.DeleteFunc(paths, func(name string) bool { return name == "-" || strings.ContainsAny(name, "*?[") })
}
