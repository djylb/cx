package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Before the first install there is no node protocol. Restrict these probes
// to stock OS commands and report unavailable measurements as unknown.
func preinstallDoctor(ctx context.Context, h endpoint) (fields, error) {
	if h.relay != nil {
		spec, e := json.Marshal(h)
		if e != nil {
			return nil, e
		}
		r, e := h.relay.call(ctx, 30*time.Second, "preinstall-doctor", string(spec))
		if e != nil {
			return nil, e
		}
		if r.code != 0 {
			return nil, fmt.Errorf("relay doctor exited %d", r.code)
		}
		var f fields
		e = json.Unmarshal(r.data, &f)
		return f, e
	}
	plat := h.Platform
	if plat == "" {
		var e error
		plat, e = detectPlatform(ctx, h)
		if e != nil {
			return nil, e
		}
	}
	if strings.HasPrefix(plat, "windows") {
		return preinstallWindows(ctx, h)
	}
	script := `root=$1
[ -n "$root" ] || root="${CXRUN_STATE:-$HOME/` + defaultStateDir + `}"
printf 'root\t%s\n' "$root"
for tool in git ssh tmux; do printf '%s\t%s\n' "$tool" "$(command -v "$tool")"; done
printf 'cx_path\t%s\n' "$(command -v "$2")"
test -d "$root" && test -w "$root" && echo state_writable
test -f "$root/host.json" && echo host_json_present
cache=${GOCACHE:-$(go env GOCACHE 2>/dev/null)}
printf 'gocache\t%s\n' "$cache"
test -d "$cache" && test -w "$cache" && echo gocache_writable
if test "$(uname -s)" = Linux; then
 test "$(stat -c %u "$root" 2>/dev/null)" = "$(id -u)" && echo state_owned_by_user
 test -r /proc/loadavg && awk '{print "load1\t" $1}' /proc/loadavg
else sysctl -n vm.loadavg 2>/dev/null | awk '{print "load1\t" $2}'; fi
df -Pk "$HOME" | awk 'NR==2{printf "disk_free_bytes\t%.0f\n",$4*1024}'
` + engineDoctorSh() + `exit 0
`
	b, e := h.rawSSH(ctx, "ssh", []string{"--", h.SSH, "sh -s -- " + shellQuote(h.State) + " " + shellQuote(h.CX)}, script, 25*time.Second)
	if e != nil {
		return nil, e
	}
	f := fields{"cx_version": nil, "platform": plat, "node_available": false, "state_writable": false, "gocache_writable": false, "host_json_present": false}
	engineRows := fields{}
	for _, e := range engines {
		engineRows[e.name] = fields{"path": nil, "login_status": nil}
	}
	f["engines"] = engineRows
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		if name, rest, ok := strings.Cut(strings.TrimPrefix(line, "engine\t"), "\t"); ok && strings.HasPrefix(line, "engine\t") {
			path, login, _ := strings.Cut(rest, "\t")
			engineRows[name] = fields{"path": stringOrNil(path), "login_status": stringOrNil(login)}
			continue
		}
		k, v, ok := strings.Cut(line, "\t")
		if ok {
			switch k {
			case "root":
				f["state_root"] = v
			case "disk_free_bytes", "load1":
				num, e := strconv.ParseFloat(v, 64)
				if e == nil {
					f[k] = num
				}
			default:
				f[k] = stringOrNil(v)
			}
			continue
		}
		switch k {
		case "state_writable", "host_json_present", "gocache_writable", "state_owned_by_user":
			f[k] = true
		}
	}
	if f["state_root"] == nil {
		return nil, errors.New("invalid preinstall doctor response")
	}
	return f, nil
}
func preinstallWindows(ctx context.Context, h endpoint) (fields, error) {
	code := "$root=" + psQuote(h.State) + ";" + `if(!$root){$root=$env:CXRUN_STATE};if(!$root){$root=Join-Path $env:LOCALAPPDATA ` + psQuote(defaultWindowsStateDir) + `}
 $f=@{platform='windows';state_root=$root;cx_version=$null;node_available=$false;host_json_present=(Test-Path (Join-Path $root host.json))}
 foreach($tool in ('git','ssh','psmux')){$f[$tool]=(Get-Command $tool -ErrorAction SilentlyContinue).Source}
 ` + engineDoctorPS() + `$f.engines=$engines
 if($f.psmux){$socket=$env:CXRUN_TMUX_SOCKET;if(!$socket){$socket='cx'};$f.psmux_session=[bool](& $f.psmux -L $socket list-sessions 2>$null)}
 $cache=$env:GOCACHE;if(!$cache -and (Get-Command go -ErrorAction SilentlyContinue)){$cache=go env GOCACHE}
 $f.gocache=$cache;foreach($item in @(@('state_writable',$root),@('gocache_writable',$cache))){$f[$item[0]]=$false;if($item[1] -and (Test-Path -LiteralPath $item[1] -PathType Container)){try{$temp=Join-Path $item[1] ([Guid]::NewGuid());$s=[IO.File]::Open($temp,'CreateNew','Write','None');$s.Dispose();[IO.File]::Delete($temp);$f[$item[0]]=$true}catch{}}}
 $f.disk_free_bytes=(Get-PSDrive -Name ([IO.Path]::GetPathRoot($HOME).Substring(0,1))).Free
 $f.cpu_usage_percent=(Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue|Measure-Object LoadPercentage -Average).Average
 $f|ConvertTo-Json -Compress;exit 0`
	b, e := h.rawSSH(ctx, "ssh", []string{"--", h.SSH, psCommand(code)}, "", 25*time.Second)
	if e != nil {
		return nil, e
	}
	var obj fields
	e = json.Unmarshal(b, &obj)
	return obj, e
}
