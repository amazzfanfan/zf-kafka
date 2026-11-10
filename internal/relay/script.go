package relay

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dop251/goja"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

type ScriptJob struct {
	Script   string `json:"script"`
	Function string `json:"function"`
	Context  any    `json:"context"`
	Secret   string `json:"secret,omitempty"`
}
type scriptReply struct {
	Value any    `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}
type scriptProcess struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
}
type ScriptPool struct {
	slots chan *scriptProcess
	once  sync.Once
}

func NewScriptPool(n int) *ScriptPool {
	p := &ScriptPool{slots: make(chan *scriptProcess, n)}
	for i := 0; i < n; i++ {
		p.slots <- nil
	}
	return p
}
func newScriptProcess() (*scriptProcess, error) {
	exe, e := os.Executable()
	if e != nil {
		return nil, e
	}
	c := exec.Command(exe, "executor")
	c.Env = []string{"GOMEMLIMIT=96MiB", "GOMAXPROCS=1"}
	in, e := c.StdinPipe()
	if e != nil {
		return nil, e
	}
	out, e := c.StdoutPipe()
	if e != nil {
		return nil, e
	}
	if e = c.Start(); e != nil {
		return nil, e
	}
	return &scriptProcess{c, in, bufio.NewReaderSize(out, 2*1048576)}, nil
}
func (p *scriptProcess) kill() {
	if p != nil {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
	}
}
func (p *ScriptPool) Run(ctx context.Context, j ScriptJob) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	var proc *scriptProcess
	select {
	case proc = <-p.slots:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if proc == nil {
		var e error
		proc, e = newScriptProcess()
		if e != nil {
			p.slots <- nil
			return nil, e
		}
	}
	b, e := json.Marshal(j)
	if e != nil {
		p.slots <- proc
		return nil, e
	}
	if len(b) > 2*1048576 {
		p.slots <- proc
		return nil, errors.New("script input too large")
	}
	done := make(chan scriptReply, 1)
	go func() {
		if _, e := proc.in.Write(append(b, '\n')); e != nil {
			done <- scriptReply{Error: "executor unavailable"}
			return
		}
		line, e := proc.out.ReadSlice('\n')
		if e != nil {
			done <- scriptReply{Error: "executor output invalid"}
			return
		}
		var r scriptReply
		if json.Unmarshal(line, &r) != nil {
			r.Error = "executor response invalid"
		}
		done <- r
	}()
	select {
	case r := <-done:
		if r.Error != "" {
			proc.kill()
			p.slots <- nil
			return nil, errors.New(r.Error)
		}
		p.slots <- proc
		return r.Value, nil
	case <-ctx.Done():
		proc.kill()
		<-done
		p.slots <- nil
		return nil, errors.New("script execution deadline exceeded")
	}
}
func (p *ScriptPool) Close() {
	p.once.Do(func() {
		for i := 0; i < cap(p.slots); i++ {
			proc := <-p.slots
			proc.kill()
		}
	})
}
func RunExecutor(in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 65536), 2*1048576)
	encoder := json.NewEncoder(out)
	for scanner.Scan() {
		var j ScriptJob
		if json.Unmarshal(scanner.Bytes(), &j) != nil {
			return errors.New("invalid script job")
		}
		r := executeScript(j)
		b, e := json.Marshal(r)
		if e != nil || len(b) > 1048576 {
			r = scriptReply{Error: "script output too large or invalid"}
		}
		if e = encoder.Encode(r); e != nil {
			return e
		}
	}
	return scanner.Err()
}
func executeScript(j ScriptJob) (reply scriptReply) {
	defer func() {
		if recover() != nil {
			reply = scriptReply{Error: "script runtime failed"}
		}
	}()
	vm := goja.New()
	vm.SetMaxCallStackSize(128)
	_ = vm.Set("crypto", map[string]any{"sha256": func(text, encoding string) string {
		h := sha256.Sum256([]byte(text))
		if encoding == "base64" {
			return base64.StdEncoding.EncodeToString(h[:])
		}
		return hex.EncodeToString(h[:])
	}, "hmacSHA256": func(text, encoding string) (string, error) {
		if j.Secret == "" {
			return "", errors.New("no signing credential bound")
		}
		h := hmac.New(sha256.New, []byte(j.Secret))
		h.Write([]byte(text))
		if encoding == "base64" {
			return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}})
	_ = vm.Set("Date", goja.Undefined())
	_, _ = vm.RunString("Math.random = undefined;")
	timer := time.AfterFunc(100*time.Millisecond, func() { vm.Interrupt("timeout") })
	defer timer.Stop()
	if _, e := vm.RunString(j.Script); e != nil {
		return scriptReply{Error: "script compilation or execution failed"}
	}
	f, ok := goja.AssertFunction(vm.Get(j.Function))
	if !ok {
		return scriptReply{Error: "script must define " + j.Function}
	}
	v, e := f(goja.Undefined(), vm.ToValue(j.Context))
	if e != nil {
		return scriptReply{Error: "script function failed"}
	}
	return scriptReply{Value: v.Export()}
}
func CheckScript(src string) error {
	if src == "" {
		return nil
	}
	if _, e := goja.Compile("adapter", src, true); e != nil {
		return fmt.Errorf("script syntax invalid: %w", e)
	}
	return nil
}
