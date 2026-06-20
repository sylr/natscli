// Copyright 2020-2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"context"
	"embed"
	"errors"
	"fmt"
	glog "log"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/jsm.go/natscontext"

	"github.com/nats-io/natscli/options"

	"github.com/spf13/cobra"
)

type command struct {
	Name    string
	Order   int
	Command func(app commandHost)
}

// Logger provides a pluggable logger implementation
type Logger interface {
	Printf(format string, a ...any)
	Fatalf(format string, a ...any)
	Print(a ...any)
	Fatal(a ...any)
	Println(a ...any)
}

var (
	commands = []*command{}
	mu       sync.Mutex
	Version  = "development"
	log      Logger
	ctx      context.Context

	//go:embed cheats
	fs embed.FS

	// These are persisted by contexts, as properties thereof.
	// So don't include NATS_CONTEXT in this list.
	overrideEnvVars = []string{"NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CREDS", "NATS_NKEY", "NATS_CERT", "NATS_KEY", "NATS_CA", "NATS_TIMEOUT", "NATS_SOCKS_PROXY", "NATS_COLOR"}
)

func registerCommand(name string, order int, c func(app commandHost)) {
	mu.Lock()
	commands = append(commands, &command{name, order, c})
	mu.Unlock()
}

// SkipContexts used during tests
var SkipContexts bool

func SetVersion(v string) {
	mu.Lock()
	defer mu.Unlock()

	Version = v
}

// SetLogger sets a custom logger to use
func SetLogger(l Logger) {
	mu.Lock()
	defer mu.Unlock()

	log = l
}

// SetContext sets the context to use
func SetContext(c context.Context) {
	mu.Lock()
	defer mu.Unlock()

	ctx = c
}

func commonConfigure(cmd commandHost, cliOpts *options.Options, disable ...string) error {
	if cliOpts != nil {
		options.DefaultOptions = cliOpts
	} else {
		options.DefaultOptions = &options.Options{
			Timeout: 5 * time.Second,
		}
	}

	if options.DefaultOptions.PrometheusNamespace == "" {
		options.DefaultOptions.PrometheusNamespace = "nats_server_check"
	}

	ctx = context.Background()
	log = goLogger{}

	sort.Slice(commands, func(i int, j int) bool {
		return commands[i].Name < commands[j].Name
	})

	shouldEnable := func(name string) bool {
		for _, d := range disable {
			if d == name {
				return false
			}
		}

		return true
	}

	for _, c := range commands {
		if shouldEnable(c.Name) {
			c.Command(cmd)
		}
	}

	// Attach stream-name completion to every --stream flag now that the full
	// command tree (and its flags) has been built.
	registerFlagCompletions(cmd)

	return nil
}

// ConfigureInCommand attaches the cli commands to cmd, prepare will load the context on demand and should be true unless override nats,
// manager and js context is given in a custom PersistentPreRunE in the caller.  Disable is a list of command names to skip.
func ConfigureInCommand(cmd *cobra.Command, cliOpts *options.Options, prepare bool, disable ...string) (*options.Options, error) {
	err := commonConfigure(cmd, cliOpts, disable...)
	if err != nil {
		return nil, err
	}

	chainPersistentPreRunE(cmd, prepare)

	return options.DefaultOptions, nil
}

// ConfigureInApp attaches the cli commands to app, prepare will load the context on demand and should be true unless override nats,
// manager and js context is given in a custom PersistentPreRunE in the caller.  Disable is a list of command names to skip.
func ConfigureInApp(app *cobra.Command, cliOpts *options.Options, prepare bool, disable ...string) (*options.Options, error) {
	err := commonConfigure(app, cliOpts, disable...)
	if err != nil {
		return nil, err
	}

	chainPersistentPreRunE(app, prepare)

	return options.DefaultOptions, nil
}

// chainPersistentPreRunE installs the root pre-run hook that applies environment
// variable backed flags and, when prepare is set, loads the NATS context. Any
// hook already present on the command is preserved and run afterwards.
func chainPersistentPreRunE(cmd *cobra.Command, prepare bool) {
	// fisk ran PreActions additively (parent and child). cobra by default runs
	// only the nearest PersistentPreRunE, which would let a subcommand's hook
	// (e.g. server check's format parser) shadow the root context loader.
	// Enabling traverse hooks restores the additive behavior.
	cobra.EnableTraverseRunHooks = true

	existing := cmd.PersistentPreRunE
	cmd.PersistentPreRunE = func(c *cobra.Command, args []string) error {
		if err := applyEnvVars(c); err != nil {
			return err
		}
		propagateNegatableChanged(c)
		if prepare {
			if err := preAction(); err != nil {
				return err
			}
		}
		if existing != nil {
			return existing(c, args)
		}
		return nil
	}
}

func preAction() (err error) {
	err = loadContext(true)
	if errors.Is(err, ErrContextNotFound) {
		fmt.Printf("The selected context %q was not found, unselecting it\n", natscontext.SelectedContext())
		natscontext.UnSelectContext()
		fmt.Println()
		return err
	}
	return nil
}

type goLogger struct{}

func (goLogger) Fatalf(format string, a ...any) { glog.Fatalf(format, a...) }
func (goLogger) Printf(format string, a ...any) { glog.Printf(format, a...) }
func (goLogger) Print(a ...any)                 { glog.Print(a...) }
func (goLogger) Println(a ...any)               { glog.Println(a...) }
func (goLogger) Fatal(a ...any)                 { glog.Fatal(a...) }

func opts() *options.Options {
	return options.DefaultOptions
}
