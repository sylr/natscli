// Copyright 2023-2025 The NATS Authors
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

package plugins

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	iu "github.com/nats-io/natscli/internal/util"
)

var validNames = regexp.MustCompile(`^[a-z]+$`)

type plugin struct {
	Cmd        string          `json:"cmd"`
	Definition json.RawMessage `json:"def"`
}

// applicationModel mirrors the subset of the introspection model that is emitted
// by a plugin binary's introspection output and that we need to rebuild the
// command tree on cobra. The JSON tags match the plugin model exactly.
type applicationModel struct {
	Name string `json:"name"`
	Help string `json:"help"`

	// embedded groups, matching the model's anonymous-struct embedding so the
	// "args", "flags" and "commands" keys unmarshal correctly.
	argGroupModel
	flagGroupModel
	cmdGroupModel
}

type cmdGroupModel struct {
	Commands []*cmdModel `json:"commands,omitempty"`
}

type argGroupModel struct {
	Args []*argModel `json:"args,omitempty"`
}

type flagGroupModel struct {
	Flags []*flagModel `json:"flags,omitempty"`
}

// cmdModel mirrors the model's command node. The arg/flag/command groups are
// embedded as pointers, exactly as in the source model, so the "args", "flags"
// and "commands" JSON keys are decoded into the nested groups.
type cmdModel struct {
	Name     string   `json:"name"`
	Aliases  []string `json:"aliases,omitempty"`
	Help     string   `json:"help"`
	HelpLong string   `json:"help_long,omitempty"`
	Hidden   bool     `json:"hidden,omitempty"`
	Default  bool     `json:"default,omitempty"`
	Tags     []string `json:"tags,omitempty"`

	*flagGroupModel
	*argGroupModel
	*cmdGroupModel
}

// argModel mirrors the model's argument node.
type argModel struct {
	Name        string   `json:"name"`
	Help        string   `json:"help"`
	Default     []string `json:"default,omitempty"`
	Envar       string   `json:"envar,omitempty"`
	PlaceHolder string   `json:"place_holder,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Hidden      bool     `json:"hidden,omitempty"`
	Cumulative  bool     `json:"cumulative"`
}

// flagModel mirrors the model's flag node.
type flagModel struct {
	Name        string   `json:"name"`
	Help        string   `json:"help"`
	Short       rune     `json:"short,omitempty"`
	Default     []string `json:"default,omitempty"`
	Envar       string   `json:"envar,omitempty"`
	PlaceHolder string   `json:"place_holder,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Hidden      bool     `json:"hidden,omitempty"`
	Boolean     bool     `json:"boolean"`
	Negatable   bool     `json:"negatable,omitempty"`
	Cumulative  bool     `json:"cumulative"`
}

// AddToApp scans the plugin directory for registered plugins and adds each one
// as a cobra command tree on app. Plugins that fail to load are logged and
// skipped so a single bad plugin cannot prevent the rest of the CLI starting.
func AddToApp(app *cobra.Command) error {
	parent, err := pluginDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(parent)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		pb, err := os.ReadFile(filepath.Join(parent, entry.Name()))
		if err != nil {
			log.Printf("Could not read plugin %v: %v", entry.Name(), err)
			continue
		}

		var p plugin
		err = json.Unmarshal(pb, &p)
		if err != nil {
			log.Printf("Could not read plugin %v: %v", entry.Name(), err)
			continue
		}

		var m applicationModel
		err = json.Unmarshal(p.Definition, &m)
		if err != nil {
			log.Printf("Invalid plugin %v: %v", entry.Name(), err)
			continue
		}

		// the plugin file name is authoritative for the top-level command name,
		// matching the old behavior where the name was passed explicitly.
		m.Name = strings.TrimSuffix(entry.Name(), ".json")

		if m.Name == "" || m.Help == "" {
			log.Printf("Invalid plugin %v: plugin declared no name or help", entry.Name())
			continue
		}

		cmd := buildPluginCommand(p.Cmd, m.Name, m.Help, "", nil, nil,
			m.flagGroupModel.Flags, m.argGroupModel.Args, m.cmdGroupModel.Commands)

		app.AddCommand(cmd)
	}

	return nil
}

// buildPluginCommand recursively builds a cobra command for a node in the
// introspected model tree. binary is the external plugin executable. A leaf command (one with
// no subcommands) gets a RunE that reconstructs the plugin argument list and
// execs the binary.
func buildPluginCommand(binary, name, help, helpLong string, aliases, tags []string, flags []*flagModel, args []*argModel, subs []*cmdModel) *cobra.Command {
	cmd := &cobra.Command{
		Use:     name,
		Short:   help,
		Long:    helpLong,
		Aliases: aliases,
	}

	// register flags on this command's local flag set, mirroring the model's
	// per-command flags. string by default, bool for booleans, StringArray for
	// cumulative flags so they can be supplied repeatedly.
	for _, f := range flags {
		switch {
		case f.Boolean:
			cmd.Flags().Bool(f.Name, false, f.Help)
		case f.Cumulative:
			def := append([]string(nil), f.Default...)
			cmd.Flags().StringArray(f.Name, def, f.Help)
		default:
			def := ""
			if len(f.Default) > 0 {
				def = f.Default[0]
			}
			cmd.Flags().String(f.Name, def, f.Help)
		}
		if f.Hidden {
			_ = cmd.Flags().MarkHidden(f.Name)
		}
	}

	if len(subs) > 0 {
		// group / parent command: recurse for children, no action of its own.
		for _, sub := range subs {
			var sf []*flagModel
			var sa []*argModel
			var sc []*cmdModel
			if sub.flagGroupModel != nil {
				sf = sub.flagGroupModel.Flags
			}
			if sub.argGroupModel != nil {
				sa = sub.argGroupModel.Args
			}
			if sub.cmdGroupModel != nil {
				sc = sub.cmdGroupModel.Commands
			}
			child := buildPluginCommand(binary, sub.Name, sub.Help, sub.HelpLong, sub.Aliases, sub.Tags, sf, sa, sc)
			child.Hidden = sub.Hidden
			cmd.AddCommand(child)
		}
		return cmd
	}

	// leaf command: it actually runs the external binary. capture the flag and
	// arg models so the RunE closure can reconstruct the command line.
	cmd.DisableFlagParsing = false
	leafFlags := flags
	cmd.RunE = func(c *cobra.Command, positional []string) error {
		execArgs := buildExecArgs(c, leafFlags, positional)

		if os.Getenv("FISK_DEBUG") != "" {
			fmt.Printf("Fisk Plugin Running: %s %s\n", binary, strings.Join(execArgs, " "))
		}

		ec := exec.Command(binary, execArgs...)
		ec.Stdout = os.Stdout
		ec.Stderr = os.Stderr
		ec.Stdin = os.Stdin
		ec.Env = os.Environ()
		return ec.Run()
	}

	return cmd
}

// buildExecArgs reconstructs the argument list passed to the external plugin
// binary, mirroring the plugin executor ordering:
//
//  1. the command path under the plugin root (cobra's command names minus the
//     plugin's own top-level name and the "nats" root),
//  2. the positional arguments the user supplied,
//  3. each set flag rendered as the executor renders it: --name=value for string flags
//     (repeated per value for cumulative flags) and --name / --no-name for
//     booleans.
//
// Only flags the user actually changed are emitted (cmd.Flags().Changed), the
// same intent as the model executor's flagsIsSet tracking.
func buildExecArgs(cmd *cobra.Command, flags []*flagModel, positional []string) []string {
	var out []string

	// command path: everything below the plugin's top-level command. The plugin
	// top-level command is the child of the root "nats" command, so we drop the
	// root and the plugin name and keep the rest of the path.
	var path []string
	for c := cmd; c != nil && c.HasParent(); c = c.Parent() {
		path = append([]string{c.Name()}, path...)
	}
	// path now is [pluginName, sub, subsub, ...]; drop the plugin name itself,
	// the binary already knows its own name.
	if len(path) > 1 {
		out = append(out, path[1:]...)
	}

	// positional args next.
	out = append(out, positional...)

	// then the flags the user set, in declared order for determinism.
	for _, f := range flags {
		if !cmd.Flags().Changed(f.Name) {
			continue
		}

		switch {
		case f.Boolean:
			b, err := cmd.Flags().GetBool(f.Name)
			if err != nil {
				continue
			}
			if f.Negatable {
				if b {
					out = append(out, "--"+f.Name)
				} else {
					out = append(out, "--no-"+f.Name)
				}
			} else if b {
				// un-negatable bool: only emit when true, matching the plugin executor.
				out = append(out, "--"+f.Name)
			}

		case f.Cumulative:
			vals, err := cmd.Flags().GetStringArray(f.Name)
			if err != nil {
				continue
			}
			for _, v := range vals {
				out = append(out, fmt.Sprintf("--%s=%s", f.Name, v))
			}

		default:
			v, err := cmd.Flags().GetString(f.Name)
			if err != nil {
				continue
			}
			out = append(out, fmt.Sprintf("--%s=%s", f.Name, v))
		}
	}

	return out
}

func Register(name string, command string, force bool) error {
	if !validNames.MatchString(name) {
		return fmt.Errorf("plugins names must match ^[a-z]$")
	}

	cmd, err := filepath.Abs(command)
	if err != nil {
		return err
	}

	store, err := pluginDir()
	if err != nil {
		return err
	}

	pluginPath := filepath.Join(store, fmt.Sprintf("%s.json", name))

	if !force {
		exist, _ := fileAccessible(pluginPath)
		if exist {
			return fmt.Errorf("plugins %s already registered, use --force to update", name)
		}
	}

	intro := exec.Command(cmd, "--fisk-introspect")
	out, err := intro.CombinedOutput()
	if err != nil {
		return err
	}

	pj, err := json.Marshal(plugin{cmd, out})
	if err != nil {
		return err
	}

	err = os.WriteFile(pluginPath, pj, 0600)
	if err != nil {
		return err
	}

	return nil
}

func fileAccessible(f string) (bool, error) {
	stat, err := os.Stat(f)
	if err != nil {
		return false, err
	}

	if stat.IsDir() {
		return false, fmt.Errorf("is a directory")
	}

	file, err := os.Open(f)
	if err != nil {
		return false, err
	}
	file.Close()

	return true, nil
}

func pluginDir() (string, error) {
	parent, err := iu.XdgShareHome()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(parent, "nats", "cli", "plugins")
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return "", err
	}

	return dir, nil
}
