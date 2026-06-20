// Copyright 2025 The NATS Authors
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
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// argMeta describes a positional argument. cobra has no declarative positional
// argument support, so this metadata is tracked separately to drive the help
// renderers and to build the cobra.Args validator. Values are bound to struct
// fields inside each command's RunE from the args slice.
type argMeta struct {
	name       string
	help       string
	typeHint   string
	dflt       string
	required   bool
	cumulative bool
	enum       []string
}

// commandArgsMeta records positional argument metadata per command. Commands are
// long lived singletons built during configuration, so a pointer keyed map is safe.
var commandArgsMeta = map[*cobra.Command][]argMeta{}

// addArg registers a required-or-optional scalar positional argument.
func addArg(cmd *cobra.Command, name string, help string, required bool, typeHint string) {
	registerArg(cmd, argMeta{name: name, help: help, required: required, typeHint: typeHint})
}

// addArgWithDefault registers an optional positional argument that has a default value.
func addArgWithDefault(cmd *cobra.Command, name string, help string, dflt string, typeHint string) {
	registerArg(cmd, argMeta{name: name, help: help, dflt: dflt, typeHint: typeHint})
}

// addArgCumulative registers a trailing positional argument that consumes all
// remaining values (e.g. fisk's StringsVar on an Arg).
func addArgCumulative(cmd *cobra.Command, name string, help string, required bool, typeHint string) {
	registerArg(cmd, argMeta{name: name, help: help, required: required, cumulative: true, typeHint: typeHint})
}

// addArgEnum registers a positional argument restricted to a fixed set of values.
// The value is validated by the cobra Args validator; bind it to a field in RunE.
func addArgEnum(cmd *cobra.Command, name string, help string, required bool, options ...string) {
	registerArg(cmd, argMeta{name: name, help: help, required: required, typeHint: "string", enum: options})
}

func registerArg(cmd *cobra.Command, m argMeta) {
	if m.typeHint == "" {
		m.typeHint = "string"
	}
	metas := append(commandArgsMeta[cmd], m)
	commandArgsMeta[cmd] = metas
	cmd.Args = makeArgsValidator(metas)
}

func commandArgs(cmd *cobra.Command) []argMeta { return commandArgsMeta[cmd] }

// makeArgsValidator builds a cobra positional-count validator from arg metadata.
func makeArgsValidator(metas []argMeta) cobra.PositionalArgs {
	min := 0
	cumulative := false
	for _, m := range metas {
		if m.required {
			min++
		}
		if m.cumulative {
			cumulative = true
		}
	}
	max := len(metas)

	return func(cmd *cobra.Command, args []string) error {
		if len(args) < min {
			return fmt.Errorf("required argument %q not provided", metas[len(args)].name)
		}
		if !cumulative && len(args) > max {
			return fmt.Errorf("unexpected argument %q", args[max])
		}
		for i, m := range metas {
			if len(m.enum) == 0 || i >= len(args) {
				continue
			}
			valid := false
			for _, o := range m.enum {
				if args[i] == o {
					valid = true
					break
				}
			}
			if !valid {
				return fmt.Errorf("argument %q must be one of %s but got %q", m.name, strings.Join(m.enum, ","), args[i])
			}
		}
		return nil
	}
}

// argValue safely returns the positional argument at index i, or "" if absent.
// Command RunE functions use this to bind optional positional args to fields.
func argValue(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}
