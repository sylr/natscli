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
	"net/url"
	"regexp"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// This file exports the cobra helper layer for callers outside the cli package
// (notably the nats/main.go entrypoint and embedders) so they can register the
// global flags and wiring using the same primitives used internally.

// NewEnumValue returns a pflag.Value restricting a string to a fixed set of options.
func NewEnumValue(target *string, dflt string, options ...string) pflag.Value {
	return newEnumValue(target, dflt, options...)
}

// NewExistingFileValue returns a pflag.Value that requires an existing file path.
func NewExistingFileValue(target *string) pflag.Value { return newExistingFileValue(target) }

// NewExistingDirValue returns a pflag.Value that requires an existing directory path.
func NewExistingDirValue(target *string) pflag.Value { return newExistingDirValue(target) }

// NewURLValue returns a pflag.Value that parses a URL.
func NewURLValue(target **url.URL) pflag.Value { return newURLValue(target) }

// NewRegexpValue returns a pflag.Value that compiles a regular expression.
func NewRegexpValue(target **regexp.Regexp) pflag.Value { return newRegexpValue(target) }

// NegatableBoolVar registers a bool flag plus a hidden --no-<name> companion.
func NegatableBoolVar(cmd *cobra.Command, target *bool, name string, def bool, usage string) {
	negatableBoolVar(cmd, target, name, def, usage)
}

// NegatablePersistentBoolVar registers a negatable bool on cmd's persistent
// flag set, so subcommands inherit both --name and --no-name.
func NegatablePersistentBoolVar(cmd *cobra.Command, target *bool, name string, def bool, usage string) {
	negatablePersistentBoolVar(cmd, target, name, def, usage)
}

// FlagEnvVar binds an environment variable to a flag.
func FlagEnvVar(cmd *cobra.Command, name string, env string) { flagEnvVar(cmd, name, env) }

// FlagPlaceholder sets the help placeholder shown for a flag's value.
func FlagPlaceholder(cmd *cobra.Command, name string, ph string) { flagPlaceholder(cmd, name, ph) }

// ConfigureCheatCommand attaches the hidden "cheat" command to root.
func ConfigureCheatCommand(root *cobra.Command) *cobra.Command { return configureCheatCommand(root) }

// RegisterContextCompletion attaches context-name completion to cmd's --context
// flag. It must be called after the flag has been registered (e.g. for the
// global persistent flags defined by the application entrypoint).
func RegisterContextCompletion(cmd *cobra.Command) {
	if cmd.Flags().Lookup("context") != nil || cmd.PersistentFlags().Lookup("context") != nil {
		_ = cmd.RegisterFlagCompletionFunc("context", completeContextNames)
	}
}
