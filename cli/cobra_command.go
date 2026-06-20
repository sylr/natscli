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
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// commandHost is the parent a command attaches itself to. Both the application
// root and any intermediate command are plain *cobra.Command values, so this is
// just an alias kept for the historic registration signatures.
type commandHost = *cobra.Command

// Annotation keys used to carry fisk-style metadata on cobra commands and flags.
const (
	tagsAnnotation        = "natscli_tags"        // command annotation: comma joined tags
	envarAnnotation       = "natscli_envar"       // flag annotation: backing environment variable
	placeholderAnnotation = "natscli_placeholder" // flag annotation: help placeholder
	negatableAnnotation   = "natscli_negatable"   // flag annotation: marks a --[no-]name bool
)

// addCommand creates a subcommand of parent and returns it. It replaces fisk's
// parent.Command(name, help) which both created and attached the command.
func addCommand(parent *cobra.Command, use string, short string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		// Errors are reported by the root command's runner; subcommands should
		// not each dump usage on a runtime (RunE) error.
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	parent.AddCommand(cmd)
	return cmd
}

// cmdAddTags records fisk-style command tags (e.g. "scope:user", "impact:ro").
func cmdAddTags(cmd *cobra.Command, tags ...string) {
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	existing := cmdTags(cmd)
	existing = append(existing, tags...)
	cmd.Annotations[tagsAnnotation] = strings.Join(existing, ",")
}

// cmdTags returns the tags recorded on a command.
func cmdTags(cmd *cobra.Command) []string {
	if cmd.Annotations == nil {
		return nil
	}
	v := cmd.Annotations[tagsAnnotation]
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// lookupFlag finds a flag on a command, checking both its local and persistent
// flag sets so it works for global (persistent) flags registered before merge.
func lookupFlag(cmd *cobra.Command, name string) *pflag.Flag {
	if f := cmd.Flags().Lookup(name); f != nil {
		return f
	}
	return cmd.PersistentFlags().Lookup(name)
}

// flagEnvVar binds an environment variable to a flag. The value is applied in
// the root PersistentPreRunE when the flag was not set on the command line.
func flagEnvVar(cmd *cobra.Command, name string, env string) {
	if f := lookupFlag(cmd, name); f != nil {
		if f.Annotations == nil {
			f.Annotations = map[string][]string{}
		}
		f.Annotations[envarAnnotation] = []string{env}
	}
}

// flagPlaceholder sets the help placeholder shown for a flag's value.
func flagPlaceholder(cmd *cobra.Command, name string, ph string) {
	if f := lookupFlag(cmd, name); f != nil {
		if f.Annotations == nil {
			f.Annotations = map[string][]string{}
		}
		f.Annotations[placeholderAnnotation] = []string{ph}
	}
}

// negatedBoolValue is the backing value for the hidden --no-<name> companion of
// a negatable bool flag: setting it to true clears the target.
type negatedBoolValue struct{ target *bool }

func (n *negatedBoolValue) Set(s string) error {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*n.target = !b
	return nil
}

func (n *negatedBoolValue) String() string { return strconv.FormatBool(!*n.target) }
func (n *negatedBoolValue) Type() string   { return "bool" }

// negatableBoolVar registers a bool flag plus a hidden --no-<name> companion,
// reproducing fisk's BoolVar which accepts both spellings.
func negatableBoolVar(cmd *cobra.Command, target *bool, name string, def bool, usage string) {
	negatableBoolVarP(cmd, target, name, "", def, usage)
}

// negatableBoolVarP is negatableBoolVar with a shorthand.
func negatableBoolVarP(cmd *cobra.Command, target *bool, name string, shorthand string, def bool, usage string) {
	negatableBoolVarOn(cmd.Flags(), target, name, shorthand, def, usage)
}

// negatablePersistentBoolVar registers a negatable bool on a command's
// persistent flag set so it is inherited by subcommands (used for global flags).
func negatablePersistentBoolVar(cmd *cobra.Command, target *bool, name string, def bool, usage string) {
	negatableBoolVarOn(cmd.PersistentFlags(), target, name, "", def, usage)
}

// negatableBoolVarOn registers the bool flag and its hidden --no-<name>
// companion on the given flag set.
func negatableBoolVarOn(fs *pflag.FlagSet, target *bool, name string, shorthand string, def bool, usage string) {
	fs.BoolVarP(target, name, shorthand, def, usage)
	f := fs.Lookup(name)
	if f.Annotations == nil {
		f.Annotations = map[string][]string{}
	}
	f.Annotations[negatableAnnotation] = []string{"true"}

	fs.Var(&negatedBoolValue{target: target}, "no-"+name, "")
	nf := fs.Lookup("no-" + name)
	nf.NoOptDefVal = "true"
	nf.Hidden = true
}

// propagateNegatableChanged marks a negatable bool flag as Changed when the
// user supplied its hidden --no-<name> companion instead. Without this, an
// IsSetByUser check via Flags().Changed("name") would miss the --no-name form
// and the command would fall back to interactive prompting.
func propagateNegatableChanged(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if !flagIsNegatable(f) {
			return
		}
		if no := cmd.Flags().Lookup("no-" + f.Name); no != nil && no.Changed {
			f.Changed = true
		}
	})
}

// flagIsNegatable reports whether a flag was registered as a negatable bool.
func flagIsNegatable(f *pflag.Flag) bool {
	if f.Annotations == nil {
		return false
	}
	_, ok := f.Annotations[negatableAnnotation]
	return ok
}

// applyEnvVars sets flags from their bound environment variables when the user
// did not provide them on the command line. By the time PersistentPreRunE runs,
// cobra has merged inherited persistent flags into the executed command's flag
// set, so a single walk covers both global and per-command env flags.
func applyEnvVars(cmd *cobra.Command) error {
	var visitErr error
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if visitErr != nil || f.Changed || f.Annotations == nil {
			return
		}
		env, ok := f.Annotations[envarAnnotation]
		if !ok || len(env) == 0 {
			return
		}
		if val, present := os.LookupEnv(env[0]); present && val != "" {
			if err := f.Value.Set(val); err != nil {
				visitErr = err
			} else {
				f.Changed = true
			}
		}
	})
	return visitErr
}
