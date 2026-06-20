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
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// llmExtraInfo is the application level extra information appended to LLM help,
// configured via SetLLMExtraInformation (mirrors fisk's LLMExtraInformation).
var llmExtraInfo string

// SetLLMExtraInformation sets text appended to the LLM friendly help output.
func SetLLMExtraInformation(text string) { llmExtraInfo = text }

// llmFormatEnabled reports whether LLMFORMAT=1 is set, which makes the default
// help render in markdown the same way --help-llm does.
func llmFormatEnabled() bool { return os.Getenv("LLMFORMAT") == "1" }

// SetupHelp wires the markdown/LLM help behaviour onto the root command:
//   - a hidden --help-llm persistent flag
//   - a help function that renders markdown when LLMFORMAT=1
//
// InterceptLLMHelp handles the --help-llm flag before normal execution.
func SetupHelp(root *cobra.Command) {
	root.PersistentFlags().Bool("help-llm", false, "Generate LLM friendly help")
	_ = root.PersistentFlags().MarkHidden("help-llm")

	// Capture cobra's built-in default help closure before installing the
	// custom one. root has no custom helpFunc and no parent yet, so HelpFunc()
	// returns the default renderer; calling it from inside the custom func
	// renders standard help without recursing.
	defaultHelp := root.HelpFunc()
	root.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		if llmFormatEnabled() {
			fmt.Fprintln(cmd.OutOrStdout(), renderLLMHelp(cmd))
			return
		}
		defaultHelp(cmd, args)
	})
}

// InterceptLLMHelp checks args for --help-llm and, if present, prints the LLM
// help for the targeted command and returns true so the caller can exit before
// normal execution (which would otherwise enforce required flags etc.).
func InterceptLLMHelp(root *cobra.Command, args []string) bool {
	wants := false
	var filtered []string
	for _, a := range args {
		if a == "--help-llm" {
			wants = true
			continue
		}
		filtered = append(filtered, a)
	}
	if !wants {
		return false
	}

	cmd, _, err := root.Find(filtered)
	if err != nil || cmd == nil {
		cmd = root
	}
	fmt.Fprintln(root.OutOrStdout(), renderLLMHelp(cmd))
	return true
}

// escapeMDTable escapes characters that would break a markdown table cell.
func escapeMDTable(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

// firstLine returns the first line of s.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// flagTypeHint returns the human readable type for a flag, matching fisk hints.
func flagTypeHint(f *pflag.Flag) string {
	t := f.Value.Type()
	switch t {
	case "stringSlice", "stringArray":
		return "string"
	case "intSlice":
		return "int"
	case "boolSlice":
		return "bool"
	default:
		return t
	}
}

// flagIsCumulative reports whether a flag accepts repeated values.
func flagIsCumulative(f *pflag.Flag) bool {
	switch f.Value.Type() {
	case "stringSlice", "stringArray", "intSlice", "boolSlice", "key=value":
		return true
	}
	// enums / url lists / existing files surface as their element type but accumulate.
	switch f.Value.(type) {
	case *enumsValue, *urlListValue, *existingFilesValue:
		return true
	}
	return false
}

// flagRequired reports whether a flag was marked required via MarkFlagRequired.
func flagRequired(f *pflag.Flag) bool {
	if f.Annotations == nil {
		return false
	}
	v, ok := f.Annotations[cobra.BashCompOneRequiredFlag]
	return ok && len(v) > 0 && v[0] == "true"
}

// flagName renders the flag name column, e.g. "`--server`, `-s`" or "`--[no-]x`".
func flagName(f *pflag.Flag) string {
	name := f.Name
	if flagIsNegatable(f) {
		name = "[no-]" + name
	}
	out := "`--" + name + "`"
	if f.Shorthand != "" {
		out += ", `-" + f.Shorthand + "`"
	}
	return out
}

// flagDefault renders the default value column, suppressing zero values.
func flagDefault(f *pflag.Flag) string {
	d := f.DefValue
	switch d {
	case "", "false", "0", "[]", "0s", "map[]":
		return ""
	}
	return "`" + escapeMDTable(d) + "`"
}

func flagEnv(f *pflag.Flag) string {
	if f.Annotations == nil {
		return ""
	}
	if v, ok := f.Annotations[envarAnnotation]; ok && len(v) > 0 {
		return "`" + v[0] + "`"
	}
	return ""
}

// renderLLMHelp produces the markdown LLM help for a command, reproducing fisk's
// LLMHelpTemplate output.
func renderLLMHelp(cmd *cobra.Command) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# %s\n", cmd.CommandPath())
	if cmd.Short != "" {
		fmt.Fprintf(&b, "%s\n", cmd.Short)
	}
	if cmd.Long != "" {
		fmt.Fprintf(&b, "\n%s\n", cmd.Long)
	}
	if tags := cmdTags(cmd); len(tags) > 0 {
		fmt.Fprintf(&b, "**Tags:** %s\n", strings.Join(tags, ", "))
	}
	if len(cmd.Aliases) > 0 {
		fmt.Fprintf(&b, "**Aliases:** %s\n", strings.Join(cmd.Aliases, ", "))
	}

	b.WriteString("## Usage\n\n```\n")
	fmt.Fprintf(&b, "%s\n", llmUsageLine(cmd))
	b.WriteString("\n```\n")

	// Arguments
	if args := commandArgs(cmd); len(args) > 0 {
		b.WriteString("## Arguments\n\n")
		b.WriteString("| Argument | Description | Type | Default | Required |\n")
		b.WriteString("|----------|-------------|------|---------|----------|\n")
		for _, a := range args {
			req := "No"
			if a.required {
				req = "Yes"
			}
			rep := ""
			if a.cumulative {
				rep = ", repeatable"
			}
			def := ""
			if a.dflt != "" {
				def = "`" + escapeMDTable(a.dflt) + "`"
			}
			fmt.Fprintf(&b, "| `%s` | %s | `%s` | %s | %s%s |\n", a.name, escapeMDTable(a.help), escapeMDTable(a.typeHint), def, req, rep)
		}
		b.WriteString("\n")
	}

	// Local flags
	local := visibleFlags(cmd.LocalFlags(), cmd.InheritedFlags())
	if len(local) > 0 {
		b.WriteString("## Flags\n\n")
		writeFlagTable(&b, local)
		b.WriteString("\n")
	}

	// Subcommands
	subs := visibleSubcommands(cmd)
	if len(subs) > 0 {
		b.WriteString("## Subcommands\n\n")
		hasTags := false
		for _, s := range subs {
			if len(cmdTags(s)) > 0 {
				hasTags = true
				break
			}
		}
		if hasTags {
			b.WriteString("| Command | Description | Tags |\n")
			b.WriteString("|---------|-------------|------|\n")
		} else {
			b.WriteString("| Command | Description |\n")
			b.WriteString("|---------|-------------|\n")
		}
		for _, s := range subs {
			if hasTags {
				fmt.Fprintf(&b, "| `%s` | %s | %s |\n", s.CommandPath(), escapeMDTable(firstLine(s.Short)), strings.Join(cmdTags(s), ", "))
			} else {
				fmt.Fprintf(&b, "| `%s` | %s |\n", s.CommandPath(), escapeMDTable(firstLine(s.Short)))
			}
		}
		b.WriteString("\n")
	}

	// Global (inherited) flags
	global := visibleFlags(cmd.InheritedFlags(), nil)
	if len(global) > 0 {
		b.WriteString("## Global Flags\n\n")
		writeFlagTable(&b, global)
		b.WriteString("\n")
	}

	if llmExtraInfo != "" {
		fmt.Fprintf(&b, "## Additional Information\n\n%s\n", llmExtraInfo)
	}

	return b.String()
}

func writeFlagTable(b *strings.Builder, flags []*pflag.Flag) {
	b.WriteString("| Flag | Description | Type | Default | Required | Env Var |\n")
	b.WriteString("|------|-------------|------|---------|----------|---------|\n")
	for _, f := range flags {
		req := "No"
		if flagRequired(f) {
			req = "Yes"
		}
		rep := ""
		if flagIsCumulative(f) {
			rep = ", repeatable"
		}
		fmt.Fprintf(b, "| %s | %s | `%s` | %s | %s%s | %s |\n",
			flagName(f), escapeMDTable(f.Usage), escapeMDTable(flagTypeHint(f)), flagDefault(f), req, rep, flagEnv(f))
	}
}

// visibleFlags returns non-hidden flags from set, excluding any present in exclude.
func visibleFlags(set *pflag.FlagSet, exclude *pflag.FlagSet) []*pflag.Flag {
	var out []*pflag.Flag
	set.VisitAll(func(f *pflag.Flag) {
		if f.Hidden {
			return
		}
		if exclude != nil && exclude.Lookup(f.Name) != nil {
			return
		}
		out = append(out, f)
	})
	return out
}

func visibleSubcommands(cmd *cobra.Command) []*cobra.Command {
	var out []*cobra.Command
	for _, c := range cmd.Commands() {
		if c.Hidden || !c.IsAvailableCommand() {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// llmUsageLine builds the usage string, e.g. "nats counter get [<flags>] <subject>".
func llmUsageLine(cmd *cobra.Command) string {
	parts := []string{cmd.CommandPath()}
	if cmd.HasAvailableSubCommands() {
		parts = append(parts, "<command>", "[<args> ...]")
		return strings.Join(parts, " ")
	}
	if cmd.LocalFlags().HasAvailableFlags() || cmd.InheritedFlags().HasAvailableFlags() {
		parts = append(parts, "[<flags>]")
	}
	for _, a := range commandArgs(cmd) {
		if a.required {
			parts = append(parts, "<"+a.name+">")
		} else {
			parts = append(parts, "[<"+a.name+">]")
		}
	}
	return strings.Join(parts, " ")
}
