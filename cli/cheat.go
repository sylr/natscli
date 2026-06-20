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
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// cheats holds the registered cheat sheets keyed by their label. It reproduces
// fisk's per-application cheat registry that backed the WithCheats()/CheatFile()
// API. Cheats are loaded from the embedded cheats/ directory via addCheat.
var (
	cheats     = map[string]string{}
	cheatTags  = []string{appName}
	cheatsHelp = fmt.Sprintf(`Shows cheats for %s

These cheats are compatible with the 'cheat' CLI tool and by saving the output
using --save these cheats become accessible within that application.

See https://github.com/cheat/cheat for more details`, appName)
)

// addCheat registers the embedded cheats/<name>.md file under the given label.
// The cmd argument is retained for call-site compatibility; cheats are surfaced
// through the top level "cheat" command rather than per command.
func addCheat(name string, _ *cobra.Command) {
	if opts().NoCheats {
		return
	}

	body, err := fs.ReadFile(fmt.Sprintf("cheats/%s.md", name))
	if err != nil {
		fatalIfError(err, "cannot load cheat %q", name)
		return
	}

	cheats[name] = string(body)
}

// configureCheatCommand attaches the "cheat" command to the root, mirroring
// fisk's WithCheats() behaviour. The command is registered hidden to match
// main.go's previous CheatCommand.Hidden() call.
func configureCheatCommand(root *cobra.Command) *cobra.Command {
	var (
		list bool
		dir  string
	)

	cheat := &cobra.Command{
		Use:           "cheat [<label>]",
		Short:         cheatsHelp,
		Hidden:        true,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			label := argValue(args, 0)
			switch {
			case dir != "":
				return saveCheats(dir)
			case list:
				listCheats(cmd)
			default:
				if len(cheats) == 0 {
					listCheats(cmd)
					return nil
				}
				if label == "" {
					if len(cheats) > 1 {
						listCheats(cmd)
						return nil
					}
					for k := range cheats {
						label = k
					}
				}
				body, ok := cheats[label]
				if !ok {
					listCheats(cmd)
					return nil
				}
				fmt.Fprintln(cmd.OutOrStdout(), body)
			}
			return nil
		},
	}

	cheat.Flags().BoolVar(&list, "list", false, "List available cheats")
	cheat.Flags().StringVar(&dir, "save", "", "Saves the cheats to the given directory")
	flagPlaceholder(cheat, "save", "DIRECTORY")
	addArg(cheat, "label", "The cheat to show", false, "string")

	root.AddCommand(cheat)
	return cheat
}

func listCheats(cmd *cobra.Command) {
	w := cmd.OutOrStdout()
	if len(cheats) == 0 {
		fmt.Fprintln(w, "No cheats defined")
		return
	}

	var list []string
	top := ""
	for k := range cheats {
		if k == appName {
			top = appName
			continue
		}
		list = append(list, k)
	}
	sort.Strings(list)
	if top != "" {
		list = append([]string{top}, list...)
	}

	fmt.Fprintln(w, "Available Cheats:")
	fmt.Fprintln(w)
	for _, k := range list {
		fmt.Fprintf(w, "    %s\n", k)
	}
}

func saveCheats(dir string) error {
	if len(cheats) == 0 {
		return fmt.Errorf("no cheats defined")
	}

	if err := os.MkdirAll(dir, 0744); err != nil {
		return err
	}

	var list []string
	for k := range cheats {
		list = append(list, k)
	}
	sort.Strings(list)

	for _, k := range list {
		if cheats[k] == "" {
			continue
		}

		f, err := os.Create(filepath.Join(dir, k))
		if err != nil {
			return err
		}

		fmt.Fprintf(f, "---\ntags: [%s]\n---\n\n", strings.Join(cheatTags, ", "))
		fmt.Fprintln(f, cheats[k])
		f.Close()
	}

	return nil
}
