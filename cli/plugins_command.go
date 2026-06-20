// Copyright 2024 The NATS Authors
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

	"github.com/nats-io/natscli/plugins"
	"github.com/spf13/cobra"
)

type pluginsCmd struct {
	name    string
	command string
	force   bool
}

func configurePluginCommand(app commandHost) {
	c := &pluginsCmd{}

	cmd := addCommand(app, "plugins", "Manage plugins")
	cmd.Hidden = true

	register := addCommand(cmd, "register", "Registers a new plugin")
	register.RunE = c.registerAction
	addArg(register, "name", "The top level name to register the command as", true, "string")
	addArg(register, "command", "The command the provides the plugins", true, "path")
	register.Flags().BoolVar(&c.force, "force", false, "Overwrite existing plugins")
}

func init() {
	registerCommand("plugins", 18, configurePluginCommand)
}

func (c *pluginsCmd) registerAction(_ *cobra.Command, args []string) error {
	c.name = args[0]
	c.command = args[1]

	fmt.Println("WARNING: Plugins support is experimental and not officially supported")
	fmt.Println()

	err := plugins.Register(c.name, c.command, c.force)
	if err != nil {
		return err
	}

	fmt.Printf("Plugin %s using command %s was added or updated\n", c.name, c.command)

	return nil
}
