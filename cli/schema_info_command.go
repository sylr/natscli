// Copyright 2020 The NATS Authors
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

	"github.com/ghodss/yaml"
	"github.com/nats-io/jsm.go/api"
	"github.com/spf13/cobra"
)

type schemaInfoCmd struct {
	schema string
	yaml   bool
}

func configureSchemaInfoCommand(schema *cobra.Command) {
	c := &schemaInfoCmd{}
	info := addCommand(schema, "info", "Display schema contents")
	info.Aliases = []string{"show", "view"}
	info.RunE = c.info
	addArg(info, "schema", "Schema ID to show", true, "string")
	info.Flags().BoolVar(&c.yaml, "yaml", false, "Produce YAML format output")
}

func (c *schemaInfoCmd) info(_ *cobra.Command, args []string) error {
	c.schema = args[0]

	schema, err := api.Schema(c.schema)
	if err != nil {
		return fmt.Errorf("could not load schema %q: %s", c.schema, err)
	}

	if c.yaml {
		schema, err = yaml.JSONToYAML(schema)
		if err != nil {
			return fmt.Errorf("could not reformat schema as YAML: %s", err)
		}
	}

	fmt.Println(string(schema))

	return nil
}
