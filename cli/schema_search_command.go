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
	"strings"

	"github.com/nats-io/jsm.go/api"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/spf13/cobra"
)

type schemaSearchCmd struct {
	filter string
	json   bool
}

func configureSchemaSearchCommand(schema *cobra.Command) {
	c := &schemaSearchCmd{}
	search := addCommand(schema, "search", "Search schemas using a pattern")
	search.Aliases = []string{"find", "list", "ls"}
	search.RunE = c.search
	addArgWithDefault(search, "pattern", "Regular expression to search for", ".", "string")
	search.Flags().BoolVar(&c.json, "json", false, "Produce JSON format output")
}

func (c *schemaSearchCmd) search(_ *cobra.Command, args []string) error {
	c.filter = "."
	if v := argValue(args, 0); v != "" {
		c.filter = v
	}

	found, err := api.SchemaSearch(c.filter)
	if err != nil {
		return fmt.Errorf("search failed: %s", err)
	}

	if c.json {
		iu.PrintJSON(found)
		return nil
	}

	if len(found) == 0 {
		fmt.Printf("No schemas matched %q\n", c.filter)
		return nil
	}

	fmt.Printf("Matched Schemas:\n\n  %s\n", strings.Join(found, "\n  "))

	return nil
}
