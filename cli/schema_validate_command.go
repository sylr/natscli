// Copyright 2020-2022 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	iu "github.com/nats-io/natscli/internal/util"
	"github.com/spf13/cobra"
)

type schemaValidateCmd struct {
	schema string
	file   string
	json   bool
}

func configureSchemaValidateCommand(schema *cobra.Command) {
	c := &schemaValidateCmd{}

	validate := addCommand(schema, "validate", "Validates a JSON file against a schema")
	validate.Aliases = []string{"check"}
	validate.RunE = c.validate
	addArg(validate, "schema", "Schema ID to validate against", true, "string")
	addArg(validate, "file", "JSON data to validate (- for stdin)", true, "string")
	validate.Flags().BoolVar(&c.json, "json", false, "Produce JSON format output")
}

func (c *schemaValidateCmd) validate(_ *cobra.Command, args []string) error {
	c.schema = args[0]
	c.file = args[1]

	var f io.ReadCloser
	var err error

	if c.file == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(c.file)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	file, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	var data any
	err = json.Unmarshal(file, &data)
	if err != nil {
		return fmt.Errorf("could not parse JSON data in %q: %s", c.file, err)
	}

	ok, errs := new(SchemaValidator).ValidateStruct(data, c.schema)
	if c.json {
		if errs == nil {
			errs = []string{}
		}
		iu.PrintJSON(errs)
		return nil
	}

	if ok {
		fmt.Printf("%s validates against %s\n", c.file, c.schema)
		return nil
	}

	fmt.Printf("Validation errors in %s:\n\n", c.file)
	fmt.Printf("  %s\n", strings.Join(errs, "\n  "))

	return nil
}
