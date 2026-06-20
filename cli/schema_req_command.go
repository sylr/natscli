// Copyright 2020-2024 The NATS Authors
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

	"github.com/nats-io/jsm.go/api"
	"github.com/spf13/cobra"
)

type schemaReqCmd struct {
	subject string
	body    string
	schema  string
	dump    bool
}

func configureSchemaReqCommand(schema *cobra.Command) {
	c := &schemaReqCmd{}

	req := addCommand(schema, "request", "Request and validate data from a NATS service")
	req.Aliases = []string{"req"}
	req.RunE = c.requestAction
	addArg(req, "subject", "The subject to send a request to", true, "string")
	addArgWithDefault(req, "body", "The body to send", `{}`, "string")
	req.Flags().StringVar(&c.schema, "schema", "", "The schema identifier to validate against")
	req.Flags().BoolVar(&c.dump, "show", false, "Show the received data")
}

func (c *schemaReqCmd) requestAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]
	c.body = `{}`
	if v := argValue(args, 1); v != "" {
		c.body = v
	}

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	res, err := nc.Request(c.subject, []byte(c.body), opts().Timeout)
	if err != nil {
		return err
	}

	if c.dump {
		var d any
		err := json.Unmarshal(res.Data, &d)
		if err != nil {
			return err
		}
		j, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(j))
		fmt.Println()
	}

	schemaType, msg, err := api.ParseMessage(res.Data)
	if err != nil {
		return err
	}

	if schemaType == "io.nats.unknown_message" {
		return fmt.Errorf("could not determine the message type")
	}

	if c.schema != "" && schemaType != c.schema {
		return fmt.Errorf("invalid message type %s", schemaType)
	}

	ok, errs := validator().ValidateStruct(msg, schemaType)
	if !ok {
		fmt.Printf("Message did not pass validation against %s\n\n", schemaType)
		for _, err := range errs {
			fmt.Printf("   %s\n", err)
		}

		return nil
	}

	fmt.Printf("Response is a valid %s message\n", schemaType)

	return nil
}
