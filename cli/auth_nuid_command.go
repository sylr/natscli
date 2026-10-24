// Copyright 2026 The NATS Authors
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

	"github.com/nats-io/nuid"
	"github.com/spf13/cobra"
)

type authNuidCommand struct{}

func configureAuthNuidCommand(auth commandHost) {
	c := &authNuidCommand{}

	nu := addCommand(auth, "nuid", "Create NUIDs")

	nuGen := addCommand(nu, "gen", "Generates NUIDs")
	nuGen.RunE = c.genAction
	cmdAddTags(nuGen, "impact:ro")
}

func (c *authNuidCommand) genAction(_ *cobra.Command, _ []string) error {
	fmt.Println(nuid.Next())
	return nil
}
