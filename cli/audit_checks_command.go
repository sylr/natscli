package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/jsm.go/audit"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/spf13/cobra"
)

type auditChecksCommand struct {
	json bool
}

func configureAuditChecksCommand(app *cobra.Command) {
	c := &auditChecksCommand{}

	checks := addCommand(app, "checks", "List configured audit checks")
	checks.Aliases = []string{"ls"}
	checks.RunE = c.checksAction
	checks.Flags().BoolVar(&c.json, "json", false, "Produce JSON output")
}

func (c *auditChecksCommand) checksAction(_ *cobra.Command, _ []string) error {
	collection, err := audit.NewDefaultCheckCollection()
	if err != nil {
		return err
	}

	var checks []*audit.Check
	collection.EachCheck(func(c *audit.Check) {
		checks = append(checks, c)
	})

	if c.json {
		return iu.PrintJSON(checks)
	}

	tbl := iu.NewTableWriterf(opts(), "Audit Checks")
	tbl.AddHeaders("Suite", "Code", "Description", "Configuration")

	for _, check := range checks {
		var cfgKeys []string
		for _, cfg := range check.Configuration {
			switch cfg.Unit {
			case audit.PercentageUnit:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s%%)", cfg.Key, f(int(cfg.Default))))
			case audit.IntUnit, audit.UIntUnit:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s)", cfg.Key, f(cfg.Default)))
			default:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s)", cfg.Key, f(cfg.Default)))
			}
		}
		sort.Strings(cfgKeys)

		tbl.AddRow(check.Suite, check.Code, check.Description, strings.Join(cfgKeys, ", "))
	}

	fmt.Println(tbl.Render())

	return nil
}
