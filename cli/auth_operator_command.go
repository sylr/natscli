// Copyright 2023-2026 The NATS Authors
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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"

	au "github.com/nats-io/natscli/internal/auth"
	"github.com/nats-io/natscli/internal/fips"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/nats-io/nkeys"

	"github.com/AlecAivazis/survey/v2"
	ab "github.com/synadia-io/jwt-auth-builder.go"

	"github.com/spf13/cobra"
)

type authOperatorCommand struct {
	operatorName         string
	operatorService      []*url.URL
	operatorServiceIsSet bool
	accountServer        *url.URL
	accountServerIsSet   bool
	listNames            bool
	force                bool
	json                 bool
	createSK             bool
	tokenFile            string
	keyFiles             []string
	pubKey               string
	outputFile           string
	encKey               string
	tags                 []string
	rmTags               []string
}

func configureAuthOperatorCommand(auth commandHost) {
	c := &authOperatorCommand{}

	op := addCommand(auth, "operator", "Manage NATS Operators")
	op.Aliases = []string{"o", "op"}

	add := addCommand(op, "add", "Adds a new Operator")
	add.RunE = c.addAction
	cmdAddTags(add, "scope:system", "impact:rw")
	addArg(add, "name", "Unique name for this Operator", false, "string")
	add.Flags().Var(newURLListValue(&c.operatorService), "service", "URLs for the Operator services")
	flagPlaceholder(add, "service", "URL")
	add.Flags().Var(newURLValue(&c.accountServer), "account-server", "URL for the account server")
	flagPlaceholder(add, "account-server", "URL")
	negatableBoolVar(add, &c.createSK, "signing-key", true, "Creates a signing key for this Operator")
	add.Flags().StringArrayVar(&c.tags, "tags", nil, "Tags to assign to this Operator")

	info := addCommand(op, "info", "Show Operator information")
	info.Aliases = []string{"i", "show", "view"}
	info.RunE = c.infoAction
	cmdAddTags(info, "scope:system", "impact:ro")
	addArg(info, "name", "Operator to view", false, "string")
	info.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	ls := addCommand(op, "list", "List Operators")
	ls.Aliases = []string{"ls"}
	ls.RunE = c.lsAction
	cmdAddTags(ls, "scope:system", "impact:ro")
	ls.Flags().BoolVar(&c.listNames, "names", false, "Show just the Operator names")

	edit := addCommand(op, "edit", "Edit an Operator")
	edit.Aliases = []string{"update"}
	edit.RunE = c.editAction
	cmdAddTags(edit, "scope:system", "impact:rw")
	addArg(edit, "name", "Operator to edit", false, "string")
	edit.Flags().Var(newURLValue(&c.accountServer), "account-server", "URL for the Account Server")
	flagPlaceholder(edit, "account-server", "URL")
	edit.Flags().Var(newURLListValue(&c.operatorService), "service", "URLs for the Operator Services")
	flagPlaceholder(edit, "service", "URL")
	edit.Flags().StringArrayVar(&c.tags, "tags", nil, "Tags to add to this Operator")
	edit.Flags().StringArrayVar(&c.rmTags, "no-tags", nil, "Tags to remove from the Operator")

	imp := addCommand(op, "import", "Imports an operator")
	imp.RunE = c.importAction
	cmdAddTags(imp, "scope:system", "impact:rw")
	addArg(imp, "token", "The JWT file containing the account to import", true, "string")
	addArgCumulative(imp, "key", "List of keys to import", false, "string")

	sel := addCommand(op, "select", "Selects the default operator")
	sel.RunE = c.selectAction
	cmdAddTags(sel, "scope:system", "impact:ro")
	addArg(sel, "name", "Operator to select", false, "string")

	backup := addCommand(op, "backup", "Creates a backup of an operator")
	backup.RunE = c.backupAction
	cmdAddTags(backup, "scope:system", "impact:ro")
	addArg(backup, "name", "Operator to act on", true, "string")
	addArg(backup, "output", "File to write backup to", true, "string")
	backup.Flags().StringVar(&c.encKey, "key", "", "Curve or X25519 NKey to encrypt with")

	restore := addCommand(op, "restore", "Restores an operator from a backup")
	restore.RunE = c.restoreAction
	cmdAddTags(restore, "scope:system", "impact:rw")
	addArg(restore, "name", "Operator to act on", true, "string")
	addArg(restore, "input", "File to read backup from", true, "string")
	restore.Flags().StringVar(&c.encKey, "key", "", "Curve or X25519 NKey to decrypt with")

	sk := addCommand(op, "keys", "Manage Operator Signing Keys")
	sk.Aliases = []string{"sk", "s"}

	skls := addCommand(sk, "list", "List Signing Keys")
	skls.Aliases = []string{"ls"}
	skls.RunE = c.skListAction
	cmdAddTags(skls, "scope:system", "impact:ro")
	addArg(skls, "name", "Operator to act on", false, "string")

	skadd := addCommand(sk, "add", "Adds a new Signing Key")
	skadd.Aliases = []string{"new", "create"}
	skadd.RunE = c.skAddAction
	cmdAddTags(skadd, "scope:system", "impact:rw")
	addArg(skadd, "name", "Operator to act on", false, "string")

	skrm := addCommand(sk, "rm", "Removes a Signing Key")
	skrm.Aliases = []string{"delete"}
	skrm.RunE = c.skRmAction
	cmdAddTags(skrm, "scope:system", "impact:rw")
	addArg(skrm, "name", "Operator to act on", false, "string")
	addArg(skrm, "key", "The public key to remove", false, "string")
	skrm.Flags().BoolVarP(&c.force, "force", "f", false, "Remove without prompting")
}

func (c *authOperatorCommand) selectAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)

	_, oper, err := au.SelectOperator(c.operatorName, true, false)
	if err != nil {
		return err
	}

	cfg, err := iu.LoadConfig()
	if err != nil {
		return err
	}
	cfg.SelectedOperator = oper.Name()
	err = iu.SaveConfig(cfg)
	if err != nil {
		return err
	}

	fmt.Printf("Selected operator %q as default\n", oper.Name())

	return nil
}

func (c *authOperatorCommand) selectOperator(pick bool) (*ab.AuthImpl, ab.Operator, error) {
	auth, oper, err := au.SelectOperator(c.operatorName, pick, true)
	if err != nil {
		return nil, nil, err
	}

	c.operatorName = oper.Name()

	return auth, oper, err
}

func (c *authOperatorCommand) skRmAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)
	c.pubKey = argValue(args, 1)

	if c.pubKey == "" {
		return fmt.Errorf("public key is required")
	}

	auth, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the signing key %s", c.pubKey), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	ok, err := operator.SigningKeys().Delete(c.pubKey)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("signing key was not found")
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Println("Signing key removed")

	return nil
}

func (c *authOperatorCommand) skAddAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)

	auth, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	k, err := operator.SigningKeys().Add()
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Println(k)

	return nil
}

func (c *authOperatorCommand) skListAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)

	_, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	for _, k := range operator.SigningKeys().List() {
		fmt.Println(k)
	}

	return nil
}

func (c *authOperatorCommand) importAction(_ *cobra.Command, args []string) error {
	c.tokenFile = args[0]
	c.keyFiles = args[1:]

	auth, err := au.GetAuthBuilder()
	if err != nil {
		return err
	}

	var token []byte
	var keys []string

	token, err = os.ReadFile(c.tokenFile)
	if err != nil {
		return err
	}

	for _, f := range c.keyFiles {
		key, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		keys = append(keys, string(key))
	}

	op, err := auth.Operators().Import(token, keys)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowOperator(os.Stdout, op)
}

func (c *authOperatorCommand) fShowOperator(w io.Writer, op ab.Operator) error {
	out, err := c.showOperator(op)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)

	return err
}

func (c *authOperatorCommand) editAction(cmd *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)
	c.accountServerIsSet = cmd.Flags().Changed("account-server")
	c.operatorServiceIsSet = cmd.Flags().Changed("service")

	auth, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	if c.accountServerIsSet {
		u := ""
		if c.accountServer != nil {
			u = c.accountServer.String()
		}

		err = operator.SetAccountServerURL(u)
		if err != nil {
			return err
		}
	}

	if c.operatorServiceIsSet {
		list := []string{}
		if c.operatorService != nil {
			for _, s := range c.operatorService {
				list = append(list, s.String())
			}
		}

		err = operator.SetOperatorServiceURL(list...)
		if err != nil {
			return err
		}
	}

	err = au.UpdateTags(operator.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowOperator(os.Stdout, operator)
}
func (c *authOperatorCommand) restoreAction(_ *cobra.Command, args []string) error {
	c.operatorName = args[0]
	c.outputFile = args[1]

	auth, err := au.GetAuthBuilder()
	if err != nil {
		return err
	}

	if au.IsAuthItemKnown(auth.Operators().List(), c.operatorName) {
		return fmt.Errorf("operator %s already exist", c.operatorName)
	}

	j, err := os.ReadFile(c.outputFile)
	if err != nil {
		return err
	}

	if c.encKey != "" {
		if fips.Enabled() {
			return fips.DisabledError("nats auth operator restore --key", "X25519")
		}

		keyData, err := iu.ReadKeyFile(c.encKey)
		if err != nil {
			return err
		}

		kp, err := nkeys.FromSeed(keyData)
		if err != nil {
			return err
		}
		pk, err := kp.PublicKey()
		if err != nil {
			return err
		}

		if !nkeys.IsValidPublicCurveKey(pk) {
			return errors.New("invalid public key provided")
		}

		j, err = base64.StdEncoding.DecodeString(string(j))
		if err != nil {
			return err
		}

		j, err = kp.Open(j, pk)
		if err != nil {
			return fmt.Errorf("open failed: %w", err)
		}
	}

	op, err := auth.Operators().Add(c.operatorName)
	if err != nil {
		return err
	}

	err = json.Unmarshal(j, op)
	if err != nil {
		return fmt.Errorf("unmarshal failed: %w", err)
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowOperator(os.Stdout, op)
}

func (c *authOperatorCommand) backupAction(_ *cobra.Command, args []string) error {
	c.operatorName = args[0]
	c.outputFile = args[1]

	_, op, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	j, err := json.MarshalIndent(op, "", "  ")
	if err != nil {
		return err
	}

	if c.encKey != "" {
		if fips.Enabled() {
			return fips.DisabledError("nats auth operator backup --key", "X25519")
		}

		keyData, err := iu.ReadKeyFile(c.encKey)
		if err != nil {
			return err
		}

		kp, err := nkeys.FromSeed(keyData)
		if err != nil {
			return err
		}
		pk, err := kp.PublicKey()
		if err != nil {
			return err
		}

		if !nkeys.IsValidPublicCurveKey(pk) {
			return errors.New("invalid public key provided")
		}

		j, err = kp.Seal(j, pk)
		if err != nil {
			return err
		}

		j = []byte(base64.StdEncoding.EncodeToString(j))
	}

	err = os.WriteFile(c.outputFile, j, 0600)
	if err != nil {
		return err
	}
	fmt.Printf("Wrote backup for %s to %s\n", op.Name(), c.outputFile)
	if c.encKey == "" {
		fmt.Println()
		fmt.Println("WARNING: The output file is unencrypted and contains secrets,")
		fmt.Println("consider encrypting it with 'nats auth nkey seal'")
	}

	return nil
}

func (c *authOperatorCommand) infoAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)

	_, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	return c.fShowOperator(os.Stdout, operator)
}

func (c *authOperatorCommand) lsAction(_ *cobra.Command, _ []string) error {
	auth, err := au.GetAuthBuilder()
	if err != nil {
		return err
	}

	list := auth.Operators().List()
	if len(list) == 0 {
		fmt.Println("No Operators found")
		return nil
	}

	if c.listNames {
		for _, op := range list {
			fmt.Println(op.Name())
		}
		return nil
	}

	table := iu.NewTableWriterf(opts(), "Operators")
	table.AddHeaders("Name", "Subject", "Accounts", "Account Server", "Signing Keys")
	for _, op := range list {
		table.AddRow(op.Name(), op.Subject(), len(op.Accounts().List()), op.AccountServerURL(), len(op.SigningKeys().List()))
	}
	fmt.Println(table.Render())

	return nil
}

func (c *authOperatorCommand) addAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)

	if c.operatorName == "" {
		err := iu.AskOne(&survey.Input{
			Message: "Operator Name",
			Help:    "A unique name for the Operator being added",
		}, &c.operatorName, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	auth, err := au.GetAuthBuilder()
	if err != nil {
		return err
	}

	if au.IsAuthItemKnown(auth.Operators().List(), c.operatorName) {
		return fmt.Errorf("operator %s already exist", c.operatorName)
	}

	operator, err := auth.Operators().Add(c.operatorName)
	if err != nil {
		return err
	}

	err = au.UpdateTags(operator.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	if c.operatorService != nil {
		list := []string{}
		for _, s := range c.operatorService {
			list = append(list, s.String())
		}

		err = operator.SetOperatorServiceURL(list...)
		if err != nil {
			return err
		}
	}

	if c.accountServer != nil {
		err = operator.SetAccountServerURL(c.accountServer.String())
		if err != nil {
			return err
		}
	}

	// always creating a system account for new operators
	system, err := operator.Accounts().Add("SYSTEM")
	if err != nil {
		return err
	}

	err = operator.SetSystemAccount(system)
	if err != nil {
		return err
	}

	if c.createSK {
		_, err = operator.SigningKeys().Add()
		if err != nil {
			return err
		}
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	operator, err = auth.Operators().Get(c.operatorName)
	if err != nil {
		return err
	}

	return c.fShowOperator(os.Stdout, operator)
}

func (c *authOperatorCommand) showOperator(operator ab.Operator) (string, error) {
	if c.json {
		return iu.ToJSON(operator)
	}

	cols := newColumnsf("Operator %s (%s)", operator.Name(), operator.Subject())
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", operator.Name())
	cols.AddRow("Subject", operator.Subject())
	if tags, _ := operator.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}
	cols.AddRowIf("Service URL(s)", operator.OperatorServiceURLs(), len(operator.OperatorServiceURLs()) > 0)
	cols.AddRowIfNotEmpty("Account Server", operator.AccountServerURL())
	cols.AddRow("Accounts", len(operator.Accounts().List()))

	sa, err := operator.SystemAccount()
	if err == nil {
		cols.AddRowf("System Account", "%s (%s)", sa.Name(), sa.Subject())
	} else {
		cols.AddRow("System Account", "not set")
	}

	if len(operator.SigningKeys().List()) > 0 {
		list := []string{}
		list = append(list, operator.SigningKeys().List()...)
		sort.Strings(list)

		cols.AddStringsAsValue("Signing Keys", list)
	}

	return cols.Render()
}
