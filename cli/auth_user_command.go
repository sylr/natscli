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
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	au "github.com/nats-io/natscli/internal/auth"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/AlecAivazis/survey/v2"
	ab "github.com/synadia-io/jwt-auth-builder.go"

	"github.com/spf13/cobra"
)

type authUserCommand struct {
	userName        string
	accountName     string
	operatorName    string
	defaults        bool
	signingKey      string
	userLocale      string
	bearerAllowed   bool
	maxPayload      int64
	maxData         int64
	maxSubs         int64
	maxPayloadIsSet bool
	maxSubsIsSet    bool
	pubAllow        []string
	pubDeny         []string
	subDeny         []string
	subAllow        []string
	tags            []string
	rmTags          []string
	listNames       bool
	force           bool
	json            bool
	credFile        string
	expire          time.Duration
	revoke          bool
}

func configureAuthUserCommand(auth commandHost) {
	c := &authUserCommand{}

	user := addCommand(auth, "user", "Manage Account Users")
	user.Aliases = []string{"u", "usr", "users"}

	addCreateFlags := func(f *cobra.Command, edit bool) {
		negatableBoolVar(f, &c.bearerAllowed, "bearer", false, "Enables the use of bearer tokens")
		f.Flags().Int64Var(&c.maxData, "data", -1, "Maximum message data size to allow")
		f.Flags().StringVar(&c.userLocale, "locale", "", "Sets the locale for the user connection")
		f.Flags().Int64Var(&c.maxPayload, "payload", 1048576, "Maximum payload size to allow")
		f.Flags().StringArrayVar(&c.pubAllow, "pub-allow", nil, "Allow publishing to a subject")
		f.Flags().StringArrayVar(&c.pubDeny, "pub-deny", nil, "Deny publishing to a subject")
		f.Flags().StringArrayVar(&c.subAllow, "sub-allow", nil, "Allow subscribing to a subject")
		f.Flags().StringArrayVar(&c.subDeny, "sub-deny", nil, "Deny subscribing to a subject")
		f.Flags().Int64Var(&c.maxSubs, "subscriptions", -1, "Maximum subscription count to allow")
		f.Flags().StringArrayVar(&c.tags, "tags", nil, "Tags to assign to this User")
		if edit {
			f.Flags().StringArrayVar(&c.rmTags, "no-tags", nil, "Tags to remove from this User")
		}
	}

	add := addCommand(user, "add", "Adds a new User")
	add.Aliases = []string{"create", "new"}
	add.RunE = c.addAction
	cmdAddTags(add, "scope:system", "impact:rw")
	addArg(add, "name", "Unique name for this User", true, "string")
	addArg(add, "account", "Account to add the user to", false, "string")
	add.Flags().StringVar(&c.signingKey, "key", "", "The public key to use when signing the user")
	add.Flags().StringVar(&c.operatorName, "operator", "", "Operator to add the user to")
	addCreateFlags(add, false)
	add.Flags().BoolVarP(&c.force, "force", "f", false, "Overwrite existing files")
	add.Flags().StringVar(&c.credFile, "credential", "", "Writes credentials to a file")
	add.Flags().BoolVar(&c.defaults, "defaults", false, "Accept default values without prompting")

	info := addCommand(user, "info", "Show User information")
	info.Aliases = []string{"i", "show", "view"}
	info.RunE = c.infoAction
	cmdAddTags(info, "scope:system", "impact:ro")
	addArg(info, "name", "Unique name for this User", false, "string")
	addArg(info, "account", "Account to query", false, "string")
	info.Flags().StringVar(&c.operatorName, "operator", "", "Operator holding the Account")
	info.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	edit := addCommand(user, "edit", "Edits User settings")
	edit.Aliases = []string{"update"}
	edit.RunE = c.editAction
	cmdAddTags(edit, "scope:system", "impact:rw")
	addArg(edit, "name", "Unique name for this User", false, "string")
	addArg(edit, "account", "Account to query", false, "string")
	edit.Flags().StringVar(&c.operatorName, "operator", "", "Operator holding the Account")
	addCreateFlags(edit, true)
	edit.Flags().StringVar(&c.credFile, "credential", "", "Writes credentials to a file")

	ls := addCommand(user, "ls", "List users")
	ls.RunE = c.lsAction
	cmdAddTags(ls, "scope:system", "impact:ro")
	addArg(ls, "account", "Account to query", false, "string")
	ls.Flags().StringVar(&c.operatorName, "operator", "", "Operator holding the Account")
	ls.Flags().BoolVar(&c.listNames, "names", false, "Show just the Account names")

	rm := addCommand(user, "rm", "Removes an user")
	rm.RunE = c.rmAction
	cmdAddTags(rm, "scope:system", "impact:rw")
	addArg(rm, "name", "Unique name for this User", false, "string")
	addArg(rm, "account", "Account to query", false, "string")
	rm.Flags().StringVar(&c.operatorName, "operator", "", "Operator holding the Account")
	rm.Flags().BoolVar(&c.revoke, "revoke", false, "Also revokes the user before deleting it")
	rm.Flags().BoolVarP(&c.force, "force", "f", false, "Removes without prompting")

	cred := addCommand(user, "credential", "Creates a credential file for a user")
	cred.Aliases = []string{"cred", "creds"}
	cred.RunE = c.credAction
	cmdAddTags(cred, "scope:system", "impact:rw")
	addArg(cred, "file", "The file to create", true, "string")
	addArg(cred, "name", "User to generate a credential for", false, "string")
	addArg(cred, "account", "Account to query", false, "string")
	cred.Flags().DurationVar(&c.expire, "expire", 0, "Duration till expiry")
	cred.Flags().StringVar(&c.operatorName, "operator", "", "Operator holding the Account")
	cred.Flags().BoolVarP(&c.force, "force", "f", false, "Overwrite existing files")
}

func (c *authUserCommand) editAction(_ *cobra.Command, args []string) error {
	c.userName = argValue(args, 0)
	c.accountName = argValue(args, 1)

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	user, _ := acct.Users().Get(c.userName)
	if user == nil {
		return fmt.Errorf("user not found")
	}

	err = c.updateUser(user)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	if c.credFile != "" {
		err = c.writeCred(user, c.credFile, true)
		if err != nil {
			return err
		}
	}

	return c.fShowUser(os.Stdout, user, acct)
}

func (c *authUserCommand) credAction(_ *cobra.Command, args []string) error {
	c.credFile = args[0]
	c.userName = argValue(args, 1)
	c.accountName = argValue(args, 2)

	if !c.force && iu.FileExists(c.credFile) {
		return fmt.Errorf("file %s already exist", c.credFile)
	}

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	user, _ := acct.Users().Get(c.userName)
	if user == nil {
		return fmt.Errorf("user not found")
	}

	err = c.writeCred(user, c.credFile, c.force)
	if err != nil {
		return err
	}

	fmt.Printf("Wrote credential for %s to %s\n", user.Name(), c.credFile)

	return nil
}

func (c *authUserCommand) rmAction(_ *cobra.Command, args []string) error {
	c.userName = argValue(args, 0)
	c.accountName = argValue(args, 1)

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the User %s", c.userName), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	user, err := acct.Users().Get(c.userName)
	if errors.Is(err, ab.ErrNotFound) {
		return fmt.Errorf("user does not exist")
	} else if err != nil {
		return err
	}

	if c.revoke {
		err = acct.Revocations().Add(user.Subject(), time.Now())
		if err != nil {
			return fmt.Errorf("revocation failed: %v", err)
		}
	}

	err = acct.Users().Delete(c.userName)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("Removed user %s\n", c.userName)

	return nil
}
func (c *authUserCommand) lsAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	users := acct.Users().List()
	if len(users) == 0 {
		fmt.Println("No users found")
		return nil
	}

	if c.listNames {
		for _, u := range users {
			fmt.Println(u.Name())
		}
		return nil
	}

	table := iu.NewTableWriterf(opts(), "Users in account %s", acct.Name())
	table.AddHeaders("Name", "Subject", "Scoped", "Sub Perms", "Pub Perms", "Max Subscriptions")
	for _, user := range users {
		limits := ab.UserLimits(user)
		if user.IsScoped() {
			scope, err := acct.ScopedSigningKeys().GetScope(user.Issuer())
			if errors.Is(err, ab.ErrNotFound) {
				table.AddRow(user.Name(), user.Subject(), user.IsScoped(), "", "", "")
				continue
			} else if err != nil {
				return err
			}
			limits = scope
		}

		var hasPub, hasSub, maxSubs string

		if len(limits.PubPermissions().Deny()) > 0 || len(limits.PubPermissions().Allow()) > 0 {
			hasPub = "✓"
		}

		if len(limits.SubPermissions().Deny()) > 0 || len(limits.SubPermissions().Allow()) > 0 {
			hasSub = "✓"
		}

		maxSubs = strconv.Itoa(int(user.MaxSubscriptions()))
		if user.MaxSubscriptions() == -1 {
			maxSubs = "Unlimited"
		}

		table.AddRow(user.Name(), user.Subject(), user.IsScoped(), hasPub, hasSub, maxSubs)
	}

	fmt.Println(table.Render())

	return nil
}

func (c *authUserCommand) pickUser(acct ab.Account) error {
	users := acct.Users().List()
	if len(users) == 0 {
		return fmt.Errorf("no users found in %s", acct.Name())
	}

	var names []string
	for _, u := range users {
		names = append(names, u.Name())
	}
	sort.Strings(names)

	err := iu.AskOne(&survey.Select{
		Message:  "Select a User",
		Options:  names,
		PageSize: iu.SelectPageSize(len(names)),
	}, &c.userName)
	if err != nil {
		return err
	}

	return nil
}

func (c *authUserCommand) infoAction(_ *cobra.Command, args []string) error {
	c.userName = argValue(args, 0)
	c.accountName = argValue(args, 1)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	user, err := acct.Users().Get(c.userName)
	if user == nil || err != nil {
		return fmt.Errorf("user %s not found", c.userName)
	}

	return c.fShowUser(os.Stdout, user, acct)
}

func (c *authUserCommand) addAction(cmd *cobra.Command, args []string) error {
	c.userName = args[0]
	c.accountName = argValue(args, 1)
	c.maxPayloadIsSet = cmd.Flags().Changed("payload")
	c.maxSubsIsSet = cmd.Flags().Changed("subscriptions")

	auth, _, acct, err := au.SelectOperatorAccount(c.operatorName, c.accountName, true)
	if err != nil {
		return err
	}

	if c.signingKey != "" {
		sk, err := au.SelectSigningKey(acct, c.signingKey)
		if err != nil {
			return err
		}
		c.signingKey = sk.Key()
	}

	if c.signingKey == "" {
		c.signingKey = acct.Subject()
	}

	user, err := acct.Users().Get(c.userName)
	switch {
	case user != nil:
		return fmt.Errorf("user %s already exist", c.userName)
	case errors.Is(err, ab.ErrNotFound):
	case err != nil:
		return err
	}

	user, err = acct.Users().Add(c.userName, c.signingKey)
	if err != nil {
		return err
	}

	if !c.defaults {
		if !c.maxPayloadIsSet {
			c.maxPayload, err = askOneInt("Maximum Payload", "-1", "The maximum message size the user can send")
			if err != nil {
				return err
			}
		}

		if !c.maxSubsIsSet {
			c.maxSubs, err = askOneInt("Maximum Subscriptions", "-1", "The maximum number of subscriptions the user can make")
			if err != nil {
				return err
			}
		}
	}

	err = au.UpdateTags(user.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	err = c.updateUser(user)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	if c.credFile != "" {
		err = c.writeCred(user, c.credFile, c.force)
		if err != nil {
			return err
		}
	}

	user, err = acct.Users().Get(c.userName)
	if user == nil || err != nil {
		return fmt.Errorf("user not found")
	}

	return c.fShowUser(os.Stdout, user, acct)
}

func (c *authUserCommand) updateUser(user ab.User) error {
	if user.IsScoped() {
		return nil
	}

	limits := user.(*ab.UserData).UserPermissionLimits()
	limits.Locale = c.userLocale
	limits.BearerToken = c.bearerAllowed
	limits.Payload = c.maxPayload
	limits.Data = c.maxData
	limits.Subs = c.maxSubs

	// TODO: should allow adding/removing not just setting
	if len(c.pubAllow) > 0 {
		if len(c.pubAllow) == 1 && c.pubAllow[0] == "" {
			c.pubAllow = []string{}
		}
		limits.Pub.Allow = c.pubAllow
	}
	if len(c.pubDeny) > 0 {
		if len(c.pubDeny) == 1 && c.pubDeny[0] == "" {
			c.pubDeny = []string{}
		}
		limits.Pub.Deny = c.pubDeny
	}
	if len(c.subAllow) > 0 {
		if len(c.subAllow) == 1 && c.subAllow[0] == "" {
			c.subAllow = []string{}
		}
		limits.Sub.Allow = c.subAllow
	}
	if len(c.subDeny) > 0 {
		if len(c.subDeny) == 1 && c.subDeny[0] == "" {
			c.subDeny = []string{}
		}
		limits.Sub.Deny = c.subDeny
	}

	err := au.UpdateTags(user.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	return user.(*ab.UserData).SetUserPermissionLimits(limits)
}

func (c *authUserCommand) fShowUser(w io.Writer, user ab.User, acct ab.Account) error {
	out, err := c.showUser(user, acct)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *authUserCommand) showUser(user ab.User, acct ab.Account) (string, error) {
	if c.json {
		return iu.ToJSON(user)
	}

	cols := newColumnsf("User %s (%s)", user.Name(), user.Subject())
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Account", fmt.Sprintf("%s (%s)", acct.Name(), user.IssuerAccount()))
	cols.AddRow("Issuer", user.Issuer())
	cols.AddRow("Scoped", user.IsScoped())
	if tags, _ := user.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}

	limits := ab.UserLimits(user)
	if user.IsScoped() {
		scope, err := acct.ScopedSigningKeys().GetScope(user.Issuer())
		if err != nil {
			return "", fmt.Errorf("could not find signing scope %s", user.Issuer())
		}
		limits = scope
	}

	err := au.RenderUserLimits(limits, cols)
	if err != nil {
		return "", err
	}

	return cols.Render()
}

func (c *authUserCommand) selectAccount(pick bool) (*ab.AuthImpl, ab.Operator, ab.Account, error) {
	auth, oper, acct, err := au.SelectOperatorAccount(c.operatorName, c.accountName, pick)
	if err != nil {
		return nil, nil, nil, err
	}

	c.operatorName = oper.Name()
	c.accountName = acct.Name()

	return auth, oper, acct, nil
}

func (c *authUserCommand) writeCred(user ab.User, credFile string, force bool) error {
	if !force && iu.FileExists(credFile) {
		return fmt.Errorf("file %s already exist", credFile)
	}

	cred, err := user.Creds(c.expire)
	if err != nil {
		return err
	}

	return os.WriteFile(c.credFile, cred, 0600)
}
