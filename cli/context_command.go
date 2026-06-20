// Copyright 2020-2026 The NATS Authors
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
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strings"
	"sync"
	"text/template"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/AlecAivazis/survey/v2"
	"github.com/fatih/color"
	"github.com/ghodss/yaml"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

type ctxCommand struct {
	json             bool
	completionFormat bool
	namesFormat      bool
	activate         bool
	description      string
	name             string
	source           string
	force            bool
	validateErrors   int
	embed            bool

	reg *natscontext.Registry
	mu  sync.Mutex
}

func configureCtxCommand(app commandHost) {
	c := ctxCommand{}

	context := addCommand(app, "context", "Manage nats configuration contexts")
	context.Aliases = []string{"ctx"}
	addCheat("contexts", context)

	save := addCommand(context, "add", "Update or create a context")
	save.Aliases = []string{"create", "save"}
	save.RunE = c.createCommand
	save.Long = `When using --creds, --nkey, --jwt and --seed the following formats are supported

  - /some/path                  - direct path to a file
  - file://path                 - alternative form for a path
  - op://vault/item/field       - fetch via the 1Password CLI ('op read')
  - nsc://Operator/Account/User - generate via the local 'nsc' binary
  - env://NAME                  - read from environment variable NAME

File paths can also be embedded into the context JSON using --embed
`

	addArg(save, "name", "The context name to act on", true, "string")
	save.Flags().StringVar(&c.description, "description", "", "Set a friendly description for this context")
	save.Flags().BoolVar(&c.activate, "select", false, "Select the saved context as the default one")
	save.Flags().BoolVar(&c.embed, "embed", false, "Embeds credential content in the body of the context")

	dupe := addCommand(context, "copy", "Copies an existing context")
	dupe.Aliases = []string{"cp"}
	dupe.RunE = c.copyCommand
	addArg(dupe, "source", "The name of the context to copy from", true, "string")
	addArg(dupe, "name", "The name of the context to create", true, "string")
	dupe.Flags().StringVar(&c.description, "description", "", "Set a friendly description for this context")
	dupe.Flags().BoolVar(&c.activate, "select", false, "Select the saved context as the default one")
	dupe.Flags().BoolVar(&c.embed, "embed", false, "Embeds credential content in the body of the context")

	edit := addCommand(context, "edit", "Edit a context in your EDITOR")
	edit.Aliases = []string{"vi"}
	edit.RunE = c.editCommand
	addArg(edit, "name", "The context name to edit", true, "string")

	ls := addCommand(context, "ls", "List known contexts")
	ls.Aliases = []string{"list", "l"}
	ls.RunE = c.listCommand
	ls.Flags().BoolVar(&c.completionFormat, "completion", false, "Format the list for use by shell completion")
	_ = ls.Flags().MarkHidden("completion")
	ls.Flags().BoolVarP(&c.json, "json", "j", false, "Show the list in JSON format")
	ls.Flags().BoolVar(&c.namesFormat, "names", false, "List just the names of known contexts")

	rm := addCommand(context, "rm", "Remove a context")
	rm.Aliases = []string{"remove"}
	rm.RunE = c.removeCommand
	addArg(rm, "name", "The context name to remove", true, "string")
	rm.Flags().BoolVarP(&c.force, "force", "f", false, "Force remove without prompting")

	pick := addCommand(context, "select", "Select the default context")
	pick.Aliases = []string{"switch", "set"}
	pick.RunE = c.selectCommand
	addArg(pick, "name", "The context name to select", false, "string")

	unselect := addCommand(context, "unselect", "Ensures that no context is the default context")
	unselect.RunE = c.unselectCommand

	info := addCommand(context, "info", "Display information on the current or named context")
	info.Aliases = []string{"show", "v", "view"}
	info.RunE = c.showCommand
	addArg(info, "name", "The context name to show", false, "string")
	info.Flags().BoolVarP(&c.json, "json", "j", false, "Show the context in JSON format")
	info.Flags().BoolVar(&c.activate, "connect", false, "Attempts to connect to NATS using the context while validating")

	validate := addCommand(context, "validate", "Validate one or all contexts")
	validate.RunE = c.validateCommand
	addArg(validate, "name", "Validate a specific context, validates all when not supplied", false, "string")
	validate.Flags().BoolVar(&c.activate, "connect", false, "Attempts to connect to NATS using the context while validating")

	previous := addCommand(context, "previous", "Switch to the previous context")
	previous.Aliases = []string{"-"}
	previous.RunE = c.switchPreviousCtx
}

func init() {
	registerCommand("context", 5, configureCtxCommand)
}

// creates and caches a default registry. Just creates if options are given
func (c *ctxCommand) registry(opts ...natscontext.RegistryOption) *natscontext.Registry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(opts) == 0 && c.reg != nil {
		return c.reg
	}

	registryOpts := []natscontext.RegistryOption{
		natscontext.WithDefaultResolvers(),
		natscontext.WithLocalSelector(),
	}

	reg := natscontext.NewRegistry(natscontext.NewDefaultFileBackend(), append(opts, registryOpts...)...)

	if len(opts) == 0 {
		c.reg = reg
	}

	return reg
}

func (c *ctxCommand) hasOverrides() bool {
	return len(c.overrideVars()) != 0
}

func (c *ctxCommand) overrideVars() []string {
	var list []string
	for _, v := range overrideEnvVars {
		if os.Getenv(v) != "" {
			list = append(list, v)
		}
	}

	return list
}

func (c *ctxCommand) validateCommand(cmd *cobra.Command, args []string) error {
	c.name = argValue(args, 0)

	var contexts []string
	var err error

	if c.name == "" {
		contexts, err = c.registry().List(ctx)
		if err != nil {
			return err
		}
	} else {
		contexts = append(contexts, c.name)
	}

	for _, name := range contexts {
		c.name = name
		err := c.showCommand(cmd, []string{name})
		if err != nil {
			fmt.Printf("Could not load %s: %s\n\n", name, color.RedString(err.Error()))
		}

		fmt.Println()
	}

	if c.validateErrors > 0 {
		return fmt.Errorf("validation failed")
	}

	return nil
}

func (c *ctxCommand) copyCommand(cmd *cobra.Command, args []string) error {
	c.source = args[0]
	c.name = args[1]

	if !c.registry().Known(ctx, c.source) {
		return fmt.Errorf("unknown context %q", c.source)
	}

	if c.registry().Known(ctx, c.name) {
		return fmt.Errorf("context %q already exist", c.name)
	}

	opts().CfgCtx = c.source

	return c.createCommand(cmd, []string{c.name})
}

var ctxYamlTemplate = `# Friendly description for this context shown when listing contexts
description: {{ .Description | t }}

# A comma separated list of NATS Servers to connect to
url: {{ .ServerURL | t }}

# Connect using a specific username, requires password to be set
user: {{ .User | t }}
# Password may be a literal value or any credential resolver URI (see creds below)
password: {{ .Password | t }}

# Connect using NATS Credentials
#
# May be a path to a credentials file, or one of the supported credential
# resolver URIs:
#
#   file:///path/to/file.creds   read bytes from a filesystem path (default
#                                for bare paths)
#   op://vault/item/field        fetch via the 1Password CLI ('op read')
#   nsc://Operator/Account/User  generate via the local 'nsc' binary
#   env://NAME                   read from environment variable NAME
#   data:;base64,<payload>       inline base64-encoded RFC 2397 payload
creds: {{ .Creds | t }}

# Connect using a NKey seed
# Accepts the same formats as creds above (bare value treated as a file path)
nkey: {{ .NKey | t }}

# Connect using an inline NATS user JWT and seed (alternative to a creds file)
# Each may be a literal value or any credential resolver URI (see creds above)
user_jwt: {{ .UserJWT | t }}
user_seed: {{ .UserSeed | t }}

# Configures a token to pass in the connection
# May be a literal value or any credential resolver URI (see creds above)
token: {{ .Token | t }}

# Sets a x509 certificate to use, both cert and key should be set
cert: {{ .Certificate | t }}
key: {{ .Key | t }}

# Sets an optional x509 trust chain to use
ca: {{ .CA | t }}

# Performs TLS Handshake before Server sends a greeting
tls_first: {{ .TLSHandshakeFirst | t }}

# Windows Certificate Store support requires windows_cert_store and windows_cert_match to be set
#
# windows_cert_store must be one of 'user' or 'machine'
# windows_cert_match_by may be 'subject' or 'issuer'
windows_cert_store: {{ .WindowsCertStore | t }}
windows_cert_match: {{ .WindowsCertStoreMatch | t }}
windows_cert_match_by: {{ .WindowsCertStoreMatchBy | t }}
windows_ca_certs_match: {{ .WindowsCaCertsMatch | t }}

# Use a custom inbox prefix
#
# Example : _INBOX.private.userid
inbox_prefix: {{ .InboxPrefix | t }}

# Sets a color scheme to use for the nats command line tool
# this will influence table color choices allowing different
# contexts to be visually distinguished.
#
# Valid values are:
#
#   rounded
#   double
#   yellow
#   blue
#   cyan
#   green
#   magenta
#   red
#
# When not set "rounded" is used
color_scheme: {{ .ColorScheme | t }}

# Connects to a specific JetStream domain
jetstream_domain: {{ .JSDomain | t }}

# Subject used as a prefix when accessing the JetStream API if imported from another account
jetstream_api_prefix: {{ .JSAPIPrefix | t }}

# Subject prefix used to access JetStream events if imported from another account
jetstream_event_prefix: {{ .JSEventPrefix | t }}

# Use a Socks5 proxy like ssh to connect to the NATS server URLS
#
# Example: socks5://example.net:1090
socks_proxy: {{ .SocksProxy | t }}
`

func (c *ctxCommand) editCommand(cmd *cobra.Command, args []string) error {
	c.name = args[0]

	if !c.registry().Known(ctx, c.name) {
		return fmt.Errorf("unknown context %q", c.name)
	}

	// TODO(rip) previously we were just acting on files, now, soon
	// we will act on remote backends so this command lost some abilities
	// like the automatic rollback or editing ability of corrupt contexts
	//
	// need to think a bit about that but for now this is ok as we progress
	// towards other backends the picture will become clearer

	registry := c.registry(natscontext.WithoutExpansion())
	nctx, err := registry.Load(ctx, c.name)
	if err != nil {
		return err
	}

	tpl, err := template.New("context").Funcs(template.FuncMap{"t": func(s any) (string, error) {
		res, err := yaml.Marshal(s)
		if err != nil {
			return "", err
		}
		return string(bytes.TrimRight(res, "\n")), nil
	}}).Parse(ctxYamlTemplate)
	if err != nil {
		return err
	}

	f, err := os.CreateTemp("", "*.yaml")
	if err != nil {
		return fmt.Errorf("could not create temporary copy to edit: %w", err)
	}

	// save to temp and edit in place as a yaml file
	err = tpl.ExecuteTemplate(f, "context", nctx)
	if err != nil {
		f.Close()
		return fmt.Errorf("could not create temporary copy to edit: %w", err)
	}
	f.Close()

	err = iu.EditFile(f.Name())
	if err != nil {
		return err
	}

	// read the yaml file and turn into json
	yctx, err := os.ReadFile(f.Name())
	if err != nil {
		return fmt.Errorf("could not read temporary copy: %w", err)
	}
	jctx, err := yaml.YAMLToJSON(yctx)
	if err != nil {
		return err
	}

	// parse and save it
	newNatsCtx, err := natscontext.NewFromBytesRaw(jctx)
	if err != nil {
		return err
	}

	err = c.registry().Save(ctx, newNatsCtx, c.name)
	if err != nil {
		return err
	}

	err = c.showCommand(cmd, args)
	if err != nil {
		return err
	}

	return nil
}

func (c *ctxCommand) renderListNames(_ string, known []*natscontext.Context) {
	var names []string
	for _, v := range known {
		names = append(names, v.Name)
	}
	sort.Strings(names)

	for _, v := range names {
		fmt.Println(v)
	}
}

func (c *ctxCommand) renderListJson(current string, known []*natscontext.Context) {
	sort.Slice(known, func(i, j int) bool {
		return known[i].Name < known[j].Name
	})

	iu.PrintJSON(known)
}

func (c *ctxCommand) renderListCompletion(current string, known []*natscontext.Context) {
	for _, nctx := range known {
		name := strings.ReplaceAll(nctx.Name, ":", `\:`)

		if name == current {
			name = name + "*"
		}

		fmt.Printf("%s:%s\n", name, nctx.Description())
	}
}

func (c *ctxCommand) renderListTable(current string, known []*natscontext.Context) {
	if len(known) == 0 {
		fmt.Println("No known contexts")
		return
	}

	table := iu.NewTableWriterf(opts(), "Known Contexts")
	table.AddHeaders("Name", "Description")

	for _, nctx := range known {
		if nctx.Name == current {
			nctx.Name = nctx.Name + "*"
		}

		table.AddRow(nctx.Name, nctx.Description())
	}

	fmt.Println(table.Render())

}
func (c *ctxCommand) listCommand(_ *cobra.Command, _ []string) error {
	names, err := c.registry().List(ctx)
	if err != nil {
		return err
	}

	current, err := c.registry().Selected(ctx)
	if err != nil && !errors.Is(err, natscontext.ErrNoneSelected) {
		return err
	}

	var contexts []*natscontext.Context

	for _, name := range names {
		cfg, err := c.registry().Load(ctx, name)
		if err != nil {
			if !c.completionFormat {
				log.Printf("Could not load context %s: %s", name, err)
			}
			continue
		}

		contexts = append(contexts, cfg)
	}

	switch {
	case c.completionFormat:
		c.renderListCompletion(current, contexts)
	case c.json:
		c.renderListJson(current, contexts)
	case c.namesFormat:
		c.renderListNames(current, contexts)
	default:
		c.renderListTable(current, contexts)
	}

	return nil
}

// formatAWSConfigStep renders one step of a context's oidc.aws_config
// assume-role chain for display, masking the secret material.
func formatAWSConfigStep(s natscontext.AWSConfigStep) string {
	var parts []string

	addIf := func(label, value string) {
		if value != "" {
			parts = append(parts, fmt.Sprintf("%s=%s", label, value))
		}
	}

	addIf("profile", s.Profile)
	addIf("region", s.Region)
	addIf("role_arn", s.RoleARN)
	addIf("session_name", s.SessionName)
	addIf("duration", s.Duration)
	addIf("access_key_id", s.AccessKeyID)
	if s.SecretAccessKey != "" {
		parts = append(parts, "secret_access_key="+strings.Repeat("*", len(s.SecretAccessKey)))
	}
	if s.SessionToken != "" {
		parts = append(parts, "session_token="+strings.Repeat("*", len(s.SessionToken)))
	}

	if len(parts) == 0 {
		return "default credential chain"
	}

	return strings.Join(parts, ", ")
}

func (c *ctxCommand) showCommand(_ *cobra.Command, args []string) error {
	c.name = argValue(args, 0)

	var err error

	if c.name == "" {
		c.name, err = c.registry().Selected(ctx)
		if err != nil && !errors.Is(err, natscontext.ErrNoneSelected) {
			return err
		}
	}

	if c.name == "" {
		return fmt.Errorf("no default context and no name supplied")
	}

	cfg, err := c.registry().Load(ctx, c.name)
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(cfg)
		return nil
	}

	checkFile := func(file string) string {
		switch {
		case file == "":
			return ""
		case strings.HasPrefix(file, "op://"):
			return color.CyanString("1Password")
		case strings.HasPrefix(file, "nsc://"):
			return color.CyanString("nsc")
		case strings.HasPrefix(file, "env://"):
			return color.CyanString("runtime environment")
		case strings.HasPrefix(file, "file://"):
			file = strings.TrimPrefix(file, "file://")
			if file == "" {
				return color.RedString("ERROR")
			}
		case strings.HasPrefix(file, "data:;base64,"):
			return color.GreenString("embedded")
		}

		if file[0] == '~' {
			usr, err := user.Current()
			if err != nil {
				return color.YellowString("failed to expand '~'. $HOME or $USER possibly not set")
			}
			file = strings.Replace(file, "~", usr.HomeDir, 1)
		}

		ok, err := iu.IsFileAccessible(file)
		if !ok || err != nil {
			c.validateErrors++
			return color.RedString("ERROR")
		}

		return color.GreenString("OK")
	}

	cols := newColumnsf("NATS Configuration Context %q", c.name)
	cols.AddRowIfNotEmpty("Description", cfg.Description())
	cols.AddRowIfNotEmpty("Server URLs", cfg.ServerURL())
	cols.AddRowIfNotEmpty("SOCKS5 Proxy", cfg.SocksProxy())
	cols.AddRowIfNotEmpty("Username", cfg.User())
	cols.AddRowIfNotEmpty("Password", strings.Repeat("*", len(cfg.Password())))
	cols.AddRowIfNotEmpty("Token", cfg.Token())
	if strings.HasPrefix(cfg.Creds(), "data:;base64,") {
		cols.AddRow("Credentials", "embedded data")
	} else {
		cols.AddRowIf("Credentials", fmt.Sprintf("%s (%s)", cfg.Creds(), checkFile(cfg.Creds())), cfg.Creds() != "")
	}
	if strings.HasPrefix(cfg.UserJWT(), "data:;base64,") {
		cols.AddRow("User JWT", "embedded data")
	} else {
		cols.AddRowIf("User JWT", fmt.Sprintf("%s (%s)", cfg.UserJWT(), checkFile(cfg.UserJWT())), cfg.UserJWT() != "")
	}
	if strings.HasPrefix(cfg.UserSeed(), "data:;base64,") {
		cols.AddRow("User Seed", "embedded data")
	} else {
		cols.AddRowIf("User Seed", fmt.Sprintf("%s (%s)", cfg.UserSeed(), checkFile(cfg.UserSeed())), cfg.UserSeed() != "")
	}
	if strings.HasPrefix(cfg.NKey(), "data:;base64,") {
		cols.AddRow("NKey", "embedded data")
	} else {
		cols.AddRowIf("NKey", fmt.Sprintf("%s (%s)", cfg.NKey(), checkFile(cfg.NKey())), cfg.NKey() != "")
	}
	if cfg.WindowsCertStore() == "" {
		cols.AddRowIf("Certificate", fmt.Sprintf("%s (%s)", cfg.Certificate(), checkFile(cfg.Certificate())), cfg.Certificate() != "")
		cols.AddRowIf("Key", fmt.Sprintf("%s (%s)", cfg.Key(), checkFile(cfg.Key())), cfg.Key() != "")
	} else {
		cols.AddRow("Certificate Store", cfg.WindowsCertStore())
		cols.AddRow("Certificate Store Match", cfg.WindowsCertStoreMatch())
		cols.AddRow("Certificate Store Match By", cfg.WindowsCertStoreMatchBy())
		cols.AddRow("Certificate Store CA Match", cfg.WindowsCaCertsMatch())
	}
	cols.AddRowIf("CA", fmt.Sprintf("%s (%s)", cfg.CA(), checkFile(cfg.CA())), cfg.CA() != "")
	cols.AddRowIf("TLS First", cfg.TLSHandshakeFirst(), cfg.TLSHandshakeFirst())
	cols.AddRowIfNotEmpty("JS API Prefix", cfg.JSAPIPrefix())
	cols.AddRowIfNotEmpty("JS Event Prefix", cfg.JSEventPrefix())
	cols.AddRowIfNotEmpty("JS Domain", cfg.JSDomain())
	cols.AddRowIfNotEmpty("Inbox Prefix", cfg.InboxPrefix())
	cols.AddRowIfNotEmpty("Path", cfg.Path())
	cols.AddRowIfNotEmpty("Color Scheme", cfg.ColorScheme())

	if oidc := cfg.OIDC(); oidc != nil {
		cols.AddSectionTitle("OIDC Auth Callout")
		cols.AddRowIfNotEmpty("Audience", oidc.Audience)
		cols.AddRowIfNotEmpty("Signing Algorithm", oidc.SigningAlgorithm)
		cols.AddRowIfNotEmpty("Token Duration", oidc.Duration)
		cols.AddRowIfNotEmpty("Cache Path", oidc.CachePath)
		cols.AddRowIfNotEmpty("Cache Refresh Before", oidc.CacheRefreshBefore)

		switch steps := oidc.AWSConfig; {
		case steps == nil:
			cols.AddRow("AWS Config", color.RedString("not configured"))
		case len(steps) == 0:
			cols.AddRow("AWS Config", "default credential chain")
		default:
			for i, step := range steps {
				cols.AddRow(fmt.Sprintf("AWS Step %d", i+1), formatAWSConfigStep(step))
			}
		}
	}

	checkConn := func() error {
		opts, err := cfg.NATSOptions()
		opts = append(opts, nats.MaxReconnects(1))
		if err != nil {
			return err
		}
		nc, err := nats.Connect(cfg.ServerURL(), opts...)
		if err != nil {
			return err
		}
		nc.Close()

		return nil
	}

	if c.activate {
		err = checkConn()
		if err != nil {
			c.validateErrors++
			cols.AddRow("Connection", color.RedString(err.Error()))
		} else {
			cols.AddRow("Connection", color.GreenString("OK"))
		}
	}

	cols.Frender(os.Stdout)

	fmt.Println()

	if c.hasOverrides() {
		fmt.Printf("%s: Shell environment overrides in place using %v", color.HiRedString("WARNING"), f(c.overrideVars()))
		fmt.Println()
	}

	return nil
}

func (c *ctxCommand) createCommand(cmd *cobra.Command, args []string) error {
	c.name = args[0]

	lname := ""
	load := false
	opts := opts()

	switch {
	case c.registry().Known(ctx, c.name):
		lname = c.name
		load = true
	case opts.CfgCtx != "":
		lname = opts.CfgCtx
		load = true
	}

	token := ""
	if opts.Password == "" && opts.Username != "" {
		token = opts.Username
		opts.Username = ""
	}

	ctxopts := []natscontext.Option{
		natscontext.WithServerURL(opts.Servers),
		natscontext.WithUser(opts.Username),
		natscontext.WithPassword(opts.Password),
		natscontext.WithToken(token),
		natscontext.WithCreds(opts.Creds),
		natscontext.WithNKey(opts.Nkey),
		natscontext.WithUserJWT(opts.UserJwt),
		natscontext.WithUserSeed(opts.UserSeed),
		natscontext.WithCertificate(opts.TlsCert),
		natscontext.WithKey(opts.TlsKey),
		natscontext.WithCA(opts.TlsCA),
		natscontext.WithWindowsCertStore(opts.WinCertStoreType),
		natscontext.WithWindowsCertStoreMatch(opts.WinCertStoreMatch),
		natscontext.WithWindowsCertStoreMatchBy(opts.WinCertStoreMatchBy),
		natscontext.WithWindowsCaCertsMatch(opts.WinCertCaStoreMatch...),
		natscontext.WithDescription(c.description),
		natscontext.WithSocksProxy(opts.SocksProxy),
		natscontext.WithJSAPIPrefix(opts.JsApiPrefix),
		natscontext.WithJSEventPrefix(opts.JsEventPrefix),
		natscontext.WithJSDomain(opts.JsDomain),
		natscontext.WithInboxPrefix(opts.InboxPrefix),
		natscontext.WithColorScheme(opts.ColorScheme),
	}
	if opts.TlsFirst {
		ctxopts = append(ctxopts, natscontext.WithTLSHandshakeFirst())
	}

	config, err := natscontext.New(lname, load, ctxopts...)
	if err != nil {
		return err
	}

	if c.embed {
		err = config.Embed()
		if err != nil {
			return err
		}
	}

	err = c.registry().Save(ctx, config, c.name)
	if err != nil {
		return err
	}

	if c.activate {
		return c.selectCommand(cmd, []string{c.name})
	}

	return c.showCommand(cmd, []string{c.name})
}

func (c *ctxCommand) removeCommand(_ *cobra.Command, args []string) error {
	c.name = args[0]

	selected, err := c.registry().Selected(ctx)
	if err != nil && !errors.Is(err, natscontext.ErrNoneSelected) {
		return err
	}

	if selected == c.name {
		if !c.force {
			return fmt.Errorf("cannot remove the selected context, select another one or use the unselect command")
		}

		_, err := c.registry().Unselect(ctx)
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete context %q", c.name), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %s", err)
		}

		if !ok {
			return nil
		}
	}

	return c.registry().Delete(ctx, c.name)
}

func (c *ctxCommand) switchPreviousCtx(cmd *cobra.Command, args []string) error {
	ctxToSwitch, err := c.registry().Previous(ctx)
	if err != nil && !errors.Is(err, natscontext.ErrNoneSelected) {
		return err
	}

	if ctxToSwitch == "" {
		return c.showCommand(cmd, args)
	}

	_, err = c.registry().Select(ctx, ctxToSwitch)
	if err != nil {
		return err
	}

	return c.showCommand(cmd, args)
}

func (c *ctxCommand) unselectCommand(_ *cobra.Command, _ []string) error {
	current, err := c.registry().Selected(ctx)
	if err != nil && !errors.Is(err, natscontext.ErrNoneSelected) {
		return err
	}

	if current == "" {
		fmt.Println("No context currently selected")
		return nil
	}

	_, err = c.registry().Unselect(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Unselected the %q context\n", current)

	return nil
}

func (c *ctxCommand) selectCommand(cmd *cobra.Command, args []string) error {
	c.name = argValue(args, 0)

	known, err := c.registry().List(ctx)
	if err != nil {
		return err
	}

	if len(known) == 0 {
		return fmt.Errorf("no context defined")
	}

	if c.name == "" {
		err := iu.AskOne(&survey.Select{
			Message:  "Select a Context",
			Options:  known,
			PageSize: iu.SelectPageSize(len(known)),
		}, &c.name)
		if err != nil {
			return err
		}
	}

	if c.name == "" {
		return fmt.Errorf("please select a context to activate")
	}

	_, err = c.registry().Select(ctx, c.name)
	if err != nil {
		return err
	}

	return c.showCommand(cmd, []string{c.name})
}
