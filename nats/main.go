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

package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/nats-io/natscli/plugins"

	"github.com/spf13/cobra"

	"github.com/nats-io/natscli/cli"
)

var version = "development"

func main() {
	help := `NATS Utility

NATS Server and JetStream administration.

See 'nats cheat' for a quick cheatsheet of commands`

	ncli := &cobra.Command{
		Use:           "nats",
		Short:         "NATS Utility",
		Long:          help,
		Version:       getVersion(),
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	ncli.SetVersionTemplate("{{.Version}}\n")

	cli.SetLLMExtraInformation(`

This application supports LLM friendly output when ran with LLMFORMAT=1

The application applies tags to its commands that are visible in help output:

 - scope:user - Operates at user level, no system credentials needed
 - scope:system - Operates at system level, requires system credentials
 - impact:ro - Read only operation, does not modify NATS data or state
 - impact:rw - Read and write operation, modifies NATS data or state

LLM optimized help output can be obtained using --help-llm for any command. You must set LLMFORMAT=1 for all invocations of this command including when looking for help.
`)

	opts, err := cli.ConfigureInApp(ncli, nil, true)
	if err != nil {
		return
	}
	cli.SetVersion(getVersion())

	pf := ncli.PersistentFlags()
	pf.StringVarP(&opts.Servers, "server", "s", "", "NATS server urls")
	pf.StringVar(&opts.Username, "user", "", "Username or Token")
	pf.StringVar(&opts.Password, "password", "", "Password")
	pf.StringVar(&opts.Token, "token", "", "Token")
	pf.StringVar(&opts.ConnectionName, "connection-name", "NATS CLI Version "+getVersion(), "Nickname to use for the underlying NATS Connection")
	pf.StringVar(&opts.Creds, "creds", "", "User credentials")
	pf.StringVar(&opts.Nkey, "nkey", "", "User NKEY")
	pf.StringVar(&opts.UserJwt, "jwt", "", "User JWT")
	pf.StringVar(&opts.UserSeed, "seed", "", "User seed")
	pf.Var(cli.NewExistingFileValue(&opts.TlsCert), "tlscert", "TLS public certificate")
	pf.Var(cli.NewExistingFileValue(&opts.TlsKey), "tlskey", "TLS private key")
	pf.Var(cli.NewExistingFileValue(&opts.TlsCA), "tlsca", "TLS certificate authority chain")
	cli.NegatablePersistentBoolVar(ncli, &opts.TlsFirst, "tlsfirst", false, "Perform TLS handshake before expecting the server greeting")
	// Alias to match nsc's spelling (--tls-first). Hidden so --help keeps
	// showing only the canonical --tlsfirst; both names bind to the same
	// variable so either spelling works in scripts.
	pf.BoolVar(&opts.TlsFirst, "tls-first", false, "alias for --tlsfirst")
	_ = pf.MarkHidden("tls-first")
	if runtime.GOOS == "windows" {
		pf.Var(cli.NewEnumValue(&opts.WinCertStoreType, "", "user", "windowscurrentuser", "machine", "windowslocalmachine"), "certstore", "Uses a Windows Certificate Store for TLS (user, machine)")
		pf.StringVar(&opts.WinCertStoreMatch, "certstore-match", "", "Which certificate to use in the store")
		pf.Var(cli.NewEnumValue(&opts.WinCertStoreMatchBy, "subject", "subject", "issuer"), "certstore-match-by", "Configures the way certificates are searched for (subject, issuer)")
		pf.StringArrayVar(&opts.WinCertCaStoreMatch, "certstore-ca-match", nil, "Which certificate authority should be used from the store")
	}
	pf.DurationVar(&opts.Timeout, "timeout", 5*time.Second, "Time to wait on responses from NATS")
	pf.StringVar(&opts.SocksProxy, "socks-proxy", "", "SOCKS5 proxy for connecting to NATS server")
	pf.StringVar(&opts.JsApiPrefix, "js-api-prefix", "", "Subject prefix for access to JetStream API")
	pf.StringVar(&opts.JsEventPrefix, "js-event-prefix", "", "Subject prefix for access to JetStream Advisories")
	pf.StringVar(&opts.JsDomain, "js-domain", "", "JetStream domain to access")
	pf.StringVar(&opts.InboxPrefix, "inbox-prefix", "", "Custom inbox prefix to use for inboxes")
	pf.StringVar(&opts.JsDomain, "domain", "", "JetStream domain to access")
	_ = pf.MarkHidden("domain")
	pf.Var(cli.NewEnumValue(&opts.ColorScheme, "", iu.ValidStyles()...), "colors", "Sets a color scheme to use")
	pf.StringVar(&opts.CfgCtx, "context", "", "Configuration context")
	pf.BoolVar(&opts.Trace, "trace", false, "Trace API interactions")
	pf.BoolVar(&cli.SkipContexts, "no-context", false, "Disable the selected context")

	cli.FlagEnvVar(ncli, "server", "NATS_URL")
	cli.FlagEnvVar(ncli, "user", "NATS_USER")
	cli.FlagEnvVar(ncli, "password", "NATS_PASSWORD")
	cli.FlagEnvVar(ncli, "token", "NATS_TOKEN")
	cli.FlagEnvVar(ncli, "creds", "NATS_CREDS")
	cli.FlagEnvVar(ncli, "nkey", "NATS_NKEY")
	cli.FlagEnvVar(ncli, "jwt", "NATS_JWT")
	cli.FlagEnvVar(ncli, "seed", "NATS_SEED")
	cli.FlagEnvVar(ncli, "tlscert", "NATS_CERT")
	cli.FlagEnvVar(ncli, "tlskey", "NATS_KEY")
	cli.FlagEnvVar(ncli, "tlsca", "NATS_CA")
	cli.FlagEnvVar(ncli, "timeout", "NATS_TIMEOUT")
	cli.FlagEnvVar(ncli, "socks-proxy", "NATS_SOCKS_PROXY")
	cli.FlagEnvVar(ncli, "colors", "NATS_COLOR")
	cli.FlagEnvVar(ncli, "context", "NATS_CONTEXT")

	cli.ConfigureCheatCommand(ncli)
	cli.SetupHelp(ncli)

	log.SetFlags(log.Ltime)

	plugins.AddToApp(ncli)

	if cli.InterceptLLMHelp(ncli, os.Args[1:]) {
		return
	}

	ncli.SetArgs(os.Args[1:])
	if err := ncli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "nats: error: %s\n", err)
		os.Exit(1)
	}
}

func getVersion() string {
	if version != "development" {
		return version
	}

	nfo, ok := debug.ReadBuildInfo()
	if !ok || (nfo != nil && nfo.Main.Version == "") {
		return version
	}

	return nfo.Main.Version
}
