// Copyright 2025 The NATS Authors
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/natscontext"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/spf13/cobra"
)

// Shell completion for JetStream stream and consumer names. Completion functions
// run during cobra's hidden __complete command, which does NOT execute a
// command's PreRunE, so they establish their own connection. Because connecting
// and listing on every TAB is slow, results are cached on disk per context with
// a short TTL.

const defaultCompletionCacheTTL = 5 * time.Minute

// completionDebug writes diagnostics to stderr when NATS_COMPLETION_DEBUG is set.
// Completion runs in a subprocess whose stdout is parsed by the shell, so debug
// output must go to stderr.
func completionDebug(format string, a ...any) {
	if os.Getenv("NATS_COMPLETION_DEBUG") != "" {
		fmt.Fprintf(os.Stderr, "completion: "+format+"\n", a...)
	}
}

// completionCacheEntry is the on-disk cache payload for one lookup.
type completionCacheEntry struct {
	Updated time.Time `json:"updated"`
	Values  []string  `json:"values"`
}

var completionKeySanitizer = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// completionCacheTTL returns the cache lifetime. NATS_COMPLETION_CACHE_TTL
// overrides the default and a value of 0 (or negative) disables caching.
func completionCacheTTL() time.Duration {
	if v := os.Getenv("NATS_COMPLETION_CACHE_TTL"); v != "" {
		if d, err := parseDuration(v); err == nil {
			return d
		}
	}
	return defaultCompletionCacheTTL
}

func completionCacheDir() (string, error) {
	base, err := iu.XdgCacheHome()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, "nats", "cli", "completion")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

// completionContextKey identifies the active context so cached names from one
// server are not offered for another.
func completionContextKey() string {
	name := opts().CfgCtx
	if name == "" {
		name = natscontext.SelectedContext()
	}
	if name == "" {
		name = "default"
	}
	return completionKeySanitizer.ReplaceAllString(name, "_")
}

func completionCacheFile(key string) (string, error) {
	dir, err := completionCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, completionKeySanitizer.ReplaceAllString(key, "_")+".json"), nil
}

// readCompletionCache returns the cached values for key when present and not
// older than the TTL. A non-positive TTL disables the cache entirely.
func readCompletionCache(key string) ([]string, bool) {
	if completionCacheTTL() <= 0 {
		return nil, false
	}
	file, err := completionCacheFile(key)
	if err != nil {
		return nil, false
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, false
	}
	var entry completionCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}
	if time.Since(entry.Updated) > completionCacheTTL() {
		return nil, false
	}
	return entry.Values, true
}

func writeCompletionCache(key string, values []string) {
	if completionCacheTTL() <= 0 {
		return
	}
	file, err := completionCacheFile(key)
	if err != nil {
		return
	}
	data, err := json.Marshal(completionCacheEntry{Updated: time.Now(), Values: values})
	if err != nil {
		return
	}
	_ = os.WriteFile(file, data, 0600)
}

// completionManager establishes a manager for use inside a completion function.
// Env backed flags are applied (PreRunE does not run during __complete) and the
// timeout is capped so completion never hangs on an unreachable server.
func completionManager(cmd *cobra.Command) (*jsm.Manager, error) {
	_ = applyEnvVars(cmd)

	if o := opts(); o.Timeout <= 0 || o.Timeout > 2*time.Second {
		o.Timeout = 2 * time.Second
	}

	// The root PersistentPreRunE may run during __complete (before flags are
	// parsed) via EnableTraverseRunHooks, caching a context built without the
	// connection flags. Discard it and reload now that the flags are parsed.
	// softFail=true is required so loadContext synthesizes an ephemeral context
	// from --server and the other connection flags when none is selected
	// (loadContext(false), as used by prepareHelper, would not).
	o := opts()
	o.Config = nil
	if err := loadContext(true); err != nil && !errors.Is(err, ErrContextNotFound) {
		return nil, err
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	return mgr, err
}

// streamCompletionKey and consumerCompletionKey build the cache keys shared by
// the completion readers and the ls-driven cache refreshers, so they cannot drift.
func streamCompletionKey() string { return completionContextKey() + "-streams" }

func consumerCompletionKey(stream string) string {
	return completionContextKey() + "-consumers-" + stream
}

// refreshStreamCompletionCache stores a complete (unfiltered) set of stream
// names gathered by another command, e.g. `nats stream ls`, so later
// completions are both warm and fresh. It is a no-op when caching is disabled.
func refreshStreamCompletionCache(names []string) {
	writeCompletionCache(streamCompletionKey(), names)
}

// refreshConsumerCompletionCache stores the consumer names of a stream gathered
// by another command, e.g. `nats consumer ls <stream>`.
func refreshConsumerCompletionCache(stream string, names []string) {
	if stream == "" {
		return
	}
	writeCompletionCache(consumerCompletionKey(stream), names)
}

func cachedStreamNames(cmd *cobra.Command) ([]string, error) {
	key := streamCompletionKey()
	if v, ok := readCompletionCache(key); ok {
		return v, nil
	}

	mgr, err := completionManager(cmd)
	if err != nil {
		return nil, err
	}
	names, err := mgr.StreamNames(nil)
	if err != nil {
		return nil, err
	}

	writeCompletionCache(key, names)
	return names, nil
}

func cachedConsumerNames(cmd *cobra.Command, stream string) ([]string, error) {
	key := consumerCompletionKey(stream)
	if v, ok := readCompletionCache(key); ok {
		return v, nil
	}

	mgr, err := completionManager(cmd)
	if err != nil {
		return nil, err
	}
	names, err := mgr.ConsumerNames(stream)
	if err != nil {
		return nil, err
	}

	writeCompletionCache(key, names)
	return names, nil
}

// completeStreamNames is a cobra completion function for stream name arguments
// and --stream flags.
func completeStreamNames(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	names, err := cachedStreamNames(cmd)
	if err != nil {
		completionDebug("stream names: %v", err)
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return matchCompletionPrefix(names, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// completeConsumerNames is a cobra completion function for consumer name
// arguments. The owning stream is taken from the --stream flag or the preceding
// positional stream argument.
func completeConsumerNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	stream := streamForCompletion(cmd, args)
	if stream == "" {
		completionDebug("consumer names: no stream resolved")
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	names, err := cachedConsumerNames(cmd, stream)
	if err != nil {
		completionDebug("consumer names for %q: %v", stream, err)
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return matchCompletionPrefix(names, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// streamForCompletion resolves the stream a consumer completion applies to,
// preferring an explicit --stream flag and otherwise the positional stream arg.
func streamForCompletion(cmd *cobra.Command, args []string) string {
	if f := cmd.Flags().Lookup("stream"); f != nil && f.Value.String() != "" {
		return f.Value.String()
	}
	for i, m := range commandArgs(cmd) {
		if m.name == "stream" && i < len(args) {
			return args[i]
		}
	}
	return ""
}

// registerFlagCompletions walks the command tree and attaches stream-name
// completion to every --stream flag.
func registerFlagCompletions(cmd *cobra.Command) {
	if cmd.Flags().Lookup("stream") != nil {
		_ = cmd.RegisterFlagCompletionFunc("stream", completeStreamNames)
	}
	for _, c := range cmd.Commands() {
		registerFlagCompletions(c)
	}
}

// completeContextNames is a cobra completion function for the --context flag. It
// lists configured contexts from the local filesystem, so no server connection
// or caching is involved.
func completeContextNames(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return matchCompletionPrefix(natscontext.KnownContexts(), toComplete), cobra.ShellCompDirectiveNoFileComp
}

func matchCompletionPrefix(values []string, toComplete string) []string {
	if toComplete == "" {
		return values
	}
	var out []string
	for _, v := range values {
		if strings.HasPrefix(v, toComplete) {
			out = append(out, v)
		}
	}
	return out
}
