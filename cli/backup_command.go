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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/backup"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/spf13/cobra"
)

type backupCmd struct {
	source string
	target string
	json   bool

	stream            string
	healthCheck       bool
	snapShotConsumers bool
	chunkSize         string
	wndSize           string
	force             bool
	failOnWarn        bool
	inputFile         string
	placementCluster  string
	placementTags     []string
	replicas          int64

	subjects            []string
	excludeSubjects     []string
	after               string
	before              string
	firstSeq            uint64
	firstSeqSet         bool
	lastSeq             uint64
	lastSeqSet          bool
	headers             []string
	excludeHeaders      []string
	payloadMatch        []string
	excludePayloadMatch []string
	lastPerSubject      int
	lastPerSubjectSet   bool
	kvCompact           bool
	renumber            bool
	dryRun              bool
	obfuscate           bool
	obfuscationKey      string
	listSubjects        bool
	keyFile             string
	values              []string
	showProgress        bool
}

func configureBackupCommand(app commandHost) {
	c := &backupCmd{}

	bk := addCommand(app, "backup", "Backup, restore, inspect and edit JetStream streams")
	bk.Long = `Backups are taken over the NATS network into a directory. The edit, info, validate and lookup commands work on that directory and support only backups taken from NATS Server 2.15 or newer.`
	addCheat("backup", bk)

	c.streamBackupCommand(bk, "stream")
	c.accountBackupCommand(bk, "account")

	restore := addCommand(bk, "restore", "Restores backups over the NATS network")
	c.streamRestoreCommand(restore, "stream")
	c.accountRestoreCommand(restore, "account")

	edit := addCommand(bk, "edit", "Creates an edited copy of a stream backup")
	edit.RunE = c.editAction
	cmdAddTags(edit, "scope:user", "impact:ro")
	addArg(edit, "source", "The directory holding the backup to edit", true, "path")
	addArg(edit, "target", "Directory to create the edited backup in", true, "string")
	edit.Flags().StringArrayVar(&c.subjects, "subject", nil, "Keep messages matching a subject pattern")
	flagPlaceholder(edit, "subject", "PATTERN")
	edit.Flags().StringArrayVar(&c.excludeSubjects, "exclude-subject", nil, "Drop messages matching a subject pattern")
	flagPlaceholder(edit, "exclude-subject", "PATTERN")
	edit.Flags().StringVar(&c.after, "after", "", "Keep messages stored at or after an RFC3339 time or a duration like 2h")
	flagPlaceholder(edit, "after", "TIME")
	edit.Flags().StringVar(&c.before, "before", "", "Keep messages stored before an RFC3339 time or a duration like 2h")
	flagPlaceholder(edit, "before", "TIME")
	edit.Flags().Uint64Var(&c.firstSeq, "first-seq", 0, "Keep messages with this sequence or higher")
	flagPlaceholder(edit, "first-seq", "N")
	edit.Flags().Uint64Var(&c.lastSeq, "last-seq", 0, "Keep messages with this sequence or lower")
	flagPlaceholder(edit, "last-seq", "N")
	edit.Flags().StringArrayVar(&c.headers, "header", nil, "Keep messages carrying a header, as NAME or NAME:VALUE")
	flagPlaceholder(edit, "header", "NAME[:VALUE]")
	edit.Flags().StringArrayVar(&c.excludeHeaders, "exclude-header", nil, "Drop messages carrying a header")
	flagPlaceholder(edit, "exclude-header", "NAME")
	edit.Flags().StringArrayVar(&c.payloadMatch, "payload-match", nil, "Keep messages whose payload matches a regular expression")
	flagPlaceholder(edit, "payload-match", "REGEX")
	edit.Flags().StringArrayVar(&c.excludePayloadMatch, "exclude-payload-match", nil, "Drop messages whose payload matches a regular expression")
	flagPlaceholder(edit, "exclude-payload-match", "REGEX")
	edit.Flags().IntVar(&c.lastPerSubject, "last-per-subject", 0, "Keep only the newest N messages of every subject")
	flagPlaceholder(edit, "last-per-subject", "N")
	edit.Flags().BoolVar(&c.kvCompact, "kv-compact", false, "Keep the latest revision of each key and drop deleted keys. KV backups only, exclusive with --last-per-subject")
	edit.Flags().BoolVar(&c.renumber, "renumber", false, "Number the kept messages from 1 and drop all consumers")
	edit.Flags().BoolVar(&c.dryRun, "dry-run", false, "Only shows what the edit would do, does not write the target")
	edit.Flags().BoolVar(&c.obfuscate, "obfuscate", false, "Replace names and subjects with keyed hashes and message bodies with zeros of the same length. Keeps counter stream values. Writes the key file beside the target")
	edit.Flags().StringVar(&c.obfuscationKey, "obfuscation-key", "", "Reuse the secret and mappings from an earlier key file")
	flagPlaceholder(edit, "obfuscation-key", "FILE")
	negatableBoolVar(edit, &c.showProgress, "progress", true, "Enables or disables progress reporting")

	info := addCommand(bk, "info", "Stream backup information")
	info.RunE = c.infoAction
	cmdAddTags(info, "scope:user", "impact:ro")
	addArg(info, "source", "The directory holding the backup", true, "path")
	info.Flags().BoolVar(&c.listSubjects, "subjects", false, "List every subject in the backup with its message count")
	negatableBoolVar(info, &c.showProgress, "progress", true, "Enables or disables progress reporting")
	info.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	validate := addCommand(bk, "validate", "Validates that a stream backup is complete and restorable")
	validate.RunE = c.validateAction
	cmdAddTags(validate, "scope:user", "impact:ro")
	addArg(validate, "source", "The directory holding the backup", true, "path")
	negatableBoolVar(validate, &c.showProgress, "progress", true, "Enables or disables progress reporting")

	lookup := addCommand(bk, "lookup", "Looks up obfuscated values in a key file")
	lookup.RunE = c.lookupAction
	cmdAddTags(lookup, "scope:user", "impact:ro")
	addArg(lookup, "keyfile", "The key file written beside an obfuscated backup", true, "path")
	addArgCumulative(lookup, "value", "Obfuscated values to look up, subjects are looked up token by token", true, "string")
}

func (c *backupCmd) streamBackupCommand(parent *cobra.Command, name string) *cobra.Command {
	cmd := addCommand(parent, name, "Creates a backup of a stream over the NATS network")
	cmd.RunE = c.streamAction
	cmdAddTags(cmd, "scope:user", "impact:ro")
	addArg(cmd, "stream", "Stream to backup", true, "string")
	addArg(cmd, "target", "Directory to create the backup in", true, "string")
	negatableBoolVar(cmd, &c.showProgress, "progress", true, "Enables or disables progress reporting using a progress bar")
	cmd.Flags().BoolVar(&c.healthCheck, "check", false, "Checks the stream for health prior to backup")
	negatableBoolVar(cmd, &c.snapShotConsumers, "consumers", true, "Enable or disable consumer backups")
	cmd.Flags().StringVar(&c.chunkSize, "chunk-size", "", "Sets a specific chunk size that the server will send")
	cmd.Flags().StringVar(&c.wndSize, "window-size", "", "Sets a specific window size that the server will send")

	return cmd
}

func (c *backupCmd) accountBackupCommand(parent *cobra.Command, name string) *cobra.Command {
	cmd := addCommand(parent, name, "Creates a backup of all  JetStream Streams over the NATS network")
	cmd.RunE = c.accountAction
	cmdAddTags(cmd, "scope:user", "impact:ro")
	addArg(cmd, "target", "Directory to create the backup in", true, "string")
	cmd.Flags().BoolVar(&c.healthCheck, "check", false, "Checks the Stream for health prior to backup")
	negatableBoolVar(cmd, &c.snapShotConsumers, "consumers", true, "Enable or disable consumer backups")
	cmd.Flags().BoolVarP(&c.force, "force", "f", false, "Perform backup without prompting")
	cmd.Flags().BoolVarP(&c.failOnWarn, "critical-warnings", "w", false, "Treat warnings as failures")

	return cmd
}

func (c *backupCmd) streamRestoreCommand(parent *cobra.Command, name string) *cobra.Command {
	cmd := addCommand(parent, name, "Restore a stream over the NATS network")
	cmd.RunE = c.restoreStreamAction
	cmdAddTags(cmd, "scope:user", "impact:rw")
	addArg(cmd, "file", "The directory holding the backup to restore", true, "path")
	negatableBoolVar(cmd, &c.showProgress, "progress", true, "Enables or disables progress reporting using a progress bar")
	cmd.Flags().Var(newExistingFileValue(&c.inputFile), "config", "Load a different configuration when restoring the stream")
	cmd.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the stream in a specific cluster")
	cmd.Flags().StringArrayVar(&c.placementTags, "tag", nil, "Place the stream on servers that has specific tags (pass multiple times)")
	cmd.Flags().Int64Var(&c.replicas, "replicas", 0, "Override how many replicas of the data to create")

	return cmd
}

func (c *backupCmd) accountRestoreCommand(parent *cobra.Command, name string) *cobra.Command {
	cmd := addCommand(parent, name, "Restore an account backup over the NATS network")
	cmd.RunE = c.restoreAccountAction
	cmdAddTags(cmd, "scope:user", "impact:rw")
	addArg(cmd, "directory", "The directory holding the account backup to restore", true, "path")
	cmd.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the stream in a specific cluster")
	cmd.Flags().StringArrayVar(&c.placementTags, "tag", nil, "Place the stream on servers that has specific tags (pass multiple times)")

	return cmd
}

// deprecatedCommand hides cmd and warns that it moved to replacement whenever it runs
func deprecatedCommand(cmd *cobra.Command, old string, replacement string) *cobra.Command {
	cmd.Hidden = true
	cmd.PreRunE = func(_ *cobra.Command, _ []string) error {
		fmt.Fprintf(os.Stderr, "WARNING: %q is deprecated and will be removed in a future release, use %q instead\n\n", old, replacement)
		return nil
	}

	return cmd
}

func init() {
	registerCommand("backup", 1, configureBackupCommand)
}

func backupStream(stream *jsm.Stream, showProgress bool, consumers bool, check bool, target string, chunkSize, wndSize int) error {
	first := true
	pmu := sync.Mutex{}
	expected := 1
	timedOut := false

	var progbar progress.Writer
	var tracker *progress.Tracker
	var err error
	var prevMsg time.Time

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	idleTimeout := 5 * time.Second
	if opts().Timeout > idleTimeout {
		idleTimeout = opts().Timeout
	}

	timeout := time.AfterFunc(idleTimeout, func() {
		cancel()
		timedOut = true
	})

	var received uint32

	cb := func(p jsm.SnapshotProgress) {
		if tracker == nil && showProgress {
			if p.BytesExpected() > 0 {
				expected = int(p.BytesExpected())
			}
			progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(expected),
				Units: iu.ProgressUnitsIBytes,
			})
		}

		if first {
			fmt.Printf("Starting backup of Stream %q with %s\n", stream.Name(), humanize.IBytes(p.BytesExpected()))
			if showProgress {
				fmt.Println()
			}

			if p.HealthCheck() {
				fmt.Printf("Health Check was requested, this can take a long time without progress reports\n\n")
			}

			first = false
		}

		if opts().Trace {
			if first {
				fmt.Printf("Received %s chunk %s\n", fiBytes(uint64(p.ChunkSize())), f(p.ChunksReceived()))
			} else {
				fmt.Printf("Received %s chunk %s with time delta %s\n", fiBytes(uint64(p.ChunkSize())), f(p.ChunksReceived()), time.Since(prevMsg))
			}
		}

		if p.ChunksReceived() != received {
			timeout.Reset(idleTimeout)
			received = p.ChunksReceived()
		}

		if tracker != nil {
			tracker.SetValue(int64(p.UncompressedBytesReceived()))
		}

		prevMsg = time.Now()
	}

	sopts := []jsm.SnapshotOption{
		jsm.SnapshotChunkSize(chunkSize),
		jsm.SnapshotWindowSize(wndSize),
		jsm.SnapshotNotify(cb),
	}

	if consumers {
		sopts = append(sopts, jsm.SnapshotConsumers())
	}

	if opts().Trace {
		sopts = append(sopts, jsm.SnapshotDebug())
		showProgress = false
	}

	if check {
		sopts = append(sopts, jsm.SnapshotHealthCheck())
	}

	fp, err := stream.SnapshotToDirectory(ctx, target, sopts...)
	if err != nil {
		return err
	}

	pmu.Lock()
	if tracker != nil {
		tracker.SetValue(int64(expected))
		tracker.MarkAsDone()
		time.Sleep(300 * time.Millisecond)
		progbar.Stop()
	}
	pmu.Unlock()

	fmt.Println()

	if timedOut {
		return fmt.Errorf("backup timed out after receiving no data for a long period")
	}

	fmt.Printf("Received %s compressed data in %s chunks for stream %q in %v, %s uncompressed \n", humanize.IBytes(fp.BytesReceived()), f(fp.ChunksReceived()), stream.Name(), fp.EndTime().Sub(fp.StartTime()).Round(time.Millisecond), fiBytes(fp.UncompressedBytesReceived()))

	return nil
}

func (c *backupCmd) streamAction(_ *cobra.Command, args []string) error {
	c.stream = args[0]
	c.target = args[1]

	var err error

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	stream, err := mgr.LoadStream(c.stream)
	if err != nil {
		return err
	}

	var chunkSize, wndSize int64
	if c.chunkSize != "" {
		if chunkSize, err = iu.ParseStringAsBytes(c.chunkSize, 32); err != nil {
			return err
		}
	}
	if c.wndSize != "" {
		if wndSize, err = iu.ParseStringAsBytes(c.wndSize, 32); err != nil {
			return err
		}
	}

	err = backupStream(stream, c.showProgress, c.snapShotConsumers, c.healthCheck, c.target, int(chunkSize), int(wndSize))
	fatalIfError(err, "snapshot failed")

	return nil
}

func (c *backupCmd) accountAction(_ *cobra.Command, args []string) error {
	c.target = args[0]

	var err error

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	streams, missing, offline, err := mgr.Streams(nil)
	if err != nil {
		return err
	}

	if len(missing) > 0 {
		return fmt.Errorf("could not obtain stream information for %d streams", len(missing))
	}
	if !c.force && len(offline) > 0 {
		return fmt.Errorf("could not obtain stream information for %d offline streams", len(offline))
	}
	if len(streams) == 0 {
		return fmt.Errorf("no streams found")
	}

	totalSize := uint64(0)
	totalConsumers := 0

	for _, s := range streams {
		state, _ := s.LatestState()
		totalConsumers += state.Consumers
		totalSize += state.Bytes
	}

	cols := newColumnsf("Performing backup of all streams to %s", c.target)
	cols.AddRow("Streams", len(streams))
	cols.AddRow("Size", humanize.IBytes(totalSize))
	cols.AddRow("Consumers:", totalConsumers)
	cols.Println()
	cols.Frender(os.Stdout)

	if !c.force {
		ok, err := askConfirmation("Perform backup", false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	err = os.MkdirAll(c.target, 0700)
	if err != nil {
		return err
	}

	var errs []error
	var warns []error

	for _, s := range streams {
		err = backupStream(s, false, c.snapShotConsumers, c.healthCheck, filepath.Join(c.target, s.Name()), 128*1024, 0)
		if errors.Is(err, jsm.ErrMemoryStreamNotSupported) {
			fmt.Printf("Backup of %s failed: %v\n", s.Name(), err)
			warns = append(warns, fmt.Errorf("%s: %w", s.Name(), err))
		} else if err != nil {
			fmt.Printf("Backup of %s failed: %s\n", s.Name(), err)
			errs = append(errs, fmt.Errorf("%s: %s", s.Name(), err))
		}
		fmt.Println()
	}

	if len(warns) > 0 {
		fmt.Printf("Backup Warnings: \n")
		for _, err := range warns {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if len(errs) > 0 {
		fmt.Printf("Backup failures: \n")
		for _, err := range errs {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if len(errs) > 0 || len(warns) > 0 && c.failOnWarn {
		return fmt.Errorf("backup failed")
	}

	return nil
}

func (c *backupCmd) restoreStream(dir string) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	var bm api.JSApiStreamRestoreRequest
	bmj, err := os.ReadFile(filepath.Join(dir, "backup.json"))
	fatalIfError(err, "restore failed")
	err = json.Unmarshal(bmj, &bm)
	fatalIfError(err, "restore failed")

	var cfg *api.StreamConfig

	known, err := mgr.IsKnownStream(bm.Config.Name)
	fatalIfError(err, "Could not check if the stream already exist")
	if known {
		fatalf("Stream %q already exist", bm.Config.Name)
	}

	var progbar progress.Writer
	var tracker *progress.Tracker
	var prevMsg time.Time

	cb := func(p jsm.RestoreProgress) {
		if opts().Trace {
			if p.ChunksSent()%100 == 0 || time.Since(prevMsg) > 500*time.Millisecond {
				fmt.Printf("Sent %v chunk %v / %v at %v / s\n", fiBytes(uint64(p.ChunkSize())), p.ChunksSent(), p.ChunksToSend(), fiBytes(p.BytesPerSecond()))
				prevMsg = time.Now()
			}
			return
		}

		if progbar == nil {
			progbar, tracker, _ = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(p.ChunksToSend() * p.ChunkSize()),
				Units: progress.UnitsBytes,
			})
		}

		tracker.SetValue(int64(p.ChunksSent() * uint32(p.ChunkSize())))
	}

	var ropts []jsm.SnapshotOption

	if c.showProgress {
		ropts = append(ropts, jsm.RestoreNotify(cb))
	} else {
		ropts = append(ropts, jsm.SnapshotDebug())
	}

	if c.inputFile != "" {
		cfg, err = (&streamCmd{}).loadConfigFile(c.inputFile)
		if err != nil {
			return err
		}

		// we need to confirm this new config has the same stream
		// name as the snapshot else the server state can get confused
		// see https://github.com/nats-io/nats-server/issues/2850
		if bm.Config.Name != cfg.Name {
			return fmt.Errorf("stream names may not be changed during restore")
		}
	} else {
		cfg = &bm.Config
	}

	if c.placementCluster != "" || len(c.placementTags) > 0 {
		cfg.Placement = &api.Placement{
			Cluster: c.placementCluster,
			Tags:    c.placementTags,
		}
	}

	if c.replicas > 0 {
		cfg.Replicas = int(c.replicas)
	}

	if cfg != nil {
		ropts = append(ropts, jsm.RestoreConfiguration(*cfg))
	}

	fmt.Printf("Starting restore of Stream %q from file %q\n\n", bm.Config.Name, dir)

	fp, _, err := mgr.RestoreSnapshotFromDirectory(ctx, bm.Config.Name, dir, ropts...)
	fatalIfError(err, "restore failed")
	if progbar != nil {
		tracker.SetValue(int64(fp.ChunksSent() * uint32(fp.ChunkSize())))
		time.Sleep(300 * time.Millisecond)
		progbar.Stop()
	}

	fmt.Println()
	fmt.Printf("Restored stream %q in %v\n", bm.Config.Name, fp.EndTime().Sub(fp.StartTime()).Round(time.Second))
	fmt.Println()

	stream, err := mgr.LoadStream(bm.Config.Name)
	fatalIfError(err, "could not request Stream info")
	err = (&streamCmd{}).showStream(stream)
	fatalIfError(err, "could not show stream")

	return nil
}

func (c *backupCmd) restoreStreamAction(_ *cobra.Command, args []string) error {
	if err := newExistingDirValue(&c.source).Set(args[0]); err != nil {
		return err
	}

	return c.restoreStream(c.source)
}

func (c *backupCmd) restoreAccountAction(_ *cobra.Command, args []string) error {
	if err := newExistingDirValue(&c.source).Set(args[0]); err != nil {
		return err
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")
	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return err
	}
	existingStreams := map[string]struct{}{}
	for _, n := range streams {
		existingStreams[n] = struct{}{}
	}
	de, err := os.ReadDir(c.source)
	fatalIfError(err, "setup failed")
	for _, d := range de {
		if !d.IsDir() {
			fatalf("expected a directory %q", d.Name())
		}
		if _, ok := existingStreams[d.Name()]; ok {
			fatalf("stream %q exists already", d.Name())
		}
		_, err := os.Stat(filepath.Join(c.source, d.Name(), "backup.json"))
		fatalIfError(err, "expected backup.json")
	}
	fmt.Printf("Restoring backup of all %d streams in directory %q\n\n", len(de), c.source)
	for _, d := range de {
		err := c.restoreStream(filepath.Join(c.source, d.Name()))
		fatalIfError(err, "restore for %s failed", d.Name())
	}
	return nil
}

func (c *backupCmd) parseBackupTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	d, err := parseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q: expected an RFC3339 time or a duration like 2h", s)
	}
	return time.Now().Add(-d), nil
}

func (c *backupCmd) editOptions() ([]backup.EditOption, error) {
	var opts []backup.EditOption

	if len(c.subjects) > 0 {
		opts = append(opts, backup.Subjects(c.subjects...))
	}
	if len(c.excludeSubjects) > 0 {
		opts = append(opts, backup.ExcludeSubjects(c.excludeSubjects...))
	}
	if c.after != "" {
		t, err := c.parseBackupTime(c.after)
		if err != nil {
			return nil, err
		}
		opts = append(opts, backup.After(t))
	}
	if c.before != "" {
		t, err := c.parseBackupTime(c.before)
		if err != nil {
			return nil, err
		}
		opts = append(opts, backup.Before(t))
	}
	if c.firstSeqSet {
		opts = append(opts, backup.FirstSeq(c.firstSeq))
	}
	if c.lastSeqSet {
		opts = append(opts, backup.LastSeq(c.lastSeq))
	}
	for _, h := range c.headers {
		name, value, hasValue := strings.Cut(h, ":")
		if hasValue {
			opts = append(opts, backup.HeaderValue(name, value))
		} else {
			opts = append(opts, backup.HeaderPresent(name))
		}
	}
	for _, h := range c.excludeHeaders {
		opts = append(opts, backup.NoHeader(h))
	}
	for _, expr := range c.payloadMatch {
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid payload expression %q: %w", expr, err)
		}
		opts = append(opts, backup.PayloadMatch(re))
	}
	for _, expr := range c.excludePayloadMatch {
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid payload expression %q: %w", expr, err)
		}
		opts = append(opts, backup.ExcludePayloadMatch(re))
	}
	if c.lastPerSubjectSet {
		opts = append(opts, backup.LastPerSubject(c.lastPerSubject))
	}
	if c.kvCompact {
		opts = append(opts, backup.KVCompact())
	}
	if c.renumber {
		opts = append(opts, backup.Renumber())
	}
	if c.dryRun {
		opts = append(opts, backup.DryRun())
	}
	if c.obfuscate {
		opts = append(opts, backup.Obfuscate())
	}
	if c.obfuscationKey != "" {
		opts = append(opts, backup.ObfuscationKeyFile(c.obfuscationKey))
	}
	opts = append(opts, backup.ToolVersion(fmt.Sprintf("nats %s", Version)))

	return opts, nil
}

func (c *backupCmd) editAction(cmd *cobra.Command, args []string) error {
	if err := newExistingDirValue(&c.source).Set(args[0]); err != nil {
		return err
	}
	c.target = args[1]
	c.firstSeqSet = cmd.Flags().Changed("first-seq")
	c.lastSeqSet = cmd.Flags().Changed("last-seq")
	c.lastPerSubjectSet = cmd.Flags().Changed("last-per-subject")

	opts, err := c.editOptions()
	if err != nil {
		return err
	}
	cb, finish := c.readProgress("Editing")
	opts = append(opts, backup.EditNotify(cb))

	res, err := backup.Edit(ctx, c.source, c.target, opts...)
	finish()
	if err != nil {
		return err
	}

	c.showEditResult(res)

	return nil
}

// readProgress shows the bar backup stream uses while an archive is read,
// one bar spanning every pass. finish completes it before any report prints
func (c *backupCmd) readProgress(verb string) (cb func(backup.Progress), finish func()) {
	var progbar progress.Writer
	var tracker *progress.Tracker
	var total int64
	first := true
	header := c.showProgress && !c.json
	bar := header && !opts().Trace

	cb = func(p backup.Progress) {
		if first {
			first = false
			total = int64(p.BytesTotal()) * int64(p.Passes())
			if !header {
				return
			}
			passes := ""
			if p.Passes() > 1 {
				passes = fmt.Sprintf(" in %d passes", p.Passes())
			}
			fmt.Printf("%s backup in %s with %s%s\n", verb, c.source, humanize.IBytes(p.BytesTotal()), passes)
			if bar {
				fmt.Println()
				progbar, tracker, _ = iu.NewProgress(opts(), &progress.Tracker{
					Total: total,
					Units: iu.ProgressUnitsIBytes,
				})
			}
		}

		if tracker != nil {
			tracker.SetValue(int64(p.Pass()-1)*int64(p.BytesTotal()) + int64(p.BytesRead()))
		}
	}

	finish = func() {
		if tracker != nil {
			tracker.SetValue(total)
			tracker.MarkAsDone()
			time.Sleep(300 * time.Millisecond)
			progbar.Stop()
			tracker = nil
		}
		if header && !first {
			fmt.Println()
		}
	}

	return cb, finish
}

func (c *backupCmd) showEditResult(res *backup.Result) {
	rep := res.Report

	var table *iu.Table
	if c.dryRun {
		table = iu.NewTableWriter(opts(), "Dry Run, Nothing Was Written")
	} else {
		table = iu.NewTableWriter(opts(), "Backup Edit Report")
	}

	table.AddRow("Stream", res.Config.Name)
	table.AddRow("Target", c.target)
	table.AddSeparator()
	table.AddRow("Source Messages", f(rep.SourceMessages))
	table.AddRow("Source Subjects", f(rep.SourceSubjects))
	table.AddRow("Messages Kept", f(rep.Kept))
	table.AddRow("Subjects Kept", f(rep.KeptSubjects))
	for _, d := range []struct {
		name  string
		count uint64
	}{
		{"Dropped by Sequence Filter", rep.Dropped.Sequence},
		{"Dropped by Time Filter", rep.Dropped.Time},
		{"Dropped by Subject Filter", rep.Dropped.Subject},
		{"Dropped by Header Filter", rep.Dropped.Header},
		{"Dropped by Payload Filter", rep.Dropped.Payload},
		{"Dropped by Last Per Subject", rep.Dropped.LastPerSubject},
		{"Dropped by KV Compaction", rep.Dropped.KVCompact},
	} {
		if d.count > 0 {
			table.AddRow(d.name, f(d.count))
		}
	}
	table.AddRow("Consumers Kept", f(rep.ConsumersKept))
	table.AddRow("Consumers Dropped", f(rep.ConsumersDropped))
	if rep.TombstonesRemovedByContentFilters > 0 {
		table.AddRow("KV Tombstones Removed by Content Filters", f(rep.TombstonesRemovedByContentFilters))
	}
	if c.kvCompact || c.lastPerSubjectSet {
		table.AddRow("Subject State Keys", f(rep.SubjectStateKeys))
		table.AddRow("Subject State Bytes", humanize.IBytes(rep.SubjectStateBytes))
	}

	for _, w := range rep.Warnings {
		table.AddRow("Warning", w)
	}

	table.AddSeparator()
	table.AddRow("Restored Messages", f(res.State.Msgs))
	table.AddRow("Restored Bytes", humanize.IBytes(res.State.Bytes))
	table.AddRow("Restored First Sequence", f(res.State.FirstSeq))
	table.AddRow("Restored Last Sequence", f(res.State.LastSeq))
	table.AddRow("Restored Consumers", f(res.State.Consumers))

	if rep.Obfuscation != nil {
		table.AddSeparator()
		table.AddRow("Obfuscated Tokens", f(rep.Obfuscation.TokensMapped))
		table.AddRow("Message Bodies Padded", f(rep.Obfuscation.BodiesPadded))
		if rep.Obfuscation.KeyFile != "" {
			table.AddRow("Key File", rep.Obfuscation.KeyFile)
		}
	}

	fmt.Println(table.Render())

	if rep.Obfuscation != nil && rep.Obfuscation.KeyFile != "" {
		fmt.Println("The key file maps the obfuscated names back to the originals: share the backup, keep the key file private.")
		fmt.Println()
	}
}

func (c *backupCmd) infoAction(_ *cobra.Command, args []string) error {
	if err := newExistingDirValue(&c.source).Set(args[0]); err != nil {
		return err
	}

	cb, finish := c.readProgress("Reading")
	infoOpts := []backup.ScanOption{backup.ScanNotify(cb)}
	if c.listSubjects {
		infoOpts = append(infoOpts, backup.WithSubjects())
	}
	nfo, err := backup.Info(c.source, infoOpts...)
	finish()
	if err != nil {
		return err
	}

	if c.json {
		return iu.PrintJSON(nfo)
	}

	cfg := nfo.Config
	cols := newColumnsf("Information for backup of Stream %s in %s", cfg.Name, c.source)

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", cfg.Name)
	cols.AddRowIf("Subjects", cfg.Subjects, len(cfg.Subjects) > 0)
	cols.AddRow("Storage", cfg.Storage.String())
	cols.AddRow("Retention", cfg.Retention.String())
	cols.AddRow("Replicas", cfg.Replicas)
	cols.AddRowUnlimited("Maximum Messages", cfg.MaxMsgs, -1)
	cols.AddRowUnlimitedIf("Maximum Per Subject", cfg.MaxMsgsPer, cfg.MaxMsgsPer <= 0)
	cols.AddRowUnlimitedIf("Maximum Bytes", humanize.IBytes(uint64(max(cfg.MaxBytes, 0))), cfg.MaxBytes == -1)
	cols.AddRowUnlimitedIf("Maximum Age", cfg.MaxAge, cfg.MaxAge <= 0)
	cols.AddRowUnlimitedIf("Maximum Message Size", humanize.IBytes(uint64(max(int64(cfg.MaxMsgSize), 0))), cfg.MaxMsgSize == -1)
	cols.AddRowUnlimited("Maximum Consumers", int64(cfg.MaxConsumers), -1)

	cols.AddSectionTitle("Consumers")
	cols.AddRow("Count", len(nfo.Consumers))
	cols.AddRowIf("Names", nfo.Consumers, len(nfo.Consumers) > 0)

	cols.AddSectionTitle("Messages")
	cols.AddRow("Messages", nfo.Messages)
	cols.AddRow("Bytes", humanize.IBytes(nfo.Bytes))
	if nfo.Messages > 0 {
		cols.AddRowf("First Sequence", "%s @ %s", f(nfo.FirstSeq), f(nfo.FirstTime))
		cols.AddRowf("Last Sequence", "%s @ %s", f(nfo.LastSeq), f(nfo.LastTime))
	}
	cols.AddRow("Subjects", nfo.NumSubjects)

	if nfo.DeclaredCountsAdvisory {
		cols.AddSectionTitle("Declared State (message and byte counts are advisory)")
	} else {
		cols.AddSectionTitle("Declared State")
	}
	cols.AddRow("Messages", nfo.Declared.Msgs)
	cols.AddRow("Bytes", humanize.IBytes(nfo.Declared.Bytes))
	cols.AddRow("First Sequence", nfo.Declared.FirstSeq)
	cols.AddRow("Last Sequence", nfo.Declared.LastSeq)
	cols.AddRow("Consumers", nfo.Declared.Consumers)

	if nfo.Edit != nil {
		cols.AddSectionTitle("Edit")
		cols.AddRowIf("Tool Version", nfo.Edit.Version, nfo.Edit.Version != "")
		if len(nfo.Edit.Options) > 0 {
			cols.AddRow("Options", nfo.Edit.Options)
		} else {
			cols.AddRow("Options", "none")
		}
		cols.AddRow("Source Digest", nfo.Edit.SourceDigest)
		cols.AddRow("Obfuscated", nfo.Edit.Obfuscated)
	}

	if nfo.Source != nil {
		cols.AddSectionTitle("Source")
		cols.AddRowIf("Subjects", nfo.Source.Subjects, len(nfo.Source.Subjects) > 0)
		cols.AddRowIf("Stream", nfo.Source.Stream, nfo.Source.Stream != "")
		cols.AddRow("Started", f(nfo.Source.Started))
		cols.AddRow("Ended", f(nfo.Source.Ended))
		cols.AddRow("Dropped", nfo.Source.Dropped)
	}

	cols.Frender(os.Stdout)

	if c.listSubjects {
		fmt.Println()
		c.showSubjects(nfo)
	}

	return nil
}

func (c *backupCmd) showSubjects(nfo *backup.InfoReport) {
	names := make([]string, 0, len(nfo.Subjects))
	for s := range nfo.Subjects {
		names = append(names, s)
	}
	sort.Strings(names)

	fmt.Println(subjectsTable(fmt.Sprintf("%d Subjects in backup of stream %s", len(names), nfo.Config.Name), names, nfo.Subjects).Render())
}

func (c *backupCmd) validateAction(_ *cobra.Command, args []string) error {
	if err := newExistingDirValue(&c.source).Set(args[0]); err != nil {
		return err
	}

	cb, finish := c.readProgress("Validating")
	rep, err := backup.Verify(c.source, backup.ScanNotify(cb))
	finish()
	if err != nil {
		return err
	}

	seqs := "no messages"
	if rep.Messages > 0 {
		seqs = fmt.Sprintf("sequences %s to %s", f(rep.FirstSeq), f(rep.LastSeq))
	}
	fmt.Printf("OK: %s entries, %s consumers, %s messages, %s subjects, %s\n", f(rep.Entries), f(rep.Consumers), f(rep.Messages), f(rep.NumSubjects), seqs)

	return nil
}

func (c *backupCmd) lookupAction(_ *cobra.Command, args []string) error {
	if err := newExistingFileValue(&c.keyFile).Set(args[0]); err != nil {
		return err
	}
	c.values = args[1:]

	kf, err := backup.LoadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	for _, v := range c.values {
		fmt.Println(kf.Reveal(v))
	}

	return nil
}
