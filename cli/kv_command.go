// Copyright 2020-2025 The NATS Authors
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
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
	"golang.org/x/term"

	"github.com/spf13/cobra"
)

type kvCommand struct {
	bucket                  string
	key                     string
	val                     string
	raw                     bool
	history                 int64
	ttl                     time.Duration
	replicas                uint
	force                   bool
	maxValueSize            int64
	maxValueSizeString      string
	maxBucketSize           int64
	maxBucketSizeString     string
	revision                uint64
	description             string
	listNames               bool
	lsVerbose               bool
	lsVerboseDisplayValue   bool
	storage                 string
	placementCluster        string
	placementTags           []string
	repubSource             string
	repubDest               string
	repubHeadersOnly        bool
	mirror                  string
	mirrorDomain            string
	sources                 []string
	compression             bool
	includeHistory          bool
	includeDeletes          bool
	updatesOnly             bool
	limitsMarkerTTL         time.Duration
	keyTTL                  time.Duration
	historySet              bool
	ttlSet                  bool
	replicaSet              bool
	maxValueSizeSet         bool
	maxBucketSizeSet        bool
	descriptionSet          bool
	compressSet             bool
	tagsSet                 bool
	clusterSet              bool
	republishSourceSet      bool
	republishDestinationSet bool
	republishHeadersSet     bool
	markerTTLSet            bool
	sourceSet               bool
	metadataIsSet           bool
	metadata                map[string]string
	noMirror                bool
}

func configureKVCommand(app commandHost) {
	c := &kvCommand{metadata: make(map[string]string)}

	help := `Interacts with a JetStream based Key-Value store

The JetStream Key-Value store uses streams to store key-value pairs
for an indefinite period or a per-bucket configured TTL.
`

	kv := addCommand(app, "kv", help)
	addCheat("kv", kv)

	addCreateFlags := func(f *cobra.Command, edit bool) {
		addArg(f, "bucket", "The bucket to act on", true, "string")
		f.Flags().StringVar(&c.description, "description", "", "A description for the bucket")
		f.Flags().Var(newValidatedInt64Value(&c.history, 1, iu.Int64RangeValidator(1, 64)), "history", "How many historic values to keep per key")
		if !edit {
			f.Flags().Var(newEnumValue(&c.storage, "", "file", "f", "memory", "m"), "storage", "Storage backend to use (file, memory)")
		}
		f.Flags().DurationVar(&c.ttl, "ttl", 0, "How long to keep values for")
		flagPlaceholder(f, "ttl", "DURATION")
		f.Flags().DurationVar(&c.limitsMarkerTTL, "marker-ttl", 0, "Enables Per-Key TTLs and Limit Markers")
		flagPlaceholder(f, "marker-ttl", "DURATION")
		f.Flags().UintVar(&c.replicas, "replicas", 1, "How many replicas of the data to store")
		f.Flags().StringVar(&c.maxValueSizeString, "max-value-size", "", "Maximum size for any single value")
		flagPlaceholder(f, "max-value-size", "BYTES")
		f.Flags().StringVar(&c.maxBucketSizeString, "max-bucket-size", "", "Maximum size for the bucket")
		flagPlaceholder(f, "max-bucket-size", "BYTES")
		f.Flags().StringArrayVar(&c.placementTags, "tags", nil, "Place the bucket on servers that has specific tags")
		f.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the bucket on a specific cluster")
		negatableBoolVar(f, &c.compression, "compress", false, "Compress the bucket data")
		f.Flags().Var(newStringMapValue(&c.metadata), "metadata", "Adds metadata to the stream")
		flagPlaceholder(f, "metadata", "META")
		f.Flags().StringVar(&c.repubSource, "republish-source", "", "Republish messages to --republish-destination")
		flagPlaceholder(f, "republish-source", "SRC")
		f.Flags().StringVar(&c.repubDest, "republish-destination", "", "Republish destination for messages in --republish-source")
		flagPlaceholder(f, "republish-destination", "DEST")
		f.Flags().BoolVar(&c.repubHeadersOnly, "republish-headers", false, "Republish only message headers, no bodies")
		if edit {
			negatableBoolVar(f, &c.noMirror, "no-mirror", false, "Removes mirror configuration from a bucket")
		} else {
			f.Flags().StringVar(&c.mirror, "mirror", "", "Creates a mirror of a different bucket")
			f.Flags().StringVar(&c.mirrorDomain, "mirror-domain", "", "When mirroring find the bucket in a different domain")
		}
		f.Flags().StringArrayVar(&c.sources, "source", nil, "Source from a different bucket")
		flagPlaceholder(f, "source", "BUCKET")
	}

	add := addCommand(kv, "add", "Adds a new KV Store Bucket")
	add.Aliases = []string{"new"}
	add.RunE = c.addAction
	cmdAddTags(add, "scope:user", "impact:rw")
	addCreateFlags(add, false)
	add.PreRunE = c.parseLimitStrings

	edit := addCommand(kv, "edit", "Edits an existing KV Store Bucket")
	edit.RunE = c.editAction
	cmdAddTags(edit, "scope:user", "impact:rw")
	addCreateFlags(edit, true)
	edit.PreRunE = c.parseLimitStrings

	put := addCommand(kv, "put", "Puts a value into a key")
	put.RunE = c.putAction
	cmdAddTags(put, "scope:user", "impact:rw")
	addArg(put, "bucket", "The bucket to act on", true, "string")
	addArg(put, "key", "The key to act on", true, "string")
	addArg(put, "value", "The value to store, when empty reads STDIN", false, "string")

	get := addCommand(kv, "get", "Gets a value for a key")
	get.RunE = c.getAction
	cmdAddTags(get, "scope:user", "impact:ro")
	addArg(get, "bucket", "The bucket to act on", true, "string")
	addArg(get, "key", "The key to act on", true, "string")
	get.Flags().Uint64Var(&c.revision, "revision", 0, "Gets a specific revision")
	get.Flags().BoolVar(&c.raw, "raw", false, "Show only the value string")

	create := addCommand(kv, "create", "Puts a value into a key only if the key is new or it's last operation was a delete")
	create.RunE = c.createAction
	cmdAddTags(create, "scope:user", "impact:rw")
	addArg(create, "bucket", "The bucket to act on", true, "string")
	addArg(create, "key", "The key to act on", true, "string")
	addArg(create, "value", "The value to store, when empty reads STDIN", false, "string")
	create.Flags().DurationVar(&c.keyTTL, "ttl", 0, "Sets a TTL for the key")
	flagPlaceholder(create, "ttl", "DURATION")

	update := addCommand(kv, "update", "Updates a key with a new value if the previous value matches the given revision")
	update.RunE = c.updateAction
	cmdAddTags(update, "scope:user", "impact:rw")
	addArg(update, "bucket", "The bucket to act on", true, "string")
	addArg(update, "key", "The key to act on", true, "string")
	addArg(update, "value", "The value to store", true, "string")
	addArg(update, "revision", "The revision of the previous value in the bucket", true, "uint")

	del := addCommand(kv, "del", "Deletes a key or the entire bucket")
	del.Aliases = []string{"rm"}
	del.RunE = c.deleteAction
	cmdAddTags(del, "scope:user", "impact:rw")
	addArg(del, "bucket", "The bucket to act on", true, "string")
	addArg(del, "key", "The key to act on", false, "string")
	del.Flags().BoolVarP(&c.force, "force", "f", false, "Act without confirmation")

	purge := addCommand(kv, "purge", "Deletes a key from the bucket, clearing history before creating a delete marker")
	purge.RunE = c.purgeAction
	cmdAddTags(purge, "scope:user", "impact:rw")
	addArg(purge, "bucket", "The bucket to act on", true, "string")
	addArg(purge, "key", "The key to act on", true, "string")
	purge.Flags().BoolVarP(&c.force, "force", "f", false, "Act without confirmation")
	purge.Flags().DurationVar(&c.keyTTL, "ttl", 0, "Sets a TTL for the purge marker")
	flagPlaceholder(purge, "ttl", "DURATION")

	history := addCommand(kv, "history", "Shows the full history for a key")
	history.RunE = c.historyAction
	cmdAddTags(history, "scope:user", "impact:ro")
	addArg(history, "bucket", "The bucket to act on", true, "string")
	addArg(history, "key", "The key to act on", true, "string")

	revert := addCommand(kv, "revert", "Reverts a value to a previous revision using put")
	revert.RunE = c.revertAction
	cmdAddTags(revert, "scope:user", "impact:rw")
	addArg(revert, "bucket", "The bucket to act on", true, "string")
	addArg(revert, "key", "The key to act on", true, "string")
	addArg(revert, "revision", "The revision to revert to", true, "uint")
	negatableBoolVar(revert, &c.force, "force", false, "Force reverting without prompting")

	status := addCommand(kv, "info", "View the status of a KV store")
	status.Aliases = []string{"view", "status"}
	status.RunE = c.infoAction
	cmdAddTags(status, "scope:user", "impact:ro")
	addArg(status, "bucket", "The bucket to act on", false, "string")

	watch := addCommand(kv, "watch", "Watch the bucket or a specific key for updated")
	watch.RunE = c.watchAction
	cmdAddTags(watch, "scope:user", "impact:ro")
	addArg(watch, "bucket", "The bucket to act on", true, "string")
	addArgWithDefault(watch, "key", "The key to act on", ">", "string")
	watch.Flags().BoolVar(&c.includeHistory, "history", false, "Includes historic values")
	negatableBoolVar(watch, &c.includeDeletes, "deletes", true, "Includes deletes in watched values")
	watch.Flags().BoolVar(&c.updatesOnly, "updates", false, "Only show new values written")
	watch.Flags().Uint64Var(&c.revision, "revision", 0, "Starts from a certain revision")

	ls := addCommand(kv, "ls", "List available buckets or the keys in a bucket")
	ls.Aliases = []string{"list"}
	ls.RunE = c.lsAction
	cmdAddTags(ls, "scope:user", "impact:ro")
	addArg(ls, "bucket", "The bucket to list the keys", false, "string")
	addArgWithDefault(ls, "key", "The key to act on", "", "string")
	ls.Flags().BoolVarP(&c.listNames, "names", "n", false, "Show just the bucket names")
	ls.Flags().BoolVarP(&c.lsVerbose, "verbose", "v", false, "Show detailed info about the key")
	ls.Flags().BoolVar(&c.lsVerboseDisplayValue, "display-value", false, "Display value in verbose output (has no effect without 'verbose')")

	rmHistory := addCommand(kv, "compact", "Reclaim space used by deleted keys")
	rmHistory.RunE = c.compactAction
	cmdAddTags(rmHistory, "scope:user", "impact:rw")
	addArg(rmHistory, "bucket", "The bucket to act on", true, "string")
	rmHistory.Flags().BoolVarP(&c.force, "force", "f", false, "Act without confirmation")
}

func init() {
	registerCommand("kv", 9, configureKVCommand)
}

func (c *kvCommand) parseLimitStrings(_ *cobra.Command, _ []string) (err error) {
	if c.maxValueSizeString != "" {
		c.maxValueSize, err = iu.ParseStringAsBytes(c.maxValueSizeString, 32)
		if err != nil {
			return err
		}
	}

	if c.maxBucketSizeString != "" {
		c.maxBucketSize, err = iu.ParseStringAsBytes(c.maxBucketSizeString, 64)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *kvCommand) strForOp(op jetstream.KeyValueOp) string {
	switch op {
	case jetstream.KeyValuePut:
		return "PUT"
	case jetstream.KeyValuePurge:
		return "PURGE"
	case jetstream.KeyValueDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func (c *kvCommand) lsAction(_ *cobra.Command, args []string) error {
	c.bucket = argValue(args, 0)
	c.key = argValue(args, 1)

	if c.bucket != "" {
		return c.lsBucketKeys()
	}

	return c.lsBuckets()
}

func (c *kvCommand) lsBucketKeys() error {
	_, js, err := prepareJSHelper()
	if err != nil {
		return fmt.Errorf("unable to prepare js helper: %s", err)
	}

	kv, err := js.KeyValue(ctx, c.bucket)
	if err != nil {
		return fmt.Errorf("unable to load bucket: %s", err)
	}

	var lister jetstream.KeyLister
	//key not empty
	if c.key != "" {
		lister, err = kv.ListKeysFiltered(ctx, c.key)
	} else {
		lister, err = kv.ListKeys(ctx)
	}

	if err != nil {
		return err
	}

	var found bool
	if c.lsVerbose {
		found, err = c.displayKeyInfo(kv, lister)
		if err != nil {
			return fmt.Errorf("unable to display key info: %s", err)
		}
	} else {
		for v := range lister.Keys() {
			found = true
			fmt.Println(v)
		}
	}
	if !found && !c.listNames {
		fmt.Println("No keys found in bucket")
		return nil
	}

	return nil
}

func (c *kvCommand) displayKeyInfo(kv jetstream.KeyValue, keys jetstream.KeyLister) (bool, error) {
	var found bool

	if kv == nil {
		return found, errors.New("key value cannot be nil")
	}

	table := iu.NewTableWriterf(opts(), "Contents for bucket '%s'", c.bucket)

	if c.lsVerboseDisplayValue {
		table.AddHeaders("Key", "Created", "Delta", "Revision", "Value")
	} else {
		table.AddHeaders("Key", "Created", "Delta", "Revision")
	}

	for keyName := range keys.Keys() {
		found = true
		kve, err := kv.Get(ctx, keyName)
		if err != nil {
			return found, fmt.Errorf("unable to fetch key %s: %s", keyName, err)
		}

		row := []interface{}{
			kve.Key(),
			f(kve.Created()),
			kve.Delta(),
			kve.Revision(),
		}

		if c.lsVerboseDisplayValue {
			row = append(row, string(kve.Value()))
		}

		table.AddRow(row...)
	}

	fmt.Println(table.Render())

	return found, nil
}

func (c *kvCommand) lsBuckets() error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	var found []*jsm.Stream

	_, _, err = mgr.EachStream(nil, func(s *jsm.Stream) {
		if s.IsKVBucket() {
			found = append(found, s)
		}
	})
	if err != nil {
		return err
	}

	if c.listNames {
		for _, s := range found {
			fmt.Println(strings.TrimPrefix(s.Name(), "KV_"))
		}
		return nil
	}

	if len(found) == 0 {
		fmt.Println("No Key-Value buckets found")

		return nil
	}

	sort.Slice(found, func(i, j int) bool {
		info, _ := found[i].LatestInformation()
		jnfo, _ := found[j].LatestInformation()

		return info.State.Bytes < jnfo.State.Bytes
	})

	table := iu.NewTableWriterf(opts(), "Key-Value Buckets")
	table.AddHeaders("Bucket", "Description", "Created", "Size", "Values", "Last Update")
	for _, s := range found {
		nfo, _ := s.LatestInformation()

		table.AddRow(strings.TrimPrefix(s.Name(), "KV_"), s.Description(), f(nfo.Created), humanize.IBytes(nfo.State.Bytes), f(nfo.State.Msgs), f(time.Since(nfo.State.LastTime)))
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) revertAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]
	revision, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return err
	}
	c.revision = revision

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	history, err := store.History(ctx, c.key)
	if err != nil {
		return err
	}

	if len(history) <= 1 {
		return errors.New("cannot revert key in a bucket where history=1")
	}

	rev, err := store.GetRevision(ctx, c.key, c.revision)
	if err != nil {
		return err
	}

	if !c.force {
		val := iu.Base64IfNotPrintable(rev.Value())
		if len(val) > 40 {
			val = fmt.Sprintf("%s...%s", val[0:15], val[len(val)-15:])
		}

		fmt.Printf("Revision: %d\n\n%v\n\n", rev.Revision(), val)
		ok, err := askConfirmation(fmt.Sprintf("Really revert to revision %d", c.revision), false)
		fatalIfError(err, "could not obtain confirmation")
		if !ok {
			return nil
		}
	}

	// We get the latest revision number so that we can revert with update() over put()
	latestRevision := history[len(history)-1].Revision()

	_, err = store.Update(ctx, c.key, rev.Value(), latestRevision)
	if err != nil {
		return err
	}

	return c.historyAction(cmd, args)
}

func (c *kvCommand) historyAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	history, err := store.History(ctx, c.key)
	if err != nil {
		return err
	}

	table := iu.NewTableWriterf(opts(), "History for %s > %s", c.bucket, c.key)
	table.AddHeaders("Key", "Revision", "Op", "Created", "Length", "Value")
	for _, r := range history {
		val := iu.Base64IfNotPrintable(r.Value())
		if len(val) > 40 {
			val = fmt.Sprintf("%s...%s", val[0:15], val[len(val)-15:])
		}

		table.AddRow(r.Key(), r.Revision(), c.strForOp(r.Operation()), f(r.Created()), f(len(r.Value())), val)
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) compactAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Purge all historic values and audit trails for deleted keys in bucket %s?", c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	return store.PurgeDeletes(ctx)
}

func (c *kvCommand) deleteAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = argValue(args, 1)

	if c.key == "" {
		return c.rmBucketAction(cmd, args)
	}

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Delete key %s > %s?", c.bucket, c.key), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	return store.Delete(ctx, c.key)
}

func (c *kvCommand) addAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.descriptionSet = cmd.Flags().Changed("description")
	c.historySet = cmd.Flags().Changed("history")
	c.ttlSet = cmd.Flags().Changed("ttl")
	c.markerTTLSet = cmd.Flags().Changed("marker-ttl")
	c.replicaSet = cmd.Flags().Changed("replicas")
	c.maxValueSizeSet = cmd.Flags().Changed("max-value-size")
	c.maxBucketSizeSet = cmd.Flags().Changed("max-bucket-size")
	c.tagsSet = cmd.Flags().Changed("tags")
	c.clusterSet = cmd.Flags().Changed("cluster")
	c.compressSet = cmd.Flags().Changed("compress")
	c.metadataIsSet = cmd.Flags().Changed("metadata")
	c.republishSourceSet = cmd.Flags().Changed("republish-source")
	c.republishDestinationSet = cmd.Flags().Changed("republish-destination")
	c.republishHeadersSet = cmd.Flags().Changed("republish-headers")
	c.sourceSet = cmd.Flags().Changed("source")

	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	storage := jetstream.FileStorage
	if strings.HasPrefix(c.storage, "m") {
		storage = jetstream.MemoryStorage
	}

	var placement *jetstream.Placement
	if c.placementCluster != "" || len(c.placementTags) > 0 {
		placement = &jetstream.Placement{Cluster: c.placementCluster}
		if len(c.placementTags) > 0 {
			placement.Tags = c.placementTags
		}
	}

	cfg := jetstream.KeyValueConfig{
		Bucket:         c.bucket,
		Description:    c.description,
		MaxValueSize:   int32(c.maxValueSize),
		History:        uint8(c.history),
		TTL:            c.ttl,
		MaxBytes:       c.maxBucketSize,
		Storage:        storage,
		Replicas:       int(c.replicas),
		Placement:      placement,
		Compression:    c.compression,
		LimitMarkerTTL: c.limitsMarkerTTL,
		Metadata:       c.metadata,
	}

	if c.repubDest != "" {
		cfg.RePublish = &jetstream.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	if c.mirror != "" {
		cfg.Mirror = &jetstream.StreamSource{
			Name:   c.mirror,
			Domain: c.mirrorDomain,
		}
	}

	for _, source := range c.sources {
		cfg.Sources = append(cfg.Sources, &jetstream.StreamSource{
			Name: source,
		})
	}

	store, err := js.CreateKeyValue(ctx, cfg)
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) editAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.descriptionSet = cmd.Flags().Changed("description")
	c.historySet = cmd.Flags().Changed("history")
	c.ttlSet = cmd.Flags().Changed("ttl")
	c.markerTTLSet = cmd.Flags().Changed("marker-ttl")
	c.replicaSet = cmd.Flags().Changed("replicas")
	c.maxValueSizeSet = cmd.Flags().Changed("max-value-size")
	c.maxBucketSizeSet = cmd.Flags().Changed("max-bucket-size")
	c.tagsSet = cmd.Flags().Changed("tags")
	c.clusterSet = cmd.Flags().Changed("cluster")
	c.compressSet = cmd.Flags().Changed("compress")
	c.metadataIsSet = cmd.Flags().Changed("metadata")
	c.republishSourceSet = cmd.Flags().Changed("republish-source")
	c.republishDestinationSet = cmd.Flags().Changed("republish-destination")
	c.republishHeadersSet = cmd.Flags().Changed("republish-headers")
	c.sourceSet = cmd.Flags().Changed("source")

	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	kv, err := js.KeyValue(ctx, c.bucket)
	if err != nil {
		return err
	}

	status, err := kv.Status(ctx)
	if err != nil {
		return err
	}
	var nfo *jetstream.StreamInfo
	if status.BackingStore() == "JetStream" {
		nfo = status.(*jetstream.KeyValueBucketStatus).StreamInfo()
	} else {
		return errors.New(c.bucket + " is not a JetStream bucket")
	}

	cfg := jetstream.KeyValueConfig{
		Bucket:         c.bucket,
		Description:    nfo.Config.Description,
		MaxValueSize:   int32(nfo.Config.MaxMsgSize),
		History:        uint8(status.History()),
		TTL:            status.TTL(),
		MaxBytes:       nfo.Config.MaxBytes,
		Storage:        nfo.Config.Storage,
		Replicas:       int(nfo.Config.Replicas),
		Placement:      nfo.Config.Placement,
		RePublish:      nfo.Config.RePublish,
		Sources:        nfo.Config.Sources,
		Mirror:         nfo.Config.Mirror,
		Compression:    status.IsCompressed(),
		LimitMarkerTTL: status.LimitMarkerTTL(),
		Metadata:       nfo.Config.Metadata,
	}

	if c.descriptionSet {
		cfg.Description = c.description
	}

	if c.maxValueSizeSet {
		cfg.MaxValueSize = int32(c.maxValueSize)
	}

	if c.historySet {
		cfg.History = uint8(c.history)
	}

	if c.ttlSet {
		cfg.TTL = c.ttl
	}

	if c.maxBucketSizeSet {
		cfg.MaxBytes = c.maxBucketSize
	}

	if c.replicaSet {
		cfg.Replicas = int(c.replicas)
	}

	if c.metadataIsSet {
		cfg.Metadata = c.metadata
	}

	var placement *jetstream.Placement
	if c.clusterSet || c.tagsSet {
		placement = &jetstream.Placement{Cluster: c.placementCluster}
		if len(c.placementTags) > 0 {
			placement.Tags = c.placementTags
		}
		cfg.Placement = placement
	}

	if c.republishDestinationSet {
		cfg.RePublish = &jetstream.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	for _, source := range c.sources {
		cfg.Sources = append(cfg.Sources, &jetstream.StreamSource{
			Name: source,
		})
	}

	if c.compressSet {
		cfg.Compression = c.compression
	}

	if c.markerTTLSet {
		cfg.LimitMarkerTTL = c.limitsMarkerTTL
	}

	if c.noMirror {
		cfg.Mirror = nil
	}

	store, err := js.UpdateKeyValue(ctx, cfg)
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) getAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	var res jetstream.KeyValueEntry
	if c.revision > 0 {
		res, err = store.GetRevision(ctx, c.key, c.revision)
	} else {
		res, err = store.Get(ctx, c.key)
	}
	if err != nil {
		return err
	}

	if c.raw {
		os.Stdout.Write(res.Value())
		return nil
	}

	fmt.Printf("%s > %s revision: %d created @ %s\n", res.Bucket(), res.Key(), res.Revision(), f(res.Created()))
	fmt.Println()
	pv := iu.Base64IfNotPrintable(res.Value())
	lpv := len(pv)
	if len(pv) > 120 {
		fmt.Printf("Showing first 120 bytes of %s, use --raw for full data\n\n", f(lpv))
		fmt.Println(pv[:120])
	} else {
		fmt.Println(iu.Base64IfNotPrintable(res.Value()))
	}

	fmt.Println()

	return nil
}

func (c *kvCommand) putAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]
	c.val = argValue(args, 2)

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	val, err := c.valOrReadVal()
	if err != nil {
		return err
	}

	_, err = store.Put(ctx, c.key, val)
	if err != nil {
		return err
	}

	fmt.Println(c.val)

	return err
}

func (c *kvCommand) createAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]
	c.val = argValue(args, 2)

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	val, err := c.valOrReadVal()
	if err != nil {
		return err
	}

	if c.keyTTL > 0 {
		_, err = store.Create(ctx, c.key, val, jetstream.KeyTTL(c.keyTTL))
	} else {
		_, err = store.Create(ctx, c.key, val)
	}
	if err != nil {
		return err
	}

	fmt.Println(c.val)

	return err
}

func (c *kvCommand) updateAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]
	c.val = args[2]
	revision, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		return err
	}
	c.revision = revision

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	val, err := c.valOrReadVal()
	if err != nil {
		return err
	}

	_, err = store.Update(ctx, c.key, val, c.revision)
	if err != nil {
		return err
	}

	fmt.Println(c.val)

	return err
}

func (c *kvCommand) valOrReadVal() ([]byte, error) {
	if c.val != "" || term.IsTerminal(int(os.Stdin.Fd())) {
		return []byte(c.val), nil
	}

	return io.ReadAll(os.Stdin)
}

func (c *kvCommand) loadBucket() (*nats.Conn, jetstream.JetStream, jetstream.KeyValue, error) {
	nc, js, err := prepareJSHelper()
	if err != nil {
		return nil, nil, nil, err
	}

	if c.bucket == "" {
		known, err := c.knownBuckets(nc)
		if err != nil {
			return nil, nil, nil, err
		}

		if len(known) == 0 {
			return nil, nil, nil, fmt.Errorf("no KV buckets found")
		}

		err = iu.AskOne(&survey.Select{
			Message:  "Select a Bucket",
			Options:  known,
			PageSize: iu.SelectPageSize(len(known)),
		}, &c.bucket)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	store, err := js.KeyValue(ctx, c.bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	return nc, js, store, err
}

func (c *kvCommand) knownBuckets(nc *nats.Conn) ([]string, error) {
	mgr, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return nil, err
	}

	var found []string
	for _, stream := range streams {
		if jsm.IsKVBucketStream(stream) {
			found = append(found, strings.TrimPrefix(stream, "KV_"))
		}
	}

	return found, nil
}

func (c *kvCommand) infoAction(_ *cobra.Command, args []string) error {
	c.bucket = argValue(args, 0)

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) watchAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = ">"
	if v := argValue(args, 1); v != "" {
		c.key = v
	}

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	var opts []jetstream.WatchOpt
	if !c.includeDeletes {
		opts = append(opts, jetstream.IgnoreDeletes())
	}
	if c.includeHistory {
		opts = append(opts, jetstream.IncludeHistory())
	}
	if c.updatesOnly {
		opts = append(opts, jetstream.UpdatesOnly())
	}
	if c.revision > 0 {
		opts = append(opts, jetstream.ResumeFromRevision(c.revision))
	}

	watch, err := store.Watch(ctx, c.key, opts...)
	if err != nil {
		return err
	}
	defer watch.Stop()

	for res := range watch.Updates() {
		if res == nil {
			continue
		}

		switch res.Operation() {
		case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
			fmt.Printf("[%s] %s %s > %s\n", f(res.Created()), color.RedString(c.strForOp(res.Operation())), res.Bucket(), res.Key())
		case jetstream.KeyValuePut:
			fmt.Printf("[%s] %s %s > %s: %s\n", f(res.Created()), color.GreenString(c.strForOp(res.Operation())), res.Bucket(), res.Key(), res.Value())
		}
	}

	return nil
}

func (c *kvCommand) purgeAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.key = args[1]

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Purge key %s > %s?", c.bucket, c.key), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping purge")
			return nil
		}
	}

	if c.keyTTL > 0 {
		return store.Purge(ctx, c.key, jetstream.PurgeTTL(c.keyTTL))
	}

	return store.Purge(ctx, c.key)
}

func (c *kvCommand) rmBucketAction(_ *cobra.Command, _ []string) error {
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Delete bucket %s?", c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	return js.DeleteKeyValue(ctx, c.bucket)
}

func (c *kvCommand) showStatus(store jetstream.KeyValue) error {
	status, err := store.Status(ctx)
	if err != nil {
		return err
	}

	var nfo *jetstream.StreamInfo
	if status.BackingStore() == "JetStream" {
		nfo = status.(*jetstream.KeyValueBucketStatus).StreamInfo()
	}

	cols := newColumnsf("")
	defer cols.Frender(os.Stdout)

	if nfo == nil {
		cols.SetHeading(fmt.Sprintf("Information for Key-Value Store Bucket %s", status.Bucket()))
	} else {
		cols.SetHeading(fmt.Sprintf("Information for Key-Value Store Bucket %s created %s", status.Bucket(), f(nfo.Created)))
	}

	cols.AddSectionTitle("Configuration")

	cols.AddRow("Bucket Name", status.Bucket())
	cols.AddRow("History Kept", status.History())
	cols.AddRow("Values Stored", status.Values())
	cols.AddRow("Compressed", status.IsCompressed())
	cols.AddRow("Per-Key TTL Supported", status.LimitMarkerTTL() > 0)
	cols.AddRowIf("Limit Marker TTL", status.LimitMarkerTTL(), status.LimitMarkerTTL() > 0)
	cols.AddRow("Backing Store Kind", status.BackingStore())

	if nfo != nil {
		cols.AddRowIfNotEmpty("Description", nfo.Config.Description)

		cols.AddRow("Bucket Size", humanize.IBytes(nfo.State.Bytes))
		if nfo.Config.MaxBytes == -1 {
			cols.AddRow("Maximum Bucket Size", "unlimited")
		} else {
			cols.AddRow("Maximum Bucket Size", humanize.IBytes(uint64(nfo.Config.MaxBytes)))
		}
		if nfo.Config.MaxMsgSize == -1 {
			cols.AddRow("Maximum Value Size", "unlimited")
		} else {
			cols.AddRow("Maximum Value Size", humanize.IBytes(uint64(nfo.Config.MaxMsgSize)))
		}
		if nfo.Config.MaxAge <= 0 {
			cols.AddRow("Maximum Age", "unlimited")
		} else {
			cols.AddRow("Maximum Age", nfo.Config.MaxAge)
		}
		cols.AddRow("JetStream Stream", nfo.Config.Name)
		cols.AddRow("Storage", nfo.Config.Storage.String())
		if nfo.Config.RePublish != nil {
			if nfo.Config.RePublish.HeadersOnly {
				cols.AddRowf("Republishing Headers", "%s to %s", nfo.Config.RePublish.Source, nfo.Config.RePublish.Destination)
			} else {
				cols.AddRowf("Republishing", "%s to %s", nfo.Config.RePublish.Source, nfo.Config.RePublish.Destination)
			}
		}

		if nfo.Mirror != nil {
			s := nfo.Config.Mirror
			cols.AddSectionTitle("Mirror Information")
			cols.AddRow("Origin Bucket", strings.TrimPrefix(s.Name, "KV_"))
			if s.External != nil {
				cols.AddRow("External API", s.External.APIPrefix)
			}

			if nfo.Mirror.Active > 0 && nfo.Mirror.Active < math.MaxInt64 {
				cols.AddRow("Last Seen", nfo.Mirror.Active)
			} else {
				cols.AddRowf("Last Seen", "never")
			}
			cols.AddRow("Lag", nfo.Mirror.Lag)
		}

		if len(nfo.Sources) > 0 {
			cols.AddSectionTitle("Sources Information")
			for _, source := range nfo.Sources {
				for _, s := range nfo.Config.Sources {
					if s.Name == source.Name {
						cols.AddRow("Source Bucket", strings.TrimPrefix(source.Name, "KV_"))
						if s.External != nil {
							cols.AddRow("External API", s.External.APIPrefix)
						}
					}
				}
				if source.Active > 0 && source.Active < math.MaxInt64 {
					cols.AddRow("Last Seen", source.Active)
				} else {
					cols.AddRow("Last Seen", "never")
				}
				cols.AddRow("Lag", source.Lag)
			}
		}

		meta := iu.RemoveReservedMetadata(status.Metadata())
		if len(meta) > 0 {
			cols.AddSectionTitle("Metadata")
			cols.AddMapStrings(meta)
		}

		if nfo.Cluster != nil {
			cols.AddSectionTitle("Cluster Information")
			renderNatsGoClusterInfo(cols, nfo)
		}
	}

	return nil
}

func renderNatsGoClusterInfo(cols *columns.Writer, info *jetstream.StreamInfo) {
	cols.AddRow("Name", info.Cluster.Name)
	cols.AddRow("Leader", info.Cluster.Leader)
	for _, r := range info.Cluster.Replicas {
		state := []string{r.Name}

		if r.Current {
			state = append(state, "current")
		} else {
			state = append(state, "outdated")
		}

		if r.Offline {
			state = append(state, "OFFLINE")
		}

		if r.Active > 0 && r.Active < math.MaxInt64 {
			state = append(state, fmt.Sprintf("seen %s ago", f(r.Active)))
		} else {
			state = append(state, "not seen")
		}

		switch {
		case r.Lag > 1:
			state = append(state, fmt.Sprintf("%s operations behind", f(r.Lag)))
		case r.Lag == 1:
			state = append(state, fmt.Sprintf("%d operation behind", r.Lag))
		}

		cols.AddRow("Replica", state)
	}
}
