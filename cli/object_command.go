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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/spf13/cobra"
)

type objCommand struct {
	bucket              string
	file                string
	overrideName        string
	hdrs                []string
	force               bool
	progress            bool
	storage             string
	listNames           bool
	placementCluster    string
	placementTags       []string
	maxBucketSize       int64
	maxBucketSizeString string
	metadata            map[string]string
	chunkSize           uint32

	description string
	replicas    uint
	ttl         time.Duration
	compression bool

	ttlIsSetByUser           bool
	replicasIsSetByUser      bool
	maxBucketSizeIsSetByUser bool
	descriptionIsSetByUser   bool
	tagsIsSetByUser          bool
	clusterIsSetByUser       bool
	metadataIsSetByUser      bool
	compressionIsSetByUser   bool
	chunkSizeIsSetByUser     bool
}

func configureObjectCommand(app commandHost) {
	c := &objCommand{
		metadata: map[string]string{},
	}

	help := `Interacts with a JetStream based Object store

The JetStream Object store uses streams to store large objects
for an indefinite period or a per-bucket configured TTL.

`

	obj := addCommand(app, "object", help)
	obj.Aliases = []string{"obj"}
	addCheat("object", obj)

	addCreateFlags := func(f *cobra.Command, edit bool) {
		f.Flags().DurationVar(&c.ttl, "ttl", 0, "How long to keep objects for")
		f.Flags().UintVar(&c.replicas, "replicas", 1, "How many replicas of the data to store")
		f.Flags().StringVar(&c.maxBucketSizeString, "max-bucket-size", "", "Maximum size for the bucket")
		f.Flags().StringVar(&c.description, "description", "", "A description for the bucket")
		if !edit {
			f.Flags().Var(newEnumValue(&c.storage, "", "file", "f", "memory", "m"), "storage", "Storage backend to use (file, memory)")
		}
		f.Flags().StringArrayVar(&c.placementTags, "tags", nil, "Place the store on servers that has specific tags")
		f.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the store on a specific cluster")
		f.Flags().Var(newStringMapValue(&c.metadata), "metadata", "Adds metadata to the bucket")
		flagPlaceholder(f, "metadata", "META")
		negatableBoolVar(f, &c.compression, "compress", false, "Compress the bucket data")
	}

	add := addCommand(obj, "add", "Adds a new Object Store Bucket")
	add.RunE = c.addAction
	cmdAddTags(add, "scope:user", "impact:rw")
	addArg(add, "bucket", "The bucket to act on", true, "string")
	addCreateFlags(add, false)
	add.PreRunE = c.parseLimitStrings

	edit := addCommand(obj, "edit", "Edit an existing Object Store Bucket")
	edit.RunE = c.editAction
	cmdAddTags(edit, "scope:user", "impact:rw")
	addArg(edit, "bucket", "The bucket to act on", true, "string")
	addCreateFlags(edit, true)
	edit.PreRunE = c.parseLimitStrings

	put := addCommand(obj, "put", "Puts a file into the store")
	put.RunE = c.putAction
	cmdAddTags(put, "scope:user", "impact:rw")
	addArg(put, "bucket", "The bucket to act on", true, "string")
	addArg(put, "file", "The file to put", false, "string")
	put.Flags().StringVar(&c.overrideName, "name", "", "Override the name supplied to the object store")
	put.Flags().StringVar(&c.description, "description", "", "Sets an optional description for the object")
	put.Flags().StringArrayVarP(&c.hdrs, "header", "H", nil, "Adds headers to the object using K:V format")
	put.Flags().Uint32Var(&c.chunkSize, "chunk-size", 0, "Sets the chunk size for the file")
	negatableBoolVar(put, &c.progress, "progress", true, "Disable progress bars")
	put.Flags().BoolVarP(&c.force, "force", "f", false, "Act without confirmation")

	del := addCommand(obj, "del", "Deletes a file or bucket from the store")
	del.Aliases = []string{"rm"}
	del.RunE = c.delAction
	cmdAddTags(del, "scope:user", "impact:rw")
	addArg(del, "bucket", "The bucket to act on", true, "string")
	addArg(del, "file", "The file to retrieve", false, "string")
	del.Flags().BoolVarP(&c.force, "force", "f", false, "Act without confirmation")

	get := addCommand(obj, "get", "Retrieves a file from the store")
	get.RunE = c.getAction
	cmdAddTags(get, "scope:user", "impact:ro")
	addArg(get, "bucket", "The bucket to act on", true, "string")
	addArg(get, "file", "The file to retrieve", true, "string")
	get.Flags().StringVarP(&c.overrideName, "output", "O", "", "Override the output file name")
	negatableBoolVar(get, &c.progress, "progress", true, "Disable progress bars")
	get.Flags().BoolVarP(&c.force, "force", "f", false, "Act without confirmation")

	info := addCommand(obj, "info", "Get information about a bucket or object")
	info.Aliases = []string{"show", "i"}
	info.RunE = c.infoAction
	cmdAddTags(info, "scope:user", "impact:ro")
	addArg(info, "bucket", "The bucket to act on", false, "string")
	addArg(info, "file", "The file to retrieve", false, "string")

	ls := addCommand(obj, "ls", "List buckets or contents of a specific bucket")
	ls.RunE = c.lsAction
	cmdAddTags(ls, "scope:user", "impact:ro")
	addArg(ls, "bucket", "The bucket to act on", false, "string")
	ls.Flags().BoolVarP(&c.listNames, "names", "n", false, "When listing buckets, show just the bucket names")

	seal := addCommand(obj, "seal", "Seals a bucket preventing further updates")
	seal.RunE = c.sealAction
	cmdAddTags(seal, "scope:user", "impact:rw")
	addArg(seal, "bucket", "The bucket to act on", true, "string")
	seal.Flags().BoolVarP(&c.force, "force", "f", false, "Force sealing without prompting")

	watch := addCommand(obj, "watch", "Watch a bucket for changes")
	watch.RunE = c.watchAction
	cmdAddTags(watch, "scope:user", "impact:ro")
	addArg(watch, "bucket", "The bucket to act on", true, "string")
}

func init() {
	registerCommand("object", 10, configureObjectCommand)
}

func (c *objCommand) parseLimitStrings(_ *cobra.Command, _ []string) (err error) {
	if c.maxBucketSizeString != "" {
		c.maxBucketSize, err = iu.ParseStringAsBytes(c.maxBucketSizeString, 64)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *objCommand) watchAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	w, err := obj.Watch(ctx, jetstream.IncludeHistory())
	if err != nil {
		return err
	}
	defer w.Stop()

	for i := range w.Updates() {
		if i == nil {
			continue
		}

		if i.Deleted {
			fmt.Printf("[%s] %s %s > %s\n", f(i.ModTime), color.RedString("DEL"), i.Bucket, i.Name)
		} else {
			fmt.Printf("[%s] %s %s > %s: %s bytes in %s chunks\n", f(i.ModTime), color.GreenString("PUT"), i.Bucket, i.Name, humanize.IBytes(i.Size), f(i.Chunks))
		}
	}

	return nil
}

func (c *objCommand) sealAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really seal Bucket %s, sealed buckets can not be unsealed or modified", c.bucket), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	err = obj.Seal(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("%s has been sealed\n", c.bucket)

	return c.showBucketInfo(obj)
}

func (c *objCommand) delAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.file = argValue(args, 1)

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	if c.file != "" {
		if !c.force {

			nfo, err := obj.GetInfo(ctx, c.file)
			if err != nil {
				return err
			}

			ok, err := askConfirmation(fmt.Sprintf("Delete %s byte file %s > %s?", humanize.IBytes(nfo.Size), c.bucket, c.file), false)
			if err != nil {
				return err
			}

			if !ok {
				fmt.Println("Skipping delete")
				return nil
			}
		}
		err = obj.Delete(ctx, c.file)
		if err != nil {
			return err
		}

		fmt.Printf("Removed %s > %s\n", c.bucket, c.file)

		return c.showBucketInfo(obj)

	} else {
		if !c.force {
			ok, err := askConfirmation(fmt.Sprintf("Delete bucket %s and all its files?", c.bucket), false)
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

		return js.DeleteObjectStore(ctx, c.bucket)
	}
}

func (c *objCommand) infoAction(_ *cobra.Command, args []string) error {
	c.bucket = argValue(args, 0)
	c.file = argValue(args, 1)

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	if c.file == "" {
		return c.showBucketInfo(obj)
	}

	nfo, err := obj.GetInfo(ctx, c.file)
	if err != nil {
		return err
	}

	c.showObjectInfo(nfo)

	return nil
}

func (c *objCommand) showBucketInfo(store jetstream.ObjectStore) error {
	status, err := store.Status(ctx)
	if err != nil {
		return err
	}

	var nfo *jetstream.StreamInfo
	if status.BackingStore() == "JetStream" {
		nfo = status.(*jetstream.ObjectBucketStatus).StreamInfo()
	}

	cols := newColumnsf("")
	defer cols.Frender(os.Stdout)

	if nfo == nil {
		cols.SetHeading(fmt.Sprintf("Information for Object Store Bucket %s", status.Bucket()))
	} else {
		cols.SetHeading(fmt.Sprintf("Information for Object Store Bucket %s created %s", status.Bucket(), f(nfo.Created)))
	}

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Bucket Name", status.Bucket())
	cols.AddRowIfNotEmpty("Description", status.Description())
	cols.AddRow("Replicas", status.Replicas())
	if status.TTL() == 0 {
		cols.AddRow("TTL", "unlimited")
	} else {
		cols.AddRow("TTL", status.TTL())
	}
	cols.AddRow("Sealed", status.Sealed())
	cols.AddRow("Size", humanize.IBytes(status.Size()))
	if nfo != nil {
		if nfo.Config.MaxBytes == -1 {
			cols.AddRow("Maximum Bucket Size", "unlimited")
		} else {
			cols.AddRow("Maximum Bucket Size", humanize.IBytes(uint64(nfo.Config.MaxBytes)))
		}
	}
	cols.AddRow("Storage", status.Storage())
	cols.AddRow("Backing Store Kind", status.BackingStore())
	if status.BackingStore() == "JetStream" {
		cols.AddRow("JetStream Stream", nfo.Config.Name)

		meta := jsm.FilterServerMetadata(nfo.Config.Metadata)
		if len(meta) > 0 {
			cols.AddMapStringsAsValue("Metadata", meta)
		}

		if nfo.Cluster != nil {
			cols.AddSectionTitle("Cluster Information")
			renderNatsGoClusterInfo(cols, nfo)
		}
	}

	return nil
}

func (c *objCommand) showObjectInfo(nfo *jetstream.ObjectInfo) {
	digest := strings.SplitN(nfo.Digest, "=", 2)
	digestBytes, _ := base64.URLEncoding.DecodeString(digest[1])

	cols := newColumnsf("Object information for %s > %s", nfo.Bucket, nfo.Name)
	defer cols.Frender(os.Stdout)

	if nfo.Description != "" {
		cols.AddRowIfNotEmpty("Description", nfo.Description)
	}
	cols.AddRow("Size", fiBytes(nfo.Size))
	cols.AddRow("Modification Time", nfo.ModTime)
	cols.AddRow("Chunks", nfo.Chunks)
	cols.AddRowf("Digest", "%s %x", digest[0], digestBytes)
	cols.AddRowIf("Deleted", nfo.Deleted, nfo.Deleted)

	if len(nfo.Headers) > 0 {
		var vals []string
		for k, v := range nfo.Headers {
			for _, i := range v {
				vals = append(vals, fmt.Sprintf("%s: %s", k, i))
			}
		}
		cols.AddStringsAsValue("Headers", vals)
	}

	if len(nfo.Metadata) > 0 {
		cols.AddSectionTitle("Metadata")
		cols.AddMapStrings(nfo.Metadata)
	}
}

func (c *objCommand) listBuckets() error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	var found []*jsm.Stream
	_, _, err = mgr.EachStream(nil, func(s *jsm.Stream) {
		if s.IsObjectBucket() {
			found = append(found, s)
		}
	})
	if err != nil {
		return err
	}

	if len(found) == 0 {
		if !c.listNames {
			fmt.Println("No Object Store buckets found")
		}

		return nil
	}

	if c.listNames {
		for _, s := range found {
			fmt.Println(strings.TrimPrefix(s.Name(), "OBJ_"))
		}
		return nil
	}

	sort.Slice(found, func(i, j int) bool {
		info, _ := found[i].LatestInformation()
		jnfo, _ := found[j].LatestInformation()

		return info.State.Bytes < jnfo.State.Bytes
	})

	table := iu.NewTableWriterf(opts(), "Object Store Buckets")
	table.AddHeaders("Bucket", "Description", "Created", "Size", "Last Update")
	for _, s := range found {
		nfo, _ := s.LatestInformation()

		table.AddRow(strings.TrimPrefix(s.Name(), "OBJ_"), s.Description(), f(nfo.Created), humanize.IBytes(nfo.State.Bytes), f(time.Since(nfo.State.LastTime)))
	}

	fmt.Println(table.Render())

	return nil
}

func (c *objCommand) lsAction(_ *cobra.Command, args []string) error {
	c.bucket = argValue(args, 0)

	if c.bucket == "" {
		return c.listBuckets()
	}

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	contents, err := obj.List(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
		return err
	}

	if c.listNames {
		for _, s := range contents {
			fmt.Println(s.Name)
		}
		return nil
	}

	if len(contents) == 0 {
		fmt.Println("No entries found")

		return nil
	}

	table := iu.NewTableWriterf(opts(), "Bucket Contents")
	table.AddHeaders("Name", "Size", "Time")

	for _, i := range contents {
		table.AddRow(i.Name, humanize.IBytes(i.Size), f(i.ModTime))
	}

	fmt.Println(table.Render())

	return nil
}

func (c *objCommand) putAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.file = argValue(args, 1)
	c.chunkSizeIsSetByUser = cmd.Flags().Changed("chunk-size")

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	name := c.file
	if c.overrideName != "" {
		name = c.overrideName
	}

	if c.file == "" && name == "" {
		return fmt.Errorf("--name is required when reading from stdin")
	}

	nfo, err := obj.GetInfo(ctx, name)
	if err == nil && !nfo.Deleted && !c.force {
		c.showObjectInfo(nfo)
		fmt.Println()
		ok, err := askConfirmation(fmt.Sprintf("Replace existing file %s > %s", c.bucket, name), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
		fmt.Println()
	}

	hdr, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return err
	}

	var (
		pr   io.Reader
		stat os.FileInfo
	)

	if c.file == "" {
		pr = os.Stdin
	} else {
		f, err := os.Open(c.file)
		if err != nil {
			return err
		}
		defer f.Close()

		stat, err = f.Stat()
		if err != nil {
			return err
		}

		pr = f
	}

	meta := jetstream.ObjectMeta{
		Name:        filepath.Clean(name),
		Description: c.description,
		Headers:     hdr,
	}

	if c.chunkSizeIsSetByUser {
		meta.Opts = &jetstream.ObjectMetaOptions{
			ChunkSize: c.chunkSize,
		}
	}

	var progbar progress.Writer
	var tracker *progress.Tracker

	stop := func() {}

	if !opts().Trace && c.progress && stat != nil && stat.Size() > 20480 {
		progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
			Total: stat.Size(),
			Units: iu.ProgressUnitsIBytes,
		})
		if err != nil {
			return err
		}

		stop = func() {
			time.Sleep(300 * time.Millisecond)
			progbar.Stop()
			fmt.Println()
		}
		pr = &progressRW{p: progbar, t: tracker, r: pr}
	}

	nfo, err = obj.Put(context.TODO(), meta, pr)
	stop()
	if err != nil {
		return err
	}

	fmt.Println()
	c.showObjectInfo(nfo)

	return nil
}

func (c *objCommand) getAction(_ *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.file = args[1]

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	getCtx := ctx
	if opts().Timeout < time.Hour {
		var cancel context.CancelFunc
		getCtx, cancel = context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
	}

	res, err := obj.Get(getCtx, c.file)
	if err != nil {
		return err
	}

	nfo, err := res.Info()
	if err != nil {
		return err
	}

	if nfo.Deleted {
		return fmt.Errorf("file has been deleted")
	}

	out := filepath.Base(nfo.Name)
	if c.overrideName != "" {
		out = c.overrideName
	}

	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}

	if !c.force {
		_, err = os.Stat(out)
		if !os.IsNotExist(err) {
			ok, err := askConfirmation(fmt.Sprintf("Replace existing target file %s", out), false)
			fatalIfError(err, "could not obtain confirmation")

			if !ok {
				return nil
			}
		}
	}

	of, err := os.Create(out)
	if err != nil {
		return err
	}
	defer of.Close()

	var progbar progress.Writer
	var tracker *progress.Tracker

	pw := io.Writer(of)
	stop := func() {}

	if !opts().Trace && c.progress && nfo.Size > 20480 {
		fmt.Println()
		progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
			Total: int64(nfo.Size),
			Units: iu.ProgressUnitsIBytes,
		})
		if err != nil {
			return err
		}
		stop = func() {
			time.Sleep(300 * time.Millisecond)
			progbar.Stop()
			fmt.Println()
		}

		pw = &progressRW{p: progbar, t: tracker, w: of}
	}

	start := time.Now()
	wc, err := io.Copy(pw, res)
	stop()
	if err != nil {
		of.Close()
		os.Remove(of.Name())
		return err
	}

	if wc > 0 && uint64(wc) != nfo.Size {
		return fmt.Errorf("wrote %s, expected %s", humanize.IBytes(uint64(wc)), humanize.IBytes(nfo.Size))
	}

	of.Close()

	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		bps := float64(nfo.Size) / elapsed.Seconds()
		fmt.Printf("Wrote: %s to %s in %v average %s/s\n", humanize.IBytes(uint64(wc)), of.Name(), f(elapsed), humanize.IBytes(uint64(bps)))
	} else {
		fmt.Printf("Wrote: %s to %s in %v\n", humanize.IBytes(uint64(wc)), of.Name(), f(elapsed))
	}

	return nil
}

func (c *objCommand) addAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.ttlIsSetByUser = cmd.Flags().Changed("ttl")
	c.replicasIsSetByUser = cmd.Flags().Changed("replicas")
	c.maxBucketSizeIsSetByUser = cmd.Flags().Changed("max-bucket-size")
	c.descriptionIsSetByUser = cmd.Flags().Changed("description")
	c.tagsIsSetByUser = cmd.Flags().Changed("tags")
	c.clusterIsSetByUser = cmd.Flags().Changed("cluster")
	c.metadataIsSetByUser = cmd.Flags().Changed("metadata")
	c.compressionIsSetByUser = cmd.Flags().Changed("compress")

	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	st := jetstream.FileStorage
	if c.storage == "memory" || c.storage == "m" {
		st = jetstream.MemoryStorage
	}

	placement := &jetstream.Placement{Cluster: c.placementCluster}
	if len(c.placementTags) > 0 {
		placement.Tags = c.placementTags
	}

	obj, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      c.bucket,
		Description: c.description,
		TTL:         c.ttl,
		Storage:     st,
		Replicas:    int(c.replicas),
		Placement:   placement,
		MaxBytes:    c.maxBucketSize,
		Metadata:    c.metadata,
		Compression: c.compression,
	})
	if err != nil {
		return err
	}

	return c.showBucketInfo(obj)
}

func (c *objCommand) editAction(cmd *cobra.Command, args []string) error {
	c.bucket = args[0]
	c.ttlIsSetByUser = cmd.Flags().Changed("ttl")
	c.replicasIsSetByUser = cmd.Flags().Changed("replicas")
	c.maxBucketSizeIsSetByUser = cmd.Flags().Changed("max-bucket-size")
	c.descriptionIsSetByUser = cmd.Flags().Changed("description")
	c.tagsIsSetByUser = cmd.Flags().Changed("tags")
	c.clusterIsSetByUser = cmd.Flags().Changed("cluster")
	c.metadataIsSetByUser = cmd.Flags().Changed("metadata")
	c.compressionIsSetByUser = cmd.Flags().Changed("compress")

	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	os, err := js.ObjectStore(ctx, c.bucket)
	if err != nil {
		return err
	}

	status, err := os.Status(ctx)
	if err != nil {
		return err
	}
	var nfo *jetstream.StreamInfo
	if status.BackingStore() == "JetStream" {
		nfo = status.(*jetstream.ObjectBucketStatus).StreamInfo()
	} else {
		return errors.New(c.bucket + " is not a JetStream bucket")
	}

	update := jetstream.ObjectStoreConfig{
		Bucket:      c.bucket,
		Description: status.Description(),
		TTL:         status.TTL(),
		Storage:     status.Storage(),
		Replicas:    status.Replicas(),
		Placement:   nfo.Config.Placement,
		MaxBytes:    nfo.Config.MaxBytes,
		Metadata:    status.Metadata(),
		Compression: status.IsCompressed(),
	}

	if c.descriptionIsSetByUser {
		update.Description = c.description
	}
	if c.ttlIsSetByUser {
		update.TTL = c.ttl
	}
	if c.replicasIsSetByUser {
		update.Replicas = int(c.replicas)
	}
	if c.clusterIsSetByUser || c.tagsIsSetByUser {
		if update.Placement == nil {
			update.Placement = &jetstream.Placement{}
		}
		if c.clusterIsSetByUser {
			update.Placement.Cluster = c.placementCluster
		}
		if c.tagsIsSetByUser {
			update.Placement.Tags = c.placementTags
		}
	}
	if c.maxBucketSizeIsSetByUser {
		update.MaxBytes = c.maxBucketSize
	}
	if c.metadataIsSetByUser {
		update.Metadata = c.metadata
	}
	if c.compressionIsSetByUser {
		update.Compression = c.compression
	}

	obj, err := js.UpdateObjectStore(ctx, update)
	if err != nil {
		return err
	}

	return c.showBucketInfo(obj)
}

func (c *objCommand) loadBucket() (*nats.Conn, jetstream.JetStream, jetstream.ObjectStore, error) {
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
			return nil, nil, nil, fmt.Errorf("no Object buckets found")
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

	store, err := js.ObjectStore(ctx, c.bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	return nc, js, store, err
}

func (c *objCommand) knownBuckets(nc *nats.Conn) ([]string, error) {
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
		if jsm.IsObjectBucketStream(stream) {
			found = append(found, strings.TrimPrefix(stream, "OBJ_"))
		}
	}

	return found, nil
}
