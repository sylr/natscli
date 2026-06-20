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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"

	"github.com/nats-io/natscli/internal/asciigraph"
	iu "github.com/nats-io/natscli/internal/util"
	terminal "golang.org/x/term"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/emicklei/dot"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/balancer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/columns"
	"gopkg.in/yaml.v3"

	"github.com/spf13/cobra"
)

type streamCmd struct {
	stream           string
	force            bool
	json             bool
	msgID            int64
	retentionPolicyS string
	inputFile        string
	outFile          string
	filterSubject    string
	showAll          bool
	acceptDefaults   bool

	destination            string
	subjects               []string
	ack                    bool
	storage                string
	maxMsgLimit            int64
	maxMsgPerSubjectLimit  int64
	maxBytesLimitString    string
	maxBytesLimit          int64
	maxAgeLimit            string
	maxMsgSizeString       string
	maxMsgSize             int64
	maxConsumers           int
	reportSortConsumers    bool
	reportSortMsgs         bool
	reportSortName         bool
	reportSortReverse      bool
	reportSortStorage      bool
	reportSort             string
	reportRaw              bool
	reportLimitCluster     string
	reportLeaderDistrib    bool
	discardPolicy          string
	validateOnly           bool
	backupDirectory        string
	showProgress           bool
	healthCheck            bool
	snapShotConsumers      bool
	dupeWindow             string
	replicas               int64
	placementCluster       string
	placementTags          []string
	placementClusterSet    bool
	placementTagsSet       bool
	peerName               string
	sources                []string
	mirror                 string
	interactive            bool
	purgeKeep              uint64
	purgeSubject           string
	purgeSequence          uint64
	description            string
	subjectTransformSource string
	subjectTransformDest   string
	noSubjectTransform     bool
	noMirror               bool
	repubSource            string
	repubDest              string
	repubHeadersOnly       bool
	noRepub                bool
	allowRollup            bool
	allowRollupSet         bool
	denyDelete             bool
	denyDeleteSet          bool
	denyPurge              bool
	denyPurgeSet           bool
	allowDirect            bool
	allowDirectSet         bool
	allowMirrorDirect      bool
	allowMirrorDirectSet   bool
	allowSchedules         bool
	allowSchedulesSet      bool
	discardPerSubj         bool
	discardPerSubjSet      bool
	showStateOnly          bool
	metadata               map[string]string
	metadataIsSet          bool
	compression            string
	compressionSet         bool
	firstSeq               uint64
	limitInactiveThreshold time.Duration
	limitMaxAckPending     int
	persistMode            string

	fServer      string
	fCluster     string
	fEmpty       bool
	fIdle        time.Duration
	fCreated     time.Duration
	fConsumers   int
	fInvert      bool
	fReplicas    uint
	fSourced     bool
	fSourcedSet  bool
	fMirrored    bool
	fMirroredSet bool
	fExpression  string
	fLeader      string

	listNames    bool
	vwStartId    int
	vwStartDelta time.Duration
	vwPageSize   int
	vwRaw        bool
	vwTranslate  string
	vwSubject    string

	dryRun                    bool
	selectedStream            *jsm.Stream
	nc                        *nats.Conn
	mgr                       *jsm.Manager
	chunkSize                 string
	wndSize                   string
	placementPreferred        string
	allowMsgTTlSet            bool
	allowMsgTTL               bool
	allowAtomicBatch          bool
	allowAtomicBatchIsSet     bool
	allowCounter              bool
	allowCounterIsSet         bool
	allowFastBatch            bool
	allowFastBatchIsSet       bool
	subjectDeleteMarkerTTLSet bool
	subjectDeleteMarkerTTL    time.Duration
	apiLevel                  int
}

type streamStat struct {
	Name      string
	Consumers int
	Msgs      int64
	Bytes     uint64
	Storage   string
	Cluster   *api.ClusterInfo
	LostBytes uint64
	LostMsgs  int
	Deleted   int
	Mirror    *api.StreamSourceInfo
	Sources   []*api.StreamSourceInfo
	Placement *api.Placement
	APILevel  string
}

func configureStreamCommand(app commandHost) {
	c := &streamCmd{msgID: -1, metadata: map[string]string{}}

	addCreateFlags := func(f *cobra.Command, edit bool) {
		f.Flags().StringArrayVar(&c.subjects, "subjects", nil, "Subjects that are consumed by the stream")
		f.Flags().StringVar(&c.description, "description", "", "Sets a contextual description for the stream")
		if !edit {
			f.Flags().Var(newEnumValue(&c.storage, "", "file", "f", "memory", "m"), "storage", "Storage backend to use (file, memory)")
		}
		f.Flags().Var(newEnumValue(&c.compression, "", "none", "s2"), "compression", "Compression algorithm to use (file storage only)")
		f.Flags().Int64Var(&c.replicas, "replicas", 0, "When clustered, how many replicas of the data to create")
		f.Flags().StringArrayVar(&c.placementTags, "tag", nil, "Place the stream on servers that has specific tags (pass multiple times)")
		f.Flags().StringArrayVar(&c.placementTags, "tags", nil, "Backward compatibility only, use --tag")
		_ = f.Flags().MarkHidden("tags")
		f.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the stream on a specific cluster")
		f.Flags().BoolVar(&c.ack, "ack", true, "Acknowledge publishes")
		f.Flags().Var(newEnumValue(&c.retentionPolicyS, "", "limits", "interest", "workq", "work"), "retention", "Defines a retention policy (limits, interest, work)")
		f.Flags().Var(newEnumValue(&c.discardPolicy, "", "new", "old"), "discard", "Defines the discard policy (new, old)")
		negatableBoolVar(f, &c.discardPerSubj, "discard-per-subject", false, "Sets the 'new' discard policy and applies it to every subject in the stream")
		if !edit {
			f.Flags().Uint64Var(&c.firstSeq, "first-sequence", 0, "Sets the starting sequence")
		}
		f.Flags().StringVar(&c.maxAgeLimit, "max-age", "", "Maximum age of messages to keep")
		f.Flags().StringVar(&c.maxBytesLimitString, "max-bytes", "", "Maximum bytes to keep")
		flagPlaceholder(f, "max-bytes", "BYTES")
		f.Flags().IntVar(&c.maxConsumers, "max-consumers", -1, "Maximum number of consumers to allow")
		f.Flags().StringVar(&c.maxMsgSizeString, "max-msg-size", "", "Maximum size any 1 message may be")
		flagPlaceholder(f, "max-msg-size", "BYTES")
		f.Flags().Int64Var(&c.maxMsgLimit, "max-msgs", 0, "Maximum amount of messages to keep")
		f.Flags().Int64Var(&c.maxMsgPerSubjectLimit, "max-msgs-per-subject", 0, "Maximum amount of messages to keep per subject")
		f.Flags().StringVar(&c.dupeWindow, "dupe-window", "", "Duration of the duplicate message tracking window")
		f.Flags().StringVar(&c.mirror, "mirror", "", "Completely mirror another stream")
		if edit {
			f.Flags().BoolVar(&c.noMirror, "no-mirror", false, "Removes current mirror configuration")
		}
		f.Flags().StringArrayVar(&c.sources, "source", nil, "Source data from other streams, merging into this one")
		flagPlaceholder(f, "source", "STREAM")
		negatableBoolVar(f, &c.allowAtomicBatch, "allow-batch", false, "Allow atomic batch publishing")
		negatableBoolVar(f, &c.allowFastBatch, "allow-fast", false, "Allow fast batch publishing")
		f.Flags().BoolVar(&c.allowCounter, "allow-counter", false, "Configures the stream as a distributed counter")
		negatableBoolVar(f, &c.allowRollup, "allow-rollup", false, "Allows roll-ups to be done by publishing messages with special headers")
		negatableBoolVar(f, &c.denyDelete, "deny-delete", false, "Deny messages from being deleted via the API")
		negatableBoolVar(f, &c.denyPurge, "deny-purge", false, "Deny entire stream or subject purges via the API")
		negatableBoolVar(f, &c.allowDirect, "allow-direct", true, "Allows fast, direct, access to stream data via the direct get API")
		negatableBoolVar(f, &c.allowMirrorDirect, "allow-mirror-direct", false, "Allows fast, direct, access to stream data via the direct get API on mirrors")
		f.Flags().BoolVar(&c.allowMsgTTL, "allow-msg-ttl", false, "Allows per-message TTL handling")
		negatableBoolVar(f, &c.allowSchedules, "allow-schedules", false, "Allows message schedules")
		f.Flags().DurationVar(&c.subjectDeleteMarkerTTL, "subject-del-markers-ttl", 0, "How long delete markers should persist in the stream")
		flagPlaceholder(f, "subject-del-markers-ttl", "DURATION")
		f.Flags().StringVar(&c.subjectTransformSource, "transform-source", "", "Stream subject transform source")
		flagPlaceholder(f, "transform-source", "SOURCE")
		f.Flags().StringVar(&c.subjectTransformDest, "transform-destination", "", "Stream subject transform destination")
		flagPlaceholder(f, "transform-destination", "DEST")
		if edit {
			f.Flags().BoolVar(&c.noSubjectTransform, "no-transform", false, "Removes current subject transform configuration")
		}
		f.Flags().Var(newStringMapValue(&c.metadata), "metadata", "Adds metadata to the stream")
		flagPlaceholder(f, "metadata", "META")
		f.Flags().StringVar(&c.repubSource, "republish-source", "", "Republish messages to --republish-destination")
		flagPlaceholder(f, "republish-source", "SOURCE")
		f.Flags().StringVar(&c.repubDest, "republish-destination", "", "Republish destination for messages in --republish-source")
		flagPlaceholder(f, "republish-destination", "DEST")
		f.Flags().BoolVar(&c.repubHeadersOnly, "republish-headers", false, "Republish only message headers, no bodies")
		if edit {
			f.Flags().BoolVar(&c.noRepub, "no-republish", false, "Removes current republish configuration")
		}
		if !edit {
			f.Flags().DurationVar(&c.limitInactiveThreshold, "limit-consumer-inactive", 0, "The maximum Consumer inactive threshold the stream allows")
			flagPlaceholder(f, "limit-consumer-inactive", "THRESHOLD")
			f.Flags().IntVar(&c.limitMaxAckPending, "limit-consumer-max-pending", 0, "The maximum Consumer Ack Pending the stream Allows")
			flagPlaceholder(f, "limit-consumer-max-pending", "PENDING")
			f.Flags().Var(newEnumValue(&c.persistMode, "", "default", "async"), "persist-mode", "Configures the persistence mode")
		}
		f.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

		f.PreRunE = c.parseLimitStrings
	}

	str := addCommand(app, "stream", "JetStream stream management")
	str.Aliases = []string{"str", "st", "ms", "s"}
	negatableBoolVarP(str, &c.showAll, "all", "a", false, "When listing or selecting streams show all streams including system ones")
	addCheat("stream", str)

	strAdd := addCommand(str, "add", "Create a new stream")
	strAdd.Aliases = []string{"create", "new"}
	strAdd.RunE = c.addAction
	cmdAddTags(strAdd, "scope:user", "impact:rw")
	addArg(strAdd, "stream", "Stream name", false, "string")
	strAdd.Flags().Var(newExistingFileValue(&c.inputFile), "config", "JSON file to read configuration from")
	strAdd.Flags().BoolVar(&c.validateOnly, "validate", false, "Only validates the configuration against the official Schema")
	strAdd.Flags().StringVar(&c.outFile, "output", "", "Save configuration instead of creating")
	flagPlaceholder(strAdd, "output", "FILE")
	addCreateFlags(strAdd, false)
	strAdd.Flags().BoolVar(&c.acceptDefaults, "defaults", false, "Accept default values for all prompts")

	strLs := addCommand(str, "ls", "List all known streams")
	strLs.Aliases = []string{"list", "l"}
	strLs.RunE = c.lsAction
	cmdAddTags(strLs, "scope:user", "impact:ro")
	strLs.Flags().StringVar(&c.filterSubject, "subject", "", "Limit the list to streams with matching subjects")
	negatableBoolVarP(strLs, &c.listNames, "names", "n", false, "Show just the stream names")
	strLs.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	strReport := addCommand(str, "report", "Reports on stream statistics")
	strReport.RunE = c.reportAction
	cmdAddTags(strReport, "scope:user", "impact:ro")
	strReport.Flags().StringVar(&c.filterSubject, "subject", "", "Limit the report to streams with matching subjects")
	strReport.Flags().StringVar(&c.reportLimitCluster, "cluster", "", "Limit report to streams within a specific cluster")
	negatableBoolVarP(strReport, &c.reportSortConsumers, "consumers", "o", false, "Sort by number of Consumers")
	negatableBoolVarP(strReport, &c.reportSortMsgs, "messages", "m", false, "Sort by number of Messages")
	negatableBoolVarP(strReport, &c.reportSortName, "name", "n", false, "Sort by stream name")
	negatableBoolVarP(strReport, &c.reportSortStorage, "storage", "t", false, "Sort by Storage type")
	negatableBoolVarP(strReport, &c.reportRaw, "raw", "r", false, "Show un-formatted numbers")
	strReport.Flags().StringVar(&c.outFile, "dot", "", "Produce a GraphViz graph of replication topology")
	negatableBoolVarP(strReport, &c.reportLeaderDistrib, "leaders", "l", false, "Show details about cluster leaders")

	findHelp := `Expression format:

Using the --expression flag arbitrary complex matching can be done across any state field(s) from the stream information.

Use this when trying to match on fields we don't specifically support or to perform complex boolean matches.

We use the expr language to perform matching, see https://expr.medv.io/docs/Language-Definition for detail about the expression language.  Expressions you enter must return a boolean value.

The following items are available to query, all using the same values seen in JSON from Stream info:

  * config - Stream configuration
  * state - Stream state
  * info - Stream information aka state.state

Additionally there is Info (with a capital I) that is the golang structure and with keys matching the struct keys in the server.

Finding streams with more than 10 messages:

   nats s find --expression 'state.messages < 10'
   nats s find --expression 'Info.State.Msgs < 10'

Finding streams in multiple clusters:

   nats s find --expression 'info.cluster.name in ["lon", "sfo"]'

Finding streams with certain subjects configured:

   nats s find --expression '"js.in.orders_1" in config.subjects'
`
	strFind := addCommand(str, "find", "Finds streams matching certain criteria")
	strFind.Aliases = []string{"query"}
	strFind.RunE = c.findAction
	cmdAddTags(strFind, "scope:user", "impact:ro")
	strFind.Long = findHelp
	strFind.Flags().StringVar(&c.fServer, "server-name", "", "Display streams present on a regular expression matched server")
	strFind.Flags().StringVar(&c.fCluster, "cluster", "", "Display streams present on a regular expression matched cluster")
	strFind.Flags().BoolVar(&c.fEmpty, "empty", false, "Display streams with no messages")
	strFind.Flags().DurationVar(&c.fIdle, "idle", 0, "Display streams with no new messages or consumer deliveries for a period")
	flagPlaceholder(strFind, "idle", "DURATION")
	strFind.Flags().DurationVar(&c.fCreated, "created", 0, "Display streams created longer ago than duration")
	flagPlaceholder(strFind, "created", "DURATION")
	strFind.Flags().IntVar(&c.fConsumers, "consumers", -1, "Display streams with fewer consumers than threshold")
	flagPlaceholder(strFind, "consumers", "THRESHOLD")
	strFind.Flags().StringVar(&c.filterSubject, "subject", "", "Filters streams by those with interest matching a subject or wildcard")
	strFind.Flags().UintVar(&c.fReplicas, "replicas", 0, "Display streams with fewer or equal replicas than the value")
	flagPlaceholder(strFind, "replicas", "REPLICAS")
	strFind.Flags().BoolVar(&c.fSourced, "sourced", false, "Display that sources data from other streams")
	strFind.Flags().BoolVar(&c.fMirrored, "mirrored", false, "Display that mirrors data from other streams")
	strFind.Flags().StringVar(&c.fLeader, "leader", "", "Display only clustered streams with a specific leader")
	flagPlaceholder(strFind, "leader", "SERVER")
	negatableBoolVarP(strFind, &c.listNames, "names", "n", false, "Show just the stream names")
	negatableBoolVar(strFind, &c.fInvert, "invert", false, "Invert the check - before becomes after, with becomes without")
	strFind.Flags().StringVar(&c.fExpression, "expression", "", "Match streams using an expression language")
	strFind.Flags().IntVar(&c.apiLevel, "api-level", 0, "Match streams that support at least the given api level")

	strInfo := addCommand(str, "info", "Stream information")
	strInfo.Aliases = []string{"nfo", "i"}
	strInfo.RunE = c.infoAction
	cmdAddTags(strInfo, "scope:user", "impact:ro")
	addArg(strInfo, "stream", "Stream to retrieve information for", false, "string")
	strInfo.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	strInfo.Flags().BoolVar(&c.showStateOnly, "state", false, "Shows only the stream state")
	strInfo.Flags().BoolVar(&c.force, "no-select", false, "Do not select streams from a list")

	strState := addCommand(str, "state", "Stream state")
	strState.RunE = c.stateAction
	cmdAddTags(strState, "scope:user", "impact:ro")
	addArg(strState, "stream", "Stream to retrieve state information for", false, "string")
	strState.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	strState.Flags().BoolVar(&c.force, "no-select", false, "Do not select streams from a list")

	strSubs := addCommand(str, "subjects", "Query subjects held in a stream")
	strSubs.Aliases = []string{"subj"}
	strSubs.RunE = c.subjectsAction
	cmdAddTags(strSubs, "scope:user", "impact:ro")
	addArg(strSubs, "stream", "Stream name", false, "string")
	addArgWithDefault(strSubs, "filter", "Limit the subjects to those matching a filter", ">", "string")
	strSubs.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	strSubs.Flags().Var(newEnumValue(&c.reportSort, "messages", "name", "subjects", "messages", "count"), "sort", "Adjusts the sorting order (name, messages)")
	negatableBoolVarP(strSubs, &c.reportSortReverse, "reverse", "R", false, "Reverse sort servers")
	negatableBoolVar(strSubs, &c.listNames, "names", false, "List only subject names")

	strEdit := addCommand(str, "edit", "Edits an existing stream")
	strEdit.Aliases = []string{"update"}
	strEdit.RunE = c.editAction
	cmdAddTags(strEdit, "scope:user", "impact:rw")
	addArg(strEdit, "stream", "Stream to retrieve edit", false, "string")
	strEdit.Flags().Var(newExistingFileValue(&c.inputFile), "config", "JSON file to read configuration from")
	negatableBoolVarP(strEdit, &c.force, "force", "f", false, "Force edit without prompting")
	negatableBoolVarP(strEdit, &c.interactive, "interactive", "i", false, "Edit the configuring using your editor")
	strEdit.Flags().BoolVar(&c.dryRun, "dry-run", false, "Only shows differences, do not edit the stream")
	addCreateFlags(strEdit, true)

	strRm := addCommand(str, "rm", "Removes a stream")
	strRm.Aliases = []string{"delete", "del"}
	strRm.RunE = c.rmAction
	cmdAddTags(strRm, "scope:user", "impact:rw")
	addArg(strRm, "stream", "Stream name", false, "string")
	negatableBoolVarP(strRm, &c.force, "force", "f", false, "Force removal without prompting")

	strPurge := addCommand(str, "purge", "Bulk removes messages from a stream")
	strPurge.RunE = c.purgeAction
	cmdAddTags(strPurge, "scope:user", "impact:rw")
	addArg(strPurge, "stream", "Stream name", false, "string")
	strPurge.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	negatableBoolVarP(strPurge, &c.force, "force", "f", false, "Force removal without prompting")
	strPurge.Flags().StringVar(&c.purgeSubject, "subject", "", "Limits the purge to a specific subject")
	flagPlaceholder(strPurge, "subject", "SUBJECT")
	strPurge.Flags().Uint64Var(&c.purgeSequence, "seq", 0, "Purge up to but not including a specific message sequence")
	flagPlaceholder(strPurge, "seq", "SEQUENCE")
	strPurge.Flags().Uint64Var(&c.purgeKeep, "keep", 0, "Keeps a certain number of messages after the purge")
	flagPlaceholder(strPurge, "keep", "MESSAGES")

	strCopy := addCommand(str, "copy", "Creates a new stream based on the configuration of another, does not copy data")
	strCopy.Aliases = []string{"cp"}
	strCopy.RunE = c.cpAction
	cmdAddTags(strCopy, "scope:user", "impact:rw")
	addArg(strCopy, "source", "Source stream to copy", true, "string")
	addArg(strCopy, "destination", "New stream to create", true, "string")
	addCreateFlags(strCopy, false)

	strRmMsg := addCommand(str, "rmm", "Securely removes an individual message from a stream")
	strRmMsg.RunE = c.rmMsgAction
	cmdAddTags(strRmMsg, "scope:user", "impact:rw")
	addArg(strRmMsg, "stream", "Stream name", false, "string")
	addArg(strRmMsg, "id", "Message Sequence to remove", false, "int")
	negatableBoolVarP(strRmMsg, &c.force, "force", "f", false, "Force removal without prompting")

	strView := addCommand(str, "view", "View messages in a stream")
	strView.RunE = c.viewAction
	cmdAddTags(strView, "scope:user", "impact:ro")
	addArg(strView, "stream", "Stream name", false, "string")
	addArgWithDefault(strView, "size", "Page size <= 25", "10", "int")
	strView.Flags().IntVar(&c.vwStartId, "id", 0, "Start at a specific message Sequence")
	strView.Flags().DurationVar(&c.vwStartDelta, "since", 0, "Delivers messages received since a duration like 1d3h5m2s")
	strView.Flags().BoolVar(&c.vwRaw, "raw", false, "Show the raw data received")
	strView.Flags().StringVar(&c.vwTranslate, "translate", "", "Translate the message data by running it through the given command before output")
	strView.Flags().StringVar(&c.vwSubject, "subject", "", "Filter the stream using a subject")

	strGet := addCommand(str, "get", "Retrieves a specific message from a Stream")
	strGet.RunE = c.getAction
	cmdAddTags(strGet, "scope:user", "impact:ro")
	addArg(strGet, "stream", "Stream name", false, "string")
	addArg(strGet, "id", "Message Sequence to retrieve", false, "int")
	strGet.Flags().StringVarP(&c.filterSubject, "last-for", "S", "", "Retrieves the message for a specific subject")
	flagPlaceholder(strGet, "last-for", "SUBJECT")
	strGet.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	strGet.Flags().StringVar(&c.vwTranslate, "translate", "", "Translate the message data by running it through the given command before output")

	strBackup := addCommand(str, "backup", "Creates a backup of a stream over the NATS network")
	strBackup.Aliases = []string{"snapshot"}
	strBackup.RunE = c.backupAction
	cmdAddTags(strBackup, "scope:user", "impact:ro")
	addArg(strBackup, "stream", "Stream to backup", true, "string")
	addArg(strBackup, "target", "Directory to create the backup in", true, "string")
	negatableBoolVar(strBackup, &c.showProgress, "progress", true, "Enables or disables progress reporting using a progress bar")
	strBackup.Flags().BoolVar(&c.healthCheck, "check", false, "Checks the stream for health prior to backup")
	negatableBoolVar(strBackup, &c.snapShotConsumers, "consumers", true, "Enable or disable consumer backups")
	strBackup.Flags().StringVar(&c.chunkSize, "chunk-size", "", "Sets a specific chunk size that the server will send")
	strBackup.Flags().StringVar(&c.wndSize, "window-size", "", "Sets a specific window size that the server will send")

	strRestore := addCommand(str, "restore", "Restore a stream over the NATS network")
	strRestore.RunE = c.restoreAction
	cmdAddTags(strRestore, "scope:user", "impact:rw")
	addArg(strRestore, "file", "The directory holding the backup to restore", true, "path")
	negatableBoolVar(strRestore, &c.showProgress, "progress", true, "Enables or disables progress reporting using a progress bar")
	strRestore.Flags().Var(newExistingFileValue(&c.inputFile), "config", "Load a different configuration when restoring the stream")
	strRestore.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the stream in a specific cluster")
	strRestore.Flags().StringArrayVar(&c.placementTags, "tag", nil, "Place the stream on servers that has specific tags (pass multiple times)")
	strRestore.Flags().Int64Var(&c.replicas, "replicas", 0, "Override how many replicas of the data to create")

	strSeal := addCommand(str, "seal", "Seals a stream preventing further updates")
	strSeal.RunE = c.sealAction
	cmdAddTags(strSeal, "scope:user", "impact:rw")
	addArg(strSeal, "stream", "The name of the stream to seal", true, "string")
	negatableBoolVarP(strSeal, &c.force, "force", "f", false, "Force sealing without prompting")

	gapDetect := addCommand(str, "gaps", "Detect gaps in the stream content that would be reported as deleted messages")
	gapDetect.RunE = c.detectGaps
	cmdAddTags(gapDetect, "scope:user", "impact:ro")
	addArg(gapDetect, "stream", "Stream to act on", false, "string")
	negatableBoolVarP(gapDetect, &c.force, "force", "f", false, "Act without prompting")
	negatableBoolVar(gapDetect, &c.showProgress, "progress", true, "Enable progress bar")
	gapDetect.Flags().BoolVar(&c.json, "json", false, "Show detected gaps in JSON format")

	graph := addCommand(str, "graph", "View a graph of stream activity")
	graph.RunE = c.graphAction
	cmdAddTags(graph, "scope:user", "impact:ro")
	addArg(graph, "stream", "The name of the stream to graph", false, "string")

	strCluster := addCommand(str, "cluster", "Manages a clustered stream")
	strCluster.Aliases = []string{"c"}
	strClusterDown := addCommand(strCluster, "step-down", "Force a new leader election by standing down the current leader")
	strClusterDown.Aliases = []string{"stepdown", "sd", "elect", "down", "d"}
	strClusterDown.RunE = c.leaderStandDown
	cmdAddTags(strClusterDown, "scope:user", "impact:rw")
	addArg(strClusterDown, "stream", "Stream to act on", false, "string")
	strClusterDown.Flags().StringVar(&c.placementPreferred, "preferred", "", "Prefer placing the leader on a specific host")
	negatableBoolVarP(strClusterDown, &c.force, "force", "f", false, "Force leader step down ignoring current leader")

	strClusterBalance := addCommand(strCluster, "balance", "Balance stream leaders")
	strClusterBalance.RunE = c.balanceAction
	cmdAddTags(strClusterBalance, "scope:user", "impact:rw")
	strClusterBalance.Flags().StringVar(&c.fServer, "server-name", "", "Balance streams present on a regular expression matched server")
	strClusterBalance.Flags().StringVar(&c.fCluster, "cluster", "", "Balance streams present on a regular expression matched cluster")
	strClusterBalance.Flags().BoolVar(&c.fEmpty, "empty", false, "Balance streams with no messages")
	strClusterBalance.Flags().DurationVar(&c.fIdle, "idle", 0, "Balance streams with no new messages or consumer deliveries for a period")
	flagPlaceholder(strClusterBalance, "idle", "DURATION")
	strClusterBalance.Flags().DurationVar(&c.fCreated, "created", 0, "Balance streams created longer ago than duration")
	flagPlaceholder(strClusterBalance, "created", "DURATION")
	strClusterBalance.Flags().IntVar(&c.fConsumers, "consumers", -1, "Balance streams with fewer consumers than threshold")
	flagPlaceholder(strClusterBalance, "consumers", "THRESHOLD")
	strClusterBalance.Flags().StringVar(&c.filterSubject, "subject", "", "Filters streams by those with interest matching a subject or wildcard and balances them")
	strClusterBalance.Flags().UintVar(&c.fReplicas, "replicas", 0, "Balance streams with fewer or equal replicas than the value")
	flagPlaceholder(strClusterBalance, "replicas", "REPLICAS")
	strClusterBalance.Flags().BoolVar(&c.fSourced, "sourced", false, "Balance that sources data from other streams")
	strClusterBalance.Flags().BoolVar(&c.fMirrored, "mirrored", false, "Balance that mirrors data from other streams")
	strClusterBalance.Flags().StringVar(&c.fLeader, "leader", "", "Balance only clustered streams with a specific leader")
	flagPlaceholder(strClusterBalance, "leader", "SERVER")
	negatableBoolVar(strClusterBalance, &c.fInvert, "invert", false, "Invert the check - before becomes after, with becomes without")
	strClusterBalance.Flags().StringVar(&c.fExpression, "expression", "", "Balance matching streams using an expression language")

	strClusterRemovePeer := addCommand(strCluster, "peer-remove", "Removes a peer from the stream cluster")
	strClusterRemovePeer.Aliases = []string{"pr"}
	strClusterRemovePeer.RunE = c.removePeer
	cmdAddTags(strClusterRemovePeer, "scope:user", "impact:rw")
	addArg(strClusterRemovePeer, "stream", "The stream to act on", false, "string")
	addArg(strClusterRemovePeer, "peer", "The name of the peer to remove", false, "string")
	negatableBoolVarP(strClusterRemovePeer, &c.force, "force", "f", false, "Force sealing without prompting")
}

func init() {
	registerCommand("stream", 16, configureStreamCommand)
}

func (c *streamCmd) kvAbstractionWarn(stream string, prompt string) error {
	fmt.Println("WARNING: Operating on the underlying stream of a Key-Value bucket is dangerous.")
	fmt.Println()
	fmt.Println("Key-Value stores are an abstraction above JetStream streams and as such require particular")
	fmt.Println("configuration to be set. Interacting with KV buckets outside of the 'nats kv' subcommand can lead")
	fmt.Println("unexpected outcomes, data loss and, technically, will mean your KV bucket is no longer a KV bucket.")
	fmt.Println()
	fmt.Println("Continuing this operation is an unsupported action.")
	fmt.Println()

	ans, err := askConfirmation(prompt, false)
	if err != nil {
		return err
	}

	if !ans {
		return fmt.Errorf("aborting Key-Value store operation")
	}

	return nil
}

func (c *streamCmd) graphAction(_ *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)

	if !iu.IsTerminal() {
		return fmt.Errorf("can only graph data on an interactive terminal")
	}

	width, height, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("failed to get terminal dimensions: %w", err)
	}

	if width < 20 || height < 20 {
		return fmt.Errorf("please increase terminal dimensions")
	}

	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	nfo, err := stream.State()
	if err != nil {
		return err
	}

	messageRates := make([]float64, width)
	messagesStored := make([]float64, width)
	limitedRates := make([]float64, width)
	lastLastSeq := nfo.LastSeq
	lastFirstSeq := nfo.FirstSeq
	lastStateTs := time.Now()

	resizeData := func(data []float64, width int) []float64 {
		if width <= 0 {
			return data
		}

		length := len(data)

		if length > width {
			return data[length-width:]
		}

		return data
	}

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			width, height, err = terminal.GetSize(int(os.Stdout.Fd()))
			if err != nil {
				height = 40
				width = 80
			}
			if width > 15 {
				width -= 11
			}
			if height > 10 {
				height -= 6
			}

			if width < 20 || height < 20 {
				return fmt.Errorf("please increase terminal dimensions")
			}

			nfo, err := stream.State()
			if err != nil {
				continue
			}

			messagesStored = append(messagesStored, float64(nfo.Msgs))
			messageRates = append(messageRates, calculateRate(float64(nfo.LastSeq), float64(lastLastSeq), time.Since(lastStateTs)))
			limitedRates = append(limitedRates, calculateRate(float64(nfo.FirstSeq), float64(lastFirstSeq), time.Since(lastStateTs)))

			lastStateTs = time.Now()
			lastLastSeq = nfo.LastSeq
			lastFirstSeq = nfo.FirstSeq

			messageRates = resizeData(messageRates, width)
			messagesStored = resizeData(messagesStored, width)
			limitedRates = resizeData(limitedRates, width)

			messagesPlot := asciigraph.Plot(messagesStored,
				asciigraph.Caption("Messages Stored"),
				asciigraph.Width(width),
				asciigraph.Height(height/3-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(fFloat2Int),
			)

			limitedRatePlot := asciigraph.Plot(limitedRates,
				asciigraph.Caption("Messages Removed / second"),
				asciigraph.Width(width),
				asciigraph.Height(height/3-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(fFloatFixedDecimal),
			)

			msgRatePlot := asciigraph.Plot(messageRates,
				asciigraph.Caption("Messages Stored / second"),
				asciigraph.Width(width),
				asciigraph.Height(height/3-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(fFloatFixedDecimal),
			)

			iu.ClearScreen()

			fmt.Printf("Stream Statistics for %s\n", c.stream)
			fmt.Println()
			fmt.Println(messagesPlot)
			fmt.Println()
			fmt.Println(limitedRatePlot)
			fmt.Println()
			fmt.Println(msgRatePlot)

		case <-ctx.Done():
			iu.ClearScreen()
			return nil
		}
	}
}

func (c *streamCmd) detectGaps(_ *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)

	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	if info.State.NumDeleted == 0 {
		if c.json {
			fmt.Println("{}")
		}

		fmt.Printf("No deleted messages in %s\n", c.stream)
		return nil
	}

	if !c.force {
		fmt.Println("WARNING: Detecting gaps in a stream consumes the entire stream and can be resource intensive on the Server, Client and Network.")
		fmt.Println()
		ok, err := askConfirmation(fmt.Sprintf("Really detect gaps in stream %s with %s messages and %s bytes", c.stream, humanize.Comma(int64(info.State.Msgs)), humanize.IBytes(info.State.Bytes)), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	if c.json {
		c.showProgress = false
	}

	var progbar progress.Writer
	var tracker *progress.Tracker

	var gaps [][2]uint64
	var cnt int

	progressCb := func(seq uint64, pending uint64) {
		if !c.showProgress {
			return
		}
		if tracker == nil {
			progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(pending),
			})
		}
		cnt++

		tracker.SetValue(int64(seq))

		if pending == 0 {
			tracker.SetValue(tracker.Total)
			tracker.MarkAsDone()
		}
	}

	gapCb := func(start, end uint64) {
		gaps = append(gaps, [2]uint64{start, end})
	}

	err = stream.DetectGaps(ctx, progressCb, gapCb)
	if tracker != nil {
		time.Sleep(250 * time.Millisecond) // let it draw
		progbar.Stop()
		fmt.Println()
	}
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(gaps)
		return nil
	}

	if len(gaps) == 0 {
		fmt.Printf("No deleted messages in %s\n", c.stream)
		return nil
	}

	var table *iu.Table
	if len(gaps) == 1 {
		table = iu.NewTableWriterf(opts(), "1 gap found in Stream %s", c.stream)
	} else {
		table = iu.NewTableWriterf(opts(), "%s gaps found in Stream %s", f(len(gaps)), c.stream)
	}

	table.AddHeaders("First Message", "Last Message")
	for _, gap := range gaps {
		table.AddRow(f(gap[0]), f(gap[1]))
	}
	fmt.Println(table.Render())

	return nil
}

func (c *streamCmd) subjectsAction(_ *cobra.Command, args []string) (err error) {
	c.stream = argValue(args, 0)
	c.filterSubject = ">"
	if v := argValue(args, 1); v != "" {
		c.filterSubject = v
	}

	asked := c.connectAndAskStream()

	subs, err := c.mgr.StreamContainedSubjects(c.stream, c.filterSubject)
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(subs)
		return nil
	}

	if asked {
		fmt.Println()
	}

	if len(subs) == 0 {
		fmt.Printf("No subjects found matching %s\n", c.filterSubject)
		return nil
	}

	var longest int
	var most uint64
	var names []string

	for s, c := range subs {
		names = append(names, s)
		if len(s) > longest {
			longest = len(s)
		}
		if c > most {
			most = c
		}
	}

	cols := 1
	countWidth := len(f(most))
	table := iu.NewTableWriterf(opts(), "%d Subjects in stream %s", len(names), c.stream)

	switch {
	case longest+countWidth < 20:
		cols = 3
		table.AddHeaders("Subject", "Count", "Subject", "Count", "Subject", "Count")
	case longest+countWidth < 30:
		cols = 2
		table.AddHeaders("Subject", "Count", "Subject", "Count")
	default:
		table.AddHeaders("Subject", "Count")
	}

	sort.Slice(names, func(i, j int) bool {
		if c.reportSort == "name" || c.reportSort == "subjects" {
			return c.boolReverse(names[i] < names[j])
		} else {
			return c.boolReverse(subs[names[i]] < subs[names[j]])
		}
	})

	if c.listNames {
		for _, n := range names {
			fmt.Println(n)
		}
		return
	}

	comma := func(i uint64) string {
		if i == 0 {
			return ""
		}
		return f(i)
	}

	iu.SliceGroups(names, cols, func(g []string) {
		if cols == 1 {
			table.AddRow(g[0], comma(subs[g[0]]))
		} else if cols == 2 {
			table.AddRow(g[0], comma(subs[g[0]]), g[1], comma(subs[g[1]]))
		} else {
			table.AddRow(g[0], comma(subs[g[0]]), g[1], comma(subs[g[1]]), g[2], comma(subs[g[2]]))
		}
	})

	fmt.Println(table.Render())

	return nil
}

func (c *streamCmd) parseLimitStrings(_ *cobra.Command, _ []string) (err error) {
	if c.maxBytesLimitString != "" {
		c.maxBytesLimit, err = iu.ParseStringAsBytes(c.maxBytesLimitString, 32)
		if err != nil {
			return err
		}
	}

	if c.maxMsgSizeString != "" {
		c.maxMsgSize, err = iu.ParseStringAsBytes(c.maxMsgSizeString, 32)
		if err != nil {
			return err
		}
	}

	return nil
}

// setCreateFlagsState populates the IsSetByUser tracking booleans for the flags
// registered by addCreateFlags, based on whether the user provided them.
func (c *streamCmd) setCreateFlagsState(cmd *cobra.Command) {
	c.compressionSet = cmd.Flags().Changed("compression")
	c.placementTagsSet = cmd.Flags().Changed("tag") || cmd.Flags().Changed("tags")
	c.placementClusterSet = cmd.Flags().Changed("cluster")
	c.discardPerSubjSet = cmd.Flags().Changed("discard-per-subject")
	c.allowAtomicBatchIsSet = cmd.Flags().Changed("allow-batch")
	c.allowFastBatchIsSet = cmd.Flags().Changed("allow-fast")
	c.allowCounterIsSet = cmd.Flags().Changed("allow-counter")
	c.allowRollupSet = cmd.Flags().Changed("allow-rollup")
	c.denyDeleteSet = cmd.Flags().Changed("deny-delete")
	c.denyPurgeSet = cmd.Flags().Changed("deny-purge")
	c.allowDirectSet = cmd.Flags().Changed("allow-direct")
	c.allowMirrorDirectSet = cmd.Flags().Changed("allow-mirror-direct")
	c.allowMsgTTlSet = cmd.Flags().Changed("allow-msg-ttl")
	c.allowSchedulesSet = cmd.Flags().Changed("allow-schedules")
	c.subjectDeleteMarkerTTLSet = cmd.Flags().Changed("subject-del-markers-ttl")
	c.metadataIsSet = cmd.Flags().Changed("metadata")
}

func (c *streamCmd) findAction(cmd *cobra.Command, _ []string) (err error) {
	c.fSourcedSet = cmd.Flags().Changed("sourced")
	c.fMirroredSet = cmd.Flags().Changed("mirrored")

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	var opts []jsm.StreamQueryOpt
	if c.fServer != "" {
		opts = append(opts, jsm.StreamQueryServerName(c.fServer))
	}
	if c.fCluster != "" {
		opts = append(opts, jsm.StreamQueryClusterName(c.fCluster))
	}
	if c.fEmpty {
		opts = append(opts, jsm.StreamQueryWithoutMessages())
	}
	if c.fIdle > 0 {
		opts = append(opts, jsm.StreamQueryIdleLongerThan(c.fIdle))
	}
	if c.fCreated > 0 {
		opts = append(opts, jsm.StreamQueryOlderThan(c.fCreated))
	}
	if c.fConsumers >= 0 {
		opts = append(opts, jsm.StreamQueryFewerConsumersThan(uint(c.fConsumers)))
	}
	if c.fInvert {
		opts = append(opts, jsm.StreamQueryInvert())
	}
	if c.filterSubject != "" {
		opts = append(opts, jsm.StreamQuerySubjectWildcard(c.filterSubject))
	}
	if c.fSourcedSet {
		opts = append(opts, jsm.StreamQueryIsSourced())
	}
	if c.fMirroredSet {
		opts = append(opts, jsm.StreamQueryIsMirror())
	}
	if c.fReplicas > 0 {
		opts = append(opts, jsm.StreamQueryReplicas(c.fReplicas))
	}
	if c.fExpression != "" {
		opts = append(opts, jsm.StreamQueryExpression(c.fExpression))
	}
	if c.fLeader != "" {
		opts = append(opts, jsm.StreamQueryLeaderServer(c.fLeader))
	}
	if c.apiLevel > 0 {
		opts = append(opts, jsm.StreamQueryApiLevelMin(c.apiLevel))
	}

	found, err := c.mgr.QueryStreams(opts...)
	if err != nil {
		return err
	}

	out := ""
	switch {
	case c.listNames:
		out = c.renderStreamsAsList(found, nil)
	default:
		out, err = c.renderStreamsAsTable(found, nil, nil)
	}
	if err != nil {
		return err
	}

	fmt.Println(out)

	return nil
}

func (c *streamCmd) loadStream(stream string) (*jsm.Stream, error) {
	if c.selectedStream != nil && c.selectedStream.Name() == stream {
		return c.selectedStream, nil
	}

	return c.mgr.LoadStream(stream)
}

func (c *streamCmd) balanceAction(cmd *cobra.Command, _ []string) error {
	var err error

	c.fSourcedSet = cmd.Flags().Changed("sourced")
	c.fMirroredSet = cmd.Flags().Changed("mirrored")

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	var opts []jsm.StreamQueryOpt
	if c.fServer != "" {
		opts = append(opts, jsm.StreamQueryServerName(c.fServer))
	}
	if c.fCluster != "" {
		opts = append(opts, jsm.StreamQueryClusterName(c.fCluster))
	}
	if c.fEmpty {
		opts = append(opts, jsm.StreamQueryWithoutMessages())
	}
	if c.fIdle > 0 {
		opts = append(opts, jsm.StreamQueryIdleLongerThan(c.fIdle))
	}
	if c.fCreated > 0 {
		opts = append(opts, jsm.StreamQueryOlderThan(c.fCreated))
	}
	if c.fConsumers >= 0 {
		opts = append(opts, jsm.StreamQueryFewerConsumersThan(uint(c.fConsumers)))
	}
	if c.fInvert {
		opts = append(opts, jsm.StreamQueryInvert())
	}
	if c.filterSubject != "" {
		opts = append(opts, jsm.StreamQuerySubjectWildcard(c.filterSubject))
	}
	if c.fSourcedSet {
		opts = append(opts, jsm.StreamQueryIsSourced())
	}
	if c.fMirroredSet {
		opts = append(opts, jsm.StreamQueryIsMirror())
	}
	if c.fReplicas > 0 {
		opts = append(opts, jsm.StreamQueryReplicas(c.fReplicas))
	}
	if c.fExpression != "" {
		opts = append(opts, jsm.StreamQueryExpression(c.fExpression))
	}
	if c.fLeader != "" {
		opts = append(opts, jsm.StreamQueryLeaderServer(c.fLeader))
	}

	found, err := c.mgr.QueryStreams(opts...)
	if err != nil {
		return err
	}

	if len(found) > 0 {
		balancer, err := balancer.New(c.mgr.NatsConn(), api.NewDefaultLogger(api.InfoLevel))
		if err != nil {
			return err
		}

		balanced, err := balancer.BalanceStreams(found)
		if err != nil {
			return fmt.Errorf("failed to balance streams: %s", err)
		}
		fmt.Printf("Balanced %d streams.\n", balanced)

	}

	return nil
}

func (c *streamCmd) leaderStandDown(_ *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)

	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	if info.Cluster == nil || len(info.Cluster.Replicas) == 0 {
		return fmt.Errorf("stream %q is not clustered", stream.Name())
	}

	leader := info.Cluster.Leader
	if leader == "" && !c.force {
		return fmt.Errorf("stream %q has no current leader", stream.Name())
	} else if leader == "" {
		leader = "<unknown>"
	}

	var p *api.Placement
	if c.placementPreferred != "" {
		err = iu.RequireAPILevel(c.mgr, 1, "placement hints during step-down requires NATS Server 2.11")
		if err != nil {
			return err
		}

		p = &api.Placement{Preferred: c.placementPreferred}
	}

	log.Printf("Requesting leader step down of %q for stream %q in a %d peer cluster group", leader, stream.Name(), len(info.Cluster.Replicas)+1)
	err = stream.LeaderStepDown(p)
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTicker(500 * time.Millisecond).C {
		if ctr == 10 {
			return fmt.Errorf("stream %q did not elect a new leader in time", stream.Name())
		}
		ctr++

		info, err = stream.Information()
		if err != nil {
			log.Printf("Failed to retrieve Stream State: %s", err)
			continue
		}

		if info.Cluster == nil || info.Cluster.Leader == "" {
			log.Printf("No leader elected")
			continue
		}

		if info.Cluster.Leader != leader {
			log.Printf("New leader elected %q", info.Cluster.Leader)
			break
		}
	}

	if info.Cluster.Leader == leader {
		log.Printf("Leader did not change after %s", time.Since(start).Round(time.Millisecond))
	}

	fmt.Println()
	return c.showStream(stream)
}

func (c *streamCmd) removePeer(_ *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)
	c.peerName = argValue(args, 1)

	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.Information()
	if err != nil {
		return err
	}

	if info.Cluster == nil {
		return fmt.Errorf("stream %q is not clustered", stream.Name())
	}

	peerNames := []string{info.Cluster.Leader}
	for _, r := range info.Cluster.Replicas {
		peerNames = append(peerNames, r.Name)
	}

	if len(peerNames) == 1 && !c.force {
		return fmt.Errorf("removing the only peer on a stream will result in data loss, use --force to force")
	}

	if c.peerName == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Select a Peer",
			Options: peerNames,
		}, &c.peerName)
		if err != nil {
			return err
		}
	}

	log.Printf("Removing peer %q", c.peerName)

	err = stream.RemoveRAFTPeer(c.peerName)
	if err != nil {
		return err
	}

	log.Printf("Requested removal of peer %q", c.peerName)

	return nil
}

func (c *streamCmd) viewAction(_ *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)
	c.vwPageSize = 10
	if v := argValue(args, 1); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid page size %q: %w", v, err)
		}
		c.vwPageSize = n
	}

	if !iu.IsTerminal() {
		return fmt.Errorf("interactive stream paging requires a valid terminal")
	}

	if c.vwPageSize > 25 {
		log.Printf("Page size is limited to 25, setting to 25...")
		c.vwPageSize = 25
	}

	c.connectAndAskStream()

	str, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	pops := []jsm.PagerOption{
		jsm.PagerSize(c.vwPageSize),
	}

	switch {
	case c.vwStartDelta > 0:
		pops = append(pops, jsm.PagerStartDelta(c.vwStartDelta))
	case c.vwStartId > 0:
		pops = append(pops, jsm.PagerStartId(c.vwStartId))
	}

	if c.vwSubject != "" {
		pops = append(pops, jsm.PagerFilterSubject(c.vwSubject))
	}

	pgr, err := str.PageContents(pops...)
	if err != nil {
		return err
	}
	defer pgr.Close()

	ctx, cancel := context.WithCancel(ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
			cancel()
		}
	}()

	shouldTerminate := false

	for {
		msg, last, err := pgr.NextMsg(ctx)
		if err != nil {
			if !last {
				return err
			}
			// later we know we reached final last after showing the final message
			shouldTerminate = true
		}

		switch {
		case msg == nil:
			shouldTerminate = true
		case c.vwRaw:
			fmt.Println(string(msg.Data))
		default:
			meta, err := jsm.ParseJSMsgMetadata(msg)
			if err == nil {
				fmt.Printf("[%d] Subject: %s Received: %s\n", meta.StreamSequence(), msg.Subject, f(meta.TimeStamp()))
			} else {
				fmt.Printf("Subject: %s Reply: %s\n", msg.Subject, msg.Reply)
			}

			if len(msg.Header) > 0 {
				fmt.Println()
				for k, vs := range msg.Header {
					for _, v := range vs {
						if k == "Nats-Stream-Source" {
							v = strings.ReplaceAll(v, "\f", "\u240A")
						}

						if k == "Nats-Subject" || k == "Nats-Stream" || k == "Nats-Sequence" || k == "Nats-Time-Stamp" || k == "Nats-Num-Pending" || k == "Nats-Last-Sequence" || k == "Nats-UpTo-Sequnce" {
							continue
						}

						fmt.Printf("  %s: %s\n", k, v)
					}
				}
			}

			outPutMSGBody(msg.Data, c.vwTranslate, msg.Subject, meta.Stream())
		}

		if shouldTerminate {
			log.Println("Reached apparent end of data")
			return nil
		}

		if last {
			next := false
			iu.AskOne(&survey.Confirm{Message: "Next Page?", Default: true}, &next)
			if !next {
				return nil
			}
		}
	}
}

func (c *streamCmd) sealAction(_ *cobra.Command, args []string) error {
	c.stream = args[0]

	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really seal Stream %s, sealed streams can not be unsealed or modified", c.stream), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not seal Stream")

	stream.Seal()
	fatalIfError(err, "could not seal Stream")

	return c.showStream(stream)
}

func (c *streamCmd) restoreAction(_ *cobra.Command, args []string) error {
	// Called both as a command (positional directory arg) and internally by
	// account restore which pre-sets backupDirectory and passes no args.
	if len(args) > 0 {
		c.backupDirectory = args[0]
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	var bm api.JSApiStreamRestoreRequest
	bmj, err := os.ReadFile(filepath.Join(c.backupDirectory, "backup.json"))
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
		if opts().Trace && (p.ChunksSent()%100 == 0 || time.Since(prevMsg) > 500*time.Millisecond) {
			fmt.Printf("Sent %v chunk %v / %v at %v / s\n", fiBytes(uint64(p.ChunkSize())), p.ChunksSent(), p.ChunksToSend(), fiBytes(p.BytesPerSecond()))
			return
		}

		prevMsg = time.Now()

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
		cfg, err = c.loadConfigFile(c.inputFile)
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

	fmt.Printf("Starting restore of Stream %q from file %q\n\n", bm.Config.Name, c.backupDirectory)

	fp, _, err := mgr.RestoreSnapshotFromDirectory(ctx, bm.Config.Name, c.backupDirectory, ropts...)
	fatalIfError(err, "restore failed")
	if c.showProgress {
		tracker.SetValue(int64(fp.ChunksSent() * uint32(fp.ChunkSize())))
		time.Sleep(300 * time.Millisecond)
		progbar.Stop()
	}

	fmt.Println()
	fmt.Printf("Restored stream %q in %v\n", bm.Config.Name, fp.EndTime().Sub(fp.StartTime()).Round(time.Second))
	fmt.Println()

	stream, err := mgr.LoadStream(bm.Config.Name)
	fatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	fatalIfError(err, "could not show stream")

	return nil
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

func (c *streamCmd) backupAction(_ *cobra.Command, args []string) error {
	c.stream = args[0]
	c.backupDirectory = args[1]

	var err error

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	// Default is set in strBackup flags.
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

	err = backupStream(stream, c.showProgress, c.snapShotConsumers, c.healthCheck, c.backupDirectory, int(chunkSize), int(wndSize))
	fatalIfError(err, "snapshot failed")

	return nil
}

func (c *streamCmd) reportAction(_ *cobra.Command, _ []string) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	if !c.json {
		fmt.Print("Obtaining Stream stats\n\n")
	}

	stats := []streamStat{}
	leaders := make(map[string]*raftLeader)
	showReplication := false
	var filter *jsm.StreamNamesFilter

	if c.filterSubject != "" {
		filter = &jsm.StreamNamesFilter{Subject: c.filterSubject}
	}

	dg := dot.NewGraph(dot.Directed)
	dg.Label("Stream Replication Structure")

	missing, offline, err := mgr.EachStream(filter, func(stream *jsm.Stream) {
		info, err := stream.LatestInformation()
		fatalIfError(err, "could not get stream info for %s", stream.Name())

		if info.Cluster != nil {
			if c.reportLimitCluster != "" && info.Cluster.Name != c.reportLimitCluster {
				return
			}

			if info.Cluster.Leader != "" {
				_, ok := leaders[info.Cluster.Leader]
				if !ok {
					leaders[info.Cluster.Leader] = &raftLeader{name: info.Cluster.Leader, cluster: info.Cluster.Name}
				}
				leaders[info.Cluster.Leader].groups++
			}
		}

		deleted := info.State.NumDeleted
		// backward compat with servers that predate the num_deleted response
		if len(info.State.Deleted) > 0 {
			deleted = len(info.State.Deleted)
		}

		apiLevel := info.Config.Metadata[api.JsMetaRequiredServerLevel]
		if apiLevel == "" {
			apiLevel = "0"
		}

		s := streamStat{
			Name:      info.Config.Name,
			Consumers: info.State.Consumers,
			Msgs:      int64(info.State.Msgs),
			Bytes:     info.State.Bytes,
			Storage:   info.Config.Storage.String(),
			Cluster:   info.Cluster,
			Deleted:   deleted,
			Mirror:    info.Mirror,
			Sources:   info.Sources,
			Placement: info.Config.Placement,
			APILevel:  apiLevel,
		}

		if info.State.Lost != nil {
			s.LostBytes = info.State.Lost.Bytes
			s.LostMsgs = len(info.State.Lost.Msgs)
		}

		if len(info.Config.Sources) > 0 {
			showReplication = true
			node, ok := dg.FindNodeById(info.Config.Name)
			if !ok {
				node = dg.Node(info.Config.Name)
			}
			for _, source := range info.Config.Sources {
				snode, ok := dg.FindNodeById(source.Name)
				if !ok {
					snode = dg.Node(source.Name)
				}
				edge := dg.Edge(snode, node).Attr("color", "green")

				if source.FilterSubject == "" {
					continue
				}

				if len(source.SubjectTransforms) == 0 {
					edge.Label(source.FilterSubject)
				} else if len(source.SubjectTransforms) == 1 {
					edge.Label(source.SubjectTransforms[0].Source + " to " + source.SubjectTransforms[0].Destination)
				}
			}
		}

		if info.Config.Mirror != nil {
			showReplication = true
			node, ok := dg.FindNodeById(info.Config.Name)
			if !ok {
				node = dg.Node(info.Config.Name)
			}
			mnode, ok := dg.FindNodeById(info.Config.Mirror.Name)
			if !ok {
				mnode = dg.Node(info.Config.Mirror.Name)
			}
			dg.Edge(mnode, node).Attr("color", "blue").Label("Mirror")
		}

		stats = append(stats, s)
	})
	if err != nil {
		return err
	}

	if len(stats) == 0 && len(missing) == 0 && len(offline) == 0 {
		if !c.json {
			fmt.Println("No Streams defined")
		}
		return nil
	}

	if c.reportSortConsumers {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Consumers < stats[j].Consumers })
	} else if c.reportSortMsgs {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Msgs < stats[j].Msgs })
	} else if c.reportSortName {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	} else if c.reportSortStorage {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Storage < stats[j].Storage })
	} else {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Bytes < stats[j].Bytes })
	}

	c.renderStreams(stats)

	if showReplication {
		c.renderReplication(stats)

		if c.outFile != "" {
			os.WriteFile(c.outFile, []byte(dg.String()), 0600)
		}
	}

	if c.reportLeaderDistrib && len(leaders) > 0 {
		renderRaftLeaders(leaders, "Streams")
	}

	c.renderMissing(os.Stdout, missing, offline)

	return nil
}

func (c *streamCmd) renderReplication(stats []streamStat) {
	table := iu.NewTableWriterf(opts(), "Replication Report")
	table.AddHeaders("Stream", "Kind", "API Prefix", "Source Stream", "Filters and Transforms", "Active", "Lag", "Error")

	for _, s := range stats {
		if len(s.Sources) == 0 && s.Mirror == nil {
			continue
		}

		if s.Mirror != nil {
			apierr := ""
			if s.Mirror.Error != nil {
				apierr = s.Mirror.Error.Error()
			}

			eApiPrefix := ""
			if s.Mirror.External != nil {
				eApiPrefix = s.Mirror.External.ApiPrefix
			}

			if c.reportRaw {
				table.AddRow(s.Name, "Mirror", eApiPrefix, s.Mirror.Name, "", s.Mirror.Active, s.Mirror.Lag, apierr)
			} else {
				table.AddRow(s.Name, "Mirror", eApiPrefix, s.Mirror.Name, "", f(s.Mirror.Active), f(s.Mirror.Lag), apierr)
			}
		}

		for _, source := range s.Sources {
			apierr := ""
			if source != nil && source.Error != nil {
				apierr = source.Error.Error()
			}

			eApiPrefix := ""
			if source.External != nil {
				eApiPrefix = source.External.ApiPrefix
			}

			filterSubject := []string{}

			for _, transform := range source.SubjectTransforms {
				filterSubject = append(filterSubject, fmt.Sprintf("%s to %s", transform.Source, transform.Destination))
			}

			if len(filterSubject) == 0 && source.FilterSubject != "" {
				filterSubject = append(filterSubject, fmt.Sprintf("%s untransformed", source.FilterSubject))
			}

			if c.reportRaw {
				table.AddRow(s.Name, "Source", eApiPrefix, source.Name, strings.Join(filterSubject, ", "), source.Active, source.Lag, apierr)
			} else {
				table.AddRow(s.Name, "Source", eApiPrefix, source.Name, strings.Join(filterSubject, ", "), f(source.Active), f(source.Lag), apierr)
			}

		}
	}
	fmt.Println(table.Render())
}

func (c *streamCmd) renderStreams(stats []streamStat) {
	table := iu.NewTableWriterf(opts(), "Stream Report")
	table.AddHeaders("Stream", "Storage", "Placement", "Consumers", "Messages", "Bytes", "Lost", "Deleted", "API Level", "Replicas")

	for _, s := range stats {
		lost := "0"
		placement := ""
		if s.Placement != nil {
			if s.Placement.Cluster != "" {
				placement = fmt.Sprintf("cluster: %s ", s.Placement.Cluster)
			}
			if len(s.Placement.Tags) > 0 {
				placement = fmt.Sprintf("%stags: %s", placement, f(s.Placement.Tags))
			}
		}

		if c.reportRaw {
			if s.LostMsgs > 0 {
				lost = fmt.Sprintf("%d (%d)", s.LostMsgs, s.LostBytes)
			}
			table.AddRow(s.Name, s.Storage, placement, s.Consumers, s.Msgs, s.Bytes, lost, s.Deleted, renderCluster(s.Cluster))
		} else {
			if s.LostMsgs > 0 {
				lost = fmt.Sprintf("%s (%s)", f(s.LostMsgs), humanize.IBytes(s.LostBytes))
			}
			table.AddRow(s.Name, s.Storage, placement, f(s.Consumers), f(s.Msgs), humanize.IBytes(s.Bytes), lost, f(s.Deleted), s.APILevel, renderCluster(s.Cluster))
		}
	}

	fmt.Println(table.Render())
}

func (c *streamCmd) loadConfigFile(file string) (*api.StreamConfig, error) {
	f, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var cfg api.StreamConfig

	// there is a chance that this is a `nats s info --json` output
	// which is a StreamInfo, so we detect if this is one of those
	// by checking if there's a config key then extract that, else
	// we try loading it as a StreamConfig

	var nfo map[string]any
	err = json.Unmarshal(f, &nfo)
	if err != nil {
		return nil, err
	}

	_, ok := nfo["config"]
	if ok {
		var nfo api.StreamInfo
		err = json.Unmarshal(f, &nfo)
		if err != nil {
			return nil, err
		}
		cfg = nfo.Config
	} else {
		err = json.Unmarshal(f, &cfg)
		if err != nil {
			return nil, err
		}
	}

	if cfg.Name != c.stream && c.stream != "" {
		cfg.Name = c.stream
	}

	return &cfg, nil
}

func (c *streamCmd) checkRepubTransform() {
	if (c.repubSource != "" && c.repubDest == "") || (c.repubSource == "" && c.repubDest != "") || (c.repubHeadersOnly && (c.repubSource == "" || c.repubDest == "")) {
		msg := "must specify both --republish-source and --republish-destination"

		if c.repubHeadersOnly {
			msg = msg + " when using --headers-only"
		}

		fatalf(msg)
	}

	if (c.subjectTransformSource != "" && c.subjectTransformDest == "") || (c.subjectTransformSource == "" && c.subjectTransformDest != "") {
		msg := "must specify both --transform-source and --transform-destination"

		if c.repubHeadersOnly {
			msg = msg + " when using --headers-only"
		}

		fatalf(msg)
	}
}

func (c *streamCmd) copyAndEditStream(cfg api.StreamConfig) (api.StreamConfig, error) {
	var err error

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		if err != nil {
			return api.StreamConfig{}, err
		}

		if cfg.Name == "" {
			cfg.Name = c.stream
		}

		return *cfg, nil
	}

	c.checkRepubTransform()

	cfg.NoAck = !c.ack

	if c.discardPolicy != "" {
		cfg.Discard = c.discardPolicyFromString()
	}

	if len(c.subjects) > 0 {
		cfg.Subjects = iu.SplitCLISubjects(c.subjects)
	}

	if c.storage != "" {
		cfg.Storage = c.storeTypeFromString(c.storage)
	}

	if c.retentionPolicyS != "" {
		cfg.Retention = c.retentionPolicyFromString()
	}

	if c.maxBytesLimit != 0 {
		cfg.MaxBytes = c.maxBytesLimit
	}

	if c.maxMsgLimit != 0 {
		cfg.MaxMsgs = c.maxMsgLimit
	}

	if c.maxMsgPerSubjectLimit != 0 {
		cfg.MaxMsgsPer = c.maxMsgPerSubjectLimit
	}

	if c.maxAgeLimit != "" {
		cfg.MaxAge, err = parseDuration(c.maxAgeLimit)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid maximum age limit format: %v", err)
		}
	}

	if c.maxMsgSize != 0 {
		cfg.MaxMsgSize = int32(c.maxMsgSize)
	}

	if c.maxConsumers != -1 {
		cfg.MaxConsumers = c.maxConsumers
	}

	if c.dupeWindow != "" {
		dw, err := parseDuration(c.dupeWindow)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid duplicate window: %v", err)
		}
		cfg.Duplicates = dw
	}

	if c.replicas != 0 {
		cfg.Replicas = int(c.replicas)
	}

	if cfg.Placement == nil {
		cfg.Placement = &api.Placement{}
	}

	// For placement constraints, we explicitly support empty strings to
	// remove, so use the *Set bool variables to distinguish "was set on
	// command-line" from "is not empty".

	if c.placementClusterSet {
		cfg.Placement.Cluster = c.placementCluster
	}

	if c.placementTagsSet {
		// With the repeated set, we do accumulate the empty string as a list item.
		// We do still need the separate IsSetByUser variable to get that.
		if len(c.placementTags) == 0 || (len(c.placementTags) == 1 && c.placementTags[0] == "") {
			cfg.Placement.Tags = nil
		} else {
			cfg.Placement.Tags = c.placementTags
		}
	}

	if cfg.Placement.Cluster == "" && len(cfg.Placement.Tags) == 0 {
		cfg.Placement = nil
	}

	if len(c.sources) > 0 || c.mirror != "" {
		return cfg, fmt.Errorf("cannot edit mirrors, or sources using the CLI, use --config instead")
	}

	if c.description != "" {
		cfg.Description = c.description
	}

	if c.allowRollupSet {
		cfg.RollupAllowed = c.allowRollup
	}

	if c.denyPurgeSet {
		cfg.DenyPurge = c.denyPurge
	}

	if c.denyDeleteSet {
		cfg.DenyDelete = c.denyDelete
	}

	if c.allowDirectSet {
		cfg.AllowDirect = c.allowDirect
	}

	if c.allowMirrorDirectSet {
		cfg.MirrorDirect = c.allowMirrorDirect
	}

	if c.allowSchedulesSet {
		cfg.AllowMsgSchedules = c.allowSchedules
	}

	if c.allowMsgTTL {
		cfg.AllowMsgTTL = c.allowMsgTTL
	}

	if c.allowAtomicBatchIsSet {
		cfg.AllowAtomicPublish = c.allowAtomicBatch
	}
	if c.allowFastBatchIsSet {
		cfg.AllowBatchPublish = c.allowFastBatch
	}

	if c.allowCounterIsSet {
		cfg.AllowMsgCounter = c.allowCounter
	}

	if c.discardPerSubjSet {
		cfg.DiscardNewPer = c.discardPerSubj
	}

	if c.metadataIsSet {
		cfg.Metadata = c.metadata
	}

	if c.compressionSet {
		if err = cfg.Compression.UnmarshalJSON([]byte(fmt.Sprintf("%q", c.compression))); err != nil {
			return cfg, fmt.Errorf("invalid compression algorithm")
		}
	}

	if !c.noRepub && c.repubSource != "" && c.repubDest != "" {
		cfg.RePublish = &api.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	if c.noSubjectTransform {
		cfg.SubjectTransform = nil
	} else {
		var subjectTransformConfig api.SubjectTransformConfig

		if cfg.SubjectTransform != nil {
			subjectTransformConfig = *cfg.SubjectTransform
		}

		subjectTransformConfig.Source = c.subjectTransformSource
		subjectTransformConfig.Destination = c.subjectTransformDest

		if subjectTransformConfig.Source != "" && subjectTransformConfig.Destination != "" {
			cfg.SubjectTransform = &subjectTransformConfig
		}
	}

	if c.subjectDeleteMarkerTTLSet {
		cfg.SubjectDeleteMarkerTTL = c.subjectDeleteMarkerTTL
	}

	if c.noMirror {
		cfg.Mirror = nil
	}

	return cfg, nil
}

func (c *streamCmd) interactiveEdit(cfg api.StreamConfig) (api.StreamConfig, error) {
	cj, err := decoratedYamlMarshal(cfg)
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile, err := os.CreateTemp("", "*.yaml")
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}
	defer os.Remove(tfile.Name())

	_, err = fmt.Fprint(tfile, string(cj))
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile.Close()

	err = iu.EditFile(tfile.Name())
	if err != nil {
		return api.StreamConfig{}, err
	}

	nb, err := os.ReadFile(tfile.Name())
	if err != nil {
		return api.StreamConfig{}, err
	}

	ncfg := api.StreamConfig{}
	err = yaml.Unmarshal(nb, &ncfg)
	if err != nil {
		return api.StreamConfig{}, err
	}

	// some yaml quirks
	if len(ncfg.Sources) == 0 {
		ncfg.Sources = nil
	}
	if len(ncfg.Metadata) == 0 {
		ncfg.Metadata = nil
	}

	return ncfg, nil
}

func (c *streamCmd) editAction(cmd *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)
	c.setCreateFlagsState(cmd)

	c.connectAndAskStream()

	sourceStream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not request Stream %s configuration", c.stream)

	// lazy deep copy
	input := sourceStream.Configuration()
	input.Metadata = iu.RemoveReservedMetadata(input.Metadata)

	ij, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var cfg api.StreamConfig
	err = json.Unmarshal(ij, &cfg)
	if err != nil {
		return err
	}

	if c.interactive {
		cfg, err = c.interactiveEdit(cfg)
		fatalIfError(err, "could not create new configuration for Stream %s", c.stream)
	} else {
		cfg, err = c.copyAndEditStream(cfg)
		fatalIfError(err, "could not create new configuration for Stream %s", c.stream)
	}

	// sorts strings to subject lists that only differ in ordering is considered equal
	sorter := cmp.Transformer("Sort", func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	})

	diff := cmp.Diff(input, cfg, sorter)
	if diff == "" {
		if !c.dryRun {
			fmt.Println("No difference in configuration")
		}

		return nil
	}

	fmt.Printf("Differences (-old +new):\n%s", diff)
	if c.dryRun {
		os.Exit(1)
	}

	if jsm.IsKVBucketStream(c.stream) {
		err := c.kvAbstractionWarn(c.stream, "Really operate on the KV stream?")
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really edit Stream %s", c.stream), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	if cfg.AllowAtomicPublish || cfg.AllowMsgCounter {
		err := c.checkCompatibility(c.mgr, &cfg)
		if err != nil {
			return err
		}
	}

	err = sourceStream.UpdateConfiguration(cfg)
	fatalIfError(err, "could not edit Stream %s", c.stream)

	if !c.json {
		fmt.Printf("Stream %s was updated\n\n", c.stream)
	}

	return c.showStream(sourceStream)
}

func (c *streamCmd) cpAction(cmd *cobra.Command, args []string) error {
	c.stream = args[0]
	c.destination = args[1]
	c.setCreateFlagsState(cmd)

	if c.stream == c.destination {
		fatalf("source and destination Stream names cannot be the same")
	}

	c.connectAndAskStream()

	sourceStream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not request Stream %s configuration", c.stream)

	// lazy deep copy
	input := sourceStream.Configuration()
	ij, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var cfg api.StreamConfig
	err = json.Unmarshal(ij, &cfg)
	if err != nil {
		return err
	}

	cfg, err = c.copyAndEditStream(cfg)
	fatalIfError(err, "could not copy Stream %s", c.stream)

	cfg.Name = c.destination

	newStream, err := c.mgr.NewStreamFromDefault(cfg.Name, cfg)
	fatalIfError(err, "could not create Stream")

	if !c.json {
		fmt.Printf("Stream %s was created\n\n", cfg.Name)
	}

	c.showStream(newStream)

	return nil
}

func (c *streamCmd) showStreamConfig(cols *columns.Writer, cfg api.StreamConfig) {
	cols.AddRowIfNotEmpty("Description", cfg.Description)
	cols.AddRowIf("Subjects", cfg.Subjects, len(cfg.Subjects) > 0)
	cols.AddRow("Replicas", cfg.Replicas)
	cols.AddRowIf("Sealed", true, cfg.Sealed)
	cols.AddRow("Storage", cfg.Storage.String())
	cols.AddRowIf("Compression", cfg.Compression, cfg.Compression != api.NoCompression)
	cols.AddRowIf("Persistence Mode", cfg.PersistMode.String(), cfg.PersistMode != api.DefaultPersistMode)
	if cfg.FirstSeq > 0 {
		cols.AddRow("First Sequence", cfg.FirstSeq)
	}

	if cfg.Placement != nil {
		cols.AddRowIfNotEmpty("Placement Cluster", cfg.Placement.Cluster)
		cols.AddRowIf("Placement Tags", cfg.Placement.Tags, len(cfg.Placement.Tags) > 0)
	}

	cols.AddSectionTitle("Options")

	if cfg.SubjectTransform != nil && cfg.SubjectTransform.Destination != "" {
		source := cfg.SubjectTransform.Source
		if source == "" {
			source = ">"
		}
		cols.AddRowf("Subject Transform", "%s to %s", source, cfg.SubjectTransform.Destination)
	}

	if cfg.RePublish != nil {
		if cfg.RePublish.HeadersOnly {
			cols.AddRowf("Republishing Headers", "%s to %s", cfg.RePublish.Source, cfg.RePublish.Destination)
		} else {
			cols.AddRowf("Republishing", "%s to %s", cfg.RePublish.Source, cfg.RePublish.Destination)
		}
	}
	cols.AddRow("Retention", cfg.Retention.String())
	cols.AddRow("Acknowledgments", !cfg.NoAck)
	dnp := cfg.Discard.String()
	if cfg.DiscardNewPer {
		dnp = "New Per Subject"
	}
	cols.AddRow("Discard Policy", dnp)
	cols.AddRow("Duplicate Window", cfg.Duplicates)
	cols.AddRowIf("Direct Get", cfg.AllowDirect, cfg.AllowDirect)
	cols.AddRowIf("Mirror Direct Get", cfg.MirrorDirect, cfg.MirrorDirect)
	cols.AddRow("Allows Atomic Batch Publish", cfg.AllowAtomicPublish)
	cols.AddRow("Allows Fast Batch Publish", cfg.AllowBatchPublish)
	cols.AddRow("Allows Counters", cfg.AllowMsgCounter)
	cols.AddRow("Allows Msg Delete", !cfg.DenyDelete)
	cols.AddRow("Allows Per-Message TTL", cfg.AllowMsgTTL)
	cols.AddRow("Allows Purge", !cfg.DenyPurge)
	cols.AddRow("Allows Schedules", cfg.AllowMsgSchedules)
	if cfg.AllowMsgTTL && cfg.SubjectDeleteMarkerTTL > 0 {
		cols.AddRow("Subject Delete Markers TTL", cfg.SubjectDeleteMarkerTTL)
	}
	cols.AddRow("Allows Rollups", cfg.RollupAllowed)

	cols.AddSectionTitle("Limits")
	if cfg.MaxMsgs == -1 {
		cols.AddRow("Maximum Messages", "unlimited")
	} else {
		cols.AddRow("Maximum Messages", cfg.MaxMsgs)
	}
	if cfg.MaxMsgsPer <= 0 {
		cols.AddRow("Maximum Per Subject", "unlimited")
	} else {
		cols.AddRow("Maximum Per Subject", cfg.MaxMsgsPer)
	}
	if cfg.MaxBytes == -1 {
		cols.AddRow("Maximum Bytes", "unlimited")
	} else {
		cols.AddRow("Maximum Bytes", humanize.IBytes(uint64(cfg.MaxBytes)))
	}
	if cfg.MaxAge <= 0 {
		cols.AddRow("Maximum Age", "unlimited")
	} else {
		cols.AddRow("Maximum Age", cfg.MaxAge)
	}
	if cfg.MaxMsgSize == -1 {
		cols.AddRow("Maximum Message Size", "unlimited")
	} else {
		cols.AddRow("Maximum Message Size", humanize.IBytes(uint64(cfg.MaxMsgSize)))
	}
	if cfg.MaxConsumers == -1 {
		cols.AddRow("Maximum Consumers", "unlimited")
	} else {
		cols.AddRow("Maximum Consumers", cfg.MaxConsumers)
	}
	cols.AddRowIf("Consumer Inactive Threshold", cfg.ConsumerLimits.InactiveThreshold, cfg.ConsumerLimits.InactiveThreshold > 0)
	cols.AddRowIf("Consumer Max Ack Pending", cfg.ConsumerLimits.MaxAckPending, cfg.ConsumerLimits.MaxAckPending > 0)

	meta := iu.RemoveReservedMetadata(cfg.Metadata)
	if len(meta) > 0 {
		cols.AddSectionTitle("Metadata")
		cols.AddMapStrings(meta)
	}

	if cfg.Mirror != nil || len(cfg.Sources) > 0 {
		cols.AddSectionTitle("Replication")
	}

	cols.AddRowIfNotEmpty("Mirror", c.renderSource(cfg.Mirror))

	if len(cfg.Sources) > 0 {
		sort.Slice(cfg.Sources, func(i, j int) bool {
			return cfg.Sources[i].Name < cfg.Sources[j].Name
		})

		for i, source := range cfg.Sources {
			l := ""
			if i == 0 {
				l = "Sources"
			}

			cols.AddRow(l, c.renderSource(source))
		}
	}

	cols.Println()
}

func (c *streamCmd) renderSource(s *api.StreamSource) string {
	if s == nil {
		return ""
	}

	var parts []string
	parts = append(parts, s.Name)

	if s.OptStartSeq > 0 {
		parts = append(parts, fmt.Sprintf("Start Seq: %s", f(s.OptStartSeq)))
	}

	if s.OptStartTime != nil {
		parts = append(parts, fmt.Sprintf("Start Time: %v", s.OptStartTime))
	}

	if s.External != nil {
		if s.External.ApiPrefix != "" {
			parts = append(parts, fmt.Sprintf("API Prefix: %s", s.External.ApiPrefix))
		}

		if s.External.DeliverPrefix != "" {
			parts = append(parts, fmt.Sprintf("Delivery Prefix: %s", s.External.DeliverPrefix))
		}
	}

	if s.Consumer != nil {
		if s.Consumer.DeliverSubject == "" {
			parts = append(parts, fmt.Sprintf("Consumer Name: %s", s.Consumer.Name))
		} else {
			parts = append(parts, fmt.Sprintf("Consumer Name: %s (%s)", s.Consumer.Name, s.Consumer.DeliverSubject))
		}
	}

	return f(parts)
}

func (c *streamCmd) showStream(stream *jsm.Stream) error {
	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	c.showStreamInfo(info)

	return nil
}

func (c *streamCmd) showStreamInfo(info *api.StreamInfo) {
	if c.json {
		err := iu.PrintJSON(info)
		fatalIfError(err, "could not display info")
		return
	}

	var cols *columns.Writer
	if c.showStateOnly {
		cols = newColumnsf("State for Stream %s created %s", c.stream, f(info.Created.Local()))
	} else {
		cols = newColumnsf("Information for Stream %s created %s", c.stream, f(info.Created.Local()))
		c.showStreamConfig(cols, info.Config)
	}

	if info.Cluster != nil && info.Cluster.Name != "" {
		cols.AddSectionTitle("Cluster Information")
		if info.Cluster != nil && info.Cluster.Name != "" {
			cols.AddRow("Name", info.Cluster.Name)
			cols.AddRowIfNotEmpty("Cluster Group", info.Cluster.RaftGroup)
			if info.Cluster.LeaderSince == nil {
				cols.AddRow("Leader", info.Cluster.Leader)
			} else {
				cols.AddRowf("Leader", "%s (%s)", info.Cluster.Leader, f(sinceRefOrNow(info.TimeStamp, *info.Cluster.LeaderSince)))
			}

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
					state = append(state, fmt.Sprintf("%s operation behind", f(r.Lag)))
				}

				cols.AddRow("Replica", state)
			}
		}
		cols.Println()
	}

	showSource := func(s *api.StreamSourceInfo) {
		cols.AddRow("Stream Name", s.Name)

		switch {
		case s.FilterSubject != "":
			filter := ">"
			if s.FilterSubject != "" {
				filter = s.FilterSubject
			}

			cols.AddRow("Subject Filter", filter)
		case len(s.SubjectTransforms) > 0:
			for i := range s.SubjectTransforms {
				t := ""

				if i == 0 {
					if len(s.SubjectTransforms) > 1 {
						t = "Subject Filters and Transforms"
					} else {
						t = "Subject Filter and Transform"
					}
				}

				if s.SubjectTransforms[i].Destination == "" {
					cols.AddRowf(t, "%s untransformed", s.SubjectTransforms[i].Source)
				} else {
					cols.AddRowf(t, "%s to %s", s.SubjectTransforms[i].Source, s.SubjectTransforms[i].Destination)
				}
			}
		}

		cols.AddRow("Lag", s.Lag)

		if s.Active > 0 && s.Active < math.MaxInt64 {
			cols.AddRow("Last Seen", s.Active)
		} else {
			cols.AddRow("Last Seen", "never")
		}

		if s.External != nil {
			cols.AddRow("Ext. API Prefix", s.External.ApiPrefix)
			if s.External.DeliverPrefix != "" {
				cols.AddRow("Ext. Delivery Prefix", s.External.DeliverPrefix)
			}
		}

		if s.Error != nil {
			cols.AddRow("Error", s.Error.Description)
		}
	}

	if info.Mirror != nil {
		cols.AddSectionTitle("Mirror Information")
		showSource(info.Mirror)
	}

	if len(info.Sources) > 0 {
		cols.AddSectionTitle("Source Information")
		for _, s := range info.Sources {
			showSource(s)
			cols.Println()
		}
	}

	cols.AddSectionTitle("State")
	iu.RenderMetaApi(cols, info.Config.Metadata)
	cols.AddRow("Messages", info.State.Msgs)
	cols.AddRow("Bytes", humanize.IBytes(info.State.Bytes))

	if info.State.Lost != nil && len(info.State.Lost.Msgs) > 0 {
		cols.AddRowf("Lost Messages", "%s (%s)", f(len(info.State.Lost.Msgs)), humanize.IBytes(info.State.Lost.Bytes))
	}

	if info.State.FirstTime.Equal(time.Unix(0, 0)) || info.State.FirstTime.IsZero() {
		cols.AddRow("First Sequence", info.State.FirstSeq)
	} else {
		cols.AddRowf("First Sequence", "%s @ %s", f(info.State.FirstSeq), f(info.State.FirstTime))
	}

	if info.State.LastTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		cols.AddRow("Last Sequence", info.State.LastSeq)
	} else {
		cols.AddRowf("Last Sequence", "%s @ %s", f(info.State.LastSeq), f(info.State.LastTime))
	}

	if len(info.State.Deleted) > 0 { // backwards compat with older servers
		cols.AddRow("Deleted Messages", len(info.State.Deleted))
	} else if info.State.NumDeleted > 0 {
		cols.AddRow("Deleted Messages", info.State.NumDeleted)
	}

	cols.AddRow("Active Consumers", info.State.Consumers)

	if info.State.NumSubjects > 0 {
		cols.AddRow("Number of Subjects", info.State.NumSubjects)
	}

	if len(info.Alternates) > 0 {
		lName := 0
		lCluster := 0
		for _, s := range info.Alternates {
			if len(s.Name) > lName {
				lName = len(s.Name)
			}
			if len(s.Cluster) > lCluster {
				lCluster = len(s.Cluster)
			}
		}

		for i, s := range info.Alternates {
			msg := fmt.Sprintf("%s%s: Cluster: %s%s", strings.Repeat(" ", lName-len(s.Name)), s.Name, strings.Repeat(" ", lCluster-len(s.Cluster)), s.Cluster)
			if s.Domain != "" {
				msg = fmt.Sprintf("%s Domain: %s", msg, s.Domain)
			}

			if i == 0 {
				cols.AddRow("Alternates", msg)
			} else {
				cols.AddRow("", msg)
			}
		}
	}

	cols.Frender(os.Stdout)
}

func (c *streamCmd) stateAction(cmd *cobra.Command, args []string) error {
	c.showStateOnly = true
	return c.infoAction(cmd, args)
}

func (c *streamCmd) infoAction(_ *cobra.Command, args []string) error {
	c.stream = argValue(args, 0)

	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	fatalIfError(err, "could not show stream")

	fmt.Println()

	return nil
}

func (c *streamCmd) discardPolicyFromString() api.DiscardPolicy {
	switch strings.ToLower(c.discardPolicy) {
	case "new":
		return api.DiscardNew
	case "old":
		return api.DiscardOld
	default:
		fatalf("invalid discard policy %s", c.discardPolicy)
		return api.DiscardOld // unreachable
	}
}

func (c *streamCmd) storeTypeFromString(s string) api.StorageType {
	switch s {
	case "file", "f":
		return api.FileStorage
	case "memory", "m":
		return api.MemoryStorage
	default:
		fatalf("invalid storage type %s", c.storage)
		return api.MemoryStorage // unreachable
	}
}

func (c *streamCmd) retentionPolicyFromString() api.RetentionPolicy {
	switch strings.ToLower(c.retentionPolicyS) {
	case "limits":
		return api.LimitsPolicy
	case "interest":
		return api.InterestPolicy
	case "work queue", "workq", "work":
		return api.WorkQueuePolicy
	default:
		fatalf("invalid retention policy %s", c.retentionPolicyS)
		return api.LimitsPolicy // unreachable
	}
}

func (c *streamCmd) prepareConfig(requireSize bool) api.StreamConfig {
	var err error

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		fatalIfError(err, "invalid input")

		cfg.Metadata = iu.RemoveReservedMetadata(cfg.Metadata)

		if c.stream != "" {
			cfg.Name = c.stream
		}

		if c.stream == "" {
			c.stream = cfg.Name
		}

		if len(c.subjects) > 0 {
			cfg.Subjects = c.subjects
		}

		if len(c.placementTags) > 0 {
			if cfg.Placement == nil {
				cfg.Placement = &api.Placement{}
			}
			cfg.Placement.Tags = c.placementTags
		}

		if c.placementCluster != "" {
			if cfg.Placement == nil {
				cfg.Placement = &api.Placement{}
			}
			cfg.Placement.Cluster = c.placementCluster
		}

		if c.description != "" {
			cfg.Description = c.description
		}

		if c.replicas > 0 {
			cfg.Replicas = int(c.replicas)
		}

		return *cfg
	}

	if c.stream == "" {
		err = iu.AskOne(&survey.Input{
			Message: "Stream Name",
		}, &c.stream, survey.WithValidator(survey.Required))
		fatalIfError(err, "invalid input")
	}

	if c.mirror == "" && len(c.sources) == 0 {
		if len(c.subjects) == 0 {
			subjects := ""
			err = iu.AskOne(&survey.Input{
				Message: "Subjects",
				Help:    "Streams consume messages from subjects, this is a space or comma separated list that can include wildcards. Settable using --subjects",
			}, &subjects, survey.WithValidator(survey.Required))
			fatalIfError(err, "invalid input")

			c.subjects = iu.SplitString(subjects)
		}

		c.subjects = iu.SplitCLISubjects(c.subjects)
	}

	if c.mirror != "" && len(c.subjects) > 0 {
		fatalf("mirrors cannot listen for messages on subjects")
	}

	if c.acceptDefaults {
		if c.storage == "" {
			c.storage = "file"
		}
		if c.compression == "" {
			c.compression = "none"
		}
		if c.replicas == 0 {
			c.replicas = 1
		}
		if c.retentionPolicyS == "" {
			c.retentionPolicyS = "Limits"
		}
		if c.discardPolicy == "" {
			c.discardPolicy = "Old"
		}
		if c.maxMsgLimit == 0 {
			c.maxMsgLimit = -1
		}
		if c.maxMsgPerSubjectLimit == 0 {
			c.maxMsgPerSubjectLimit = -1
		}
		if c.maxBytesLimitString == "" {
			c.maxBytesLimit = -1
		}
		if requireSize && c.maxBytesLimitString == "" {
			c.maxBytesLimit = 256 * 1024 * 1024
		}
		if c.maxAgeLimit == "" {
			c.maxAgeLimit = "-1"
		}
		if c.maxMsgSizeString == "" {
			c.maxMsgSize = -1
		}
	}

	if c.storage == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Storage",
			Options: []string{"file", "memory"},
			Help:    "Streams are stored on the server, this can be one of many backends and all are usable in clustering mode. Settable using --storage",
		}, &c.storage, survey.WithValidator(survey.Required))
		fatalIfError(err, "invalid input")
	}

	storage := c.storeTypeFromString(c.storage)

	var compression api.Compression
	err = compression.UnmarshalJSON([]byte(fmt.Sprintf("%q", c.compression)))
	fatalIfError(err, "invalid compression algorithm")

	if c.replicas == 0 {
		c.replicas, err = askOneInt("Replication", "1", "When clustered, defines how many replicas of the data to store.  Settable using --replicas")
		fatalIfError(err, "invalid input")
	}
	if c.replicas <= 0 {
		fatalf("replicas should be >= 1")
	}

	if c.retentionPolicyS == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Retention Policy",
			Options: []string{"Limits", "Interest", "Work Queue"},
			Help:    "Messages are retained either based on limits like size and age (Limits), as long as there are Consumers (Interest) or until any worker processed them (Work Queue)",
			Default: "Limits",
		}, &c.retentionPolicyS, survey.WithValidator(survey.Required))
		fatalIfError(err, "invalid input")
	}

	if c.discardPolicy == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Discard Policy",
			Options: []string{"New", "Old"},
			Help:    "Once the Stream reaches its limits of size or messages, the New policy will prevent further messages from being added while Old will delete old messages.",
			Default: "Old",
		}, &c.discardPolicy, survey.WithValidator(survey.Required))
		fatalIfError(err, "invalid input")
	}

	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Stream Messages Limit", "-1", "Defines the amount of messages to keep in the store for this Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		fatalIfError(err, "invalid input")
		if c.maxMsgLimit <= 0 {
			c.maxMsgLimit = -1
		}
	}

	if c.maxMsgPerSubjectLimit == 0 && len(c.subjects) > 0 && (len(c.subjects) > 0 || strings.Contains(c.subjects[0], "*") || strings.Contains(c.subjects[0], ">")) {
		c.maxMsgPerSubjectLimit, err = askOneInt("Per Subject Messages Limit", "-1", "Defines the amount of messages to keep in the store for this Stream per unique subject, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs-per-subject")
		fatalIfError(err, "invalid input")
		if c.maxMsgPerSubjectLimit <= 0 {
			c.maxMsgPerSubjectLimit = -1
		}
	}

	var maxAge time.Duration

	if c.maxBytesLimit == 0 {
		reqd := ""
		defltSize := "-1"
		if requireSize {
			reqd = "MaxBytes is required per Account Settings"
			defltSize = "256MB"
		}

		c.maxBytesLimit, err = askOneBytes("Total Stream Size", defltSize, "Defines the combined size of all messages in a Stream, when exceeded messages are removed or new ones are rejected, -1 for unlimited. Settable using --max-bytes", reqd)
		fatalIfError(err, "invalid input")
	}

	if c.maxBytesLimit <= 0 {
		c.maxBytesLimit = -1
	}

	if c.maxAgeLimit == "" {
		err = iu.AskOne(&survey.Input{
			Message: "Message TTL",
			Default: "-1",
			Help:    "Defines the oldest messages that can be stored in the Stream, any messages older than this period will be removed, -1 for unlimited. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --max-age",
		}, &c.maxAgeLimit)
		fatalIfError(err, "invalid input")
	}

	if c.maxAgeLimit != "-1" {
		maxAge, err = parseDuration(c.maxAgeLimit)
		fatalIfError(err, "invalid maximum age limit format")
	}

	if c.maxMsgSize == 0 {
		c.maxMsgSize, err = askOneBytes("Max Message Size", "-1", "Defines the maximum size any single message may be to be accepted by the Stream. Settable using --max-msg-size", "")
		fatalIfError(err, "invalid input")
	}

	if c.maxMsgSize == 0 {
		c.maxMsgSize = -1
	}

	if c.maxMsgSize > math.MaxInt32 {
		fatalf("max value size %s is too big maximum is %s", f(c.maxMsgSize), f(math.MaxInt32))
	}

	var dupeWindow time.Duration

	if c.dupeWindow == "" && c.mirror == "" {
		defaultDW := (2 * time.Minute).String()
		if maxAge > 0 && maxAge < 2*time.Minute {
			defaultDW = maxAge.String()
		}

		if c.acceptDefaults {
			c.dupeWindow = defaultDW
		} else {
			err = iu.AskOne(&survey.Input{
				Message: "Duplicate tracking time window",
				Default: defaultDW,
				Help:    "Duplicate messages are identified by the Msg-Id headers and tracked within a window of this size. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --dupe-window",
			}, &c.dupeWindow)
			fatalIfError(err, "invalid input")
		}
	}

	if c.dupeWindow != "" {
		dupeWindow, err = parseDuration(c.dupeWindow)
		fatalIfError(err, "invalid duplicate window format")
	}

	if !c.acceptDefaults {
		if !c.allowRollupSet {
			c.allowRollup, err = askConfirmation("Allow message Roll-ups", false)
			fatalIfError(err, "invalid input")
		}

		if !c.denyDeleteSet {
			allow, err := askConfirmation("Allow message deletion", true)
			fatalIfError(err, "invalid input")
			c.denyDelete = !allow
		}

		if !c.denyPurgeSet {
			allow, err := askConfirmation("Allow purging subjects or the entire stream", true)
			fatalIfError(err, "invalid input")
			c.denyPurge = !allow
		}
	}

	cfg := api.StreamConfig{
		Name:                   c.stream,
		Description:            c.description,
		Subjects:               c.subjects,
		MaxMsgs:                c.maxMsgLimit,
		MaxMsgsPer:             c.maxMsgPerSubjectLimit,
		MaxBytes:               c.maxBytesLimit,
		MaxMsgSize:             int32(c.maxMsgSize),
		Duplicates:             dupeWindow,
		MaxAge:                 maxAge,
		Storage:                storage,
		Compression:            compression,
		FirstSeq:               c.firstSeq,
		NoAck:                  !c.ack,
		Retention:              c.retentionPolicyFromString(),
		Discard:                c.discardPolicyFromString(),
		MaxConsumers:           c.maxConsumers,
		Replicas:               int(c.replicas),
		RollupAllowed:          c.allowRollup,
		DenyPurge:              c.denyPurge,
		DenyDelete:             c.denyDelete,
		AllowDirect:            c.allowDirect,
		AllowMsgTTL:            c.allowMsgTTL,
		AllowAtomicPublish:     c.allowAtomicBatch,
		AllowBatchPublish:      c.allowFastBatch,
		AllowMsgCounter:        c.allowCounter,
		AllowMsgSchedules:      c.allowSchedules,
		SubjectDeleteMarkerTTL: c.subjectDeleteMarkerTTL,
		MirrorDirect:           c.allowMirrorDirect,
		DiscardNewPer:          c.discardPerSubj,
	}

	if c.limitInactiveThreshold > 0 {
		cfg.ConsumerLimits.InactiveThreshold = c.limitInactiveThreshold
	}

	if c.limitMaxAckPending > 0 {
		cfg.ConsumerLimits.MaxAckPending = c.limitMaxAckPending
	}

	if len(c.metadata) > 0 {
		cfg.Metadata = c.metadata
	}

	if c.placementCluster != "" || len(c.placementTags) > 0 {
		cfg.Placement = &api.Placement{
			Cluster: c.placementCluster,
			Tags:    c.placementTags,
		}
	}

	if c.mirror != "" {
		if iu.IsJsonObjectString(c.mirror) {
			cfg.Mirror, err = c.parseStreamSource(c.mirror)
			fatalIfError(err, "invalid mirror")
		} else {
			cfg.Mirror = c.askMirror()
		}
	}

	for _, source := range c.sources {
		if iu.IsJsonObjectString(source) {
			ss, err := c.parseStreamSource(source)
			fatalIfError(err, "invalid source")
			cfg.Sources = append(cfg.Sources, ss)
		} else {
			ss := c.askSource(source, fmt.Sprintf("%s Source", source))
			cfg.Sources = append(cfg.Sources, ss)
		}
	}

	c.checkRepubTransform()

	if c.repubSource != "" && c.repubDest != "" {
		cfg.RePublish = &api.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	if c.subjectTransformSource != "" && c.subjectTransformDest != "" {
		cfg.SubjectTransform = &api.SubjectTransformConfig{
			Source:      c.subjectTransformSource,
			Destination: c.subjectTransformDest,
		}
	}

	if c.persistMode == "async" {
		cfg.PersistMode = api.AsyncPersistMode
	}

	cfg.Metadata = iu.RemoveReservedMetadata(cfg.Metadata)

	return cfg
}

func (c *streamCmd) askMirror() *api.StreamSource {
	mirror := &api.StreamSource{Name: c.mirror}

	if c.acceptDefaults {
		return mirror
	}

	askDurable, err := askConfirmation("Configure a custom durable consumer", false)
	fatalIfError(err, "Could not request mirror details")
	if askDurable {
		mirror.Consumer = &api.StreamConsumerSource{}

		err = iu.AskOne(&survey.Input{
			Message: "Durable consumer name",
			Help:    "The name of the durable to read messages from",
		}, &mirror.Consumer.Name, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request mirror details")

		err = iu.AskOne(&survey.Input{
			Message: "Delivery subject",
			Help:    "The delivery subject for the consumer",
		}, &mirror.Consumer.DeliverSubject, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request mirror details")
	}

	if !askDurable {
		ok, err := askConfirmation("Adjust mirror start", false)
		fatalIfError(err, "Could not request mirror details")
		if ok {
			a, err := askOneInt("Mirror Start Sequence", "0", "Start mirroring at a specific sequence")
			fatalIfError(err, "Invalid sequence")
			mirror.OptStartSeq = uint64(a)

			if mirror.OptStartSeq == 0 {
				ts := ""
				err = iu.AskOne(&survey.Input{
					Message: "Mirror Start Time (YYYY:MM:DD HH:MM:SS)",
					Help:    "Start replicating as a specific time stamp in UTC time",
				}, &ts)
				fatalIfError(err, "could not request start time")
				if ts != "" {
					t, err := time.Parse("2006:01:02 15:04:05", ts)
					fatalIfError(err, "invalid time format")
					mirror.OptStartTime = &t
				}
			}
		}
	}

	ok, err := askConfirmation("Adjust mirror filter and transform", false)
	fatalIfError(err, "Could not request mirror details")

	if ok {
		var sources []string
		var destinations []string

		for {
			var source string
			var destination string

			err = iu.AskOne(&survey.Input{
				Message: "Filter mirror by subject (hit enter to finish)",
				Help:    "Only replicate data matching this subject",
			}, &source)
			fatalIfError(err, "could not request filter")

			if source == "" {
				break
			}

			err = iu.AskOne(&survey.Input{
				Message: "Subject transform destination",
				Help:    "Transform the subjects using this destination (hit enter for no transformation)",
			}, &destination)
			fatalIfError(err, "could not request transform destination")

			sources = append(sources, source)
			destinations = append(destinations, destination)
		}

		for i := range sources {
			mirror.SubjectTransforms = append(mirror.SubjectTransforms, api.SubjectTransformConfig{
				Source:      sources[i],
				Destination: destinations[i],
			})
		}
	}

	ok, err = askConfirmation("Import mirror from a different JetStream domain", false)
	fatalIfError(err, "Could not request mirror details")
	if ok {
		mirror.External = &api.ExternalStream{}
		domainName := ""
		err = iu.AskOne(&survey.Input{
			Message: "Foreign JetStream domain name",
			Help:    "The domain name from where to import the JetStream API",
		}, &domainName, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request mirror details")
		mirror.External.ApiPrefix = fmt.Sprintf("$JS.%s.API", domainName)

		if !askDurable {
			err = iu.AskOne(&survey.Input{
				Message: "Delivery prefix",
				Help:    "Optional prefix of the delivery subject",
			}, &mirror.External.DeliverPrefix)
			fatalIfError(err, "Could not request mirror details")
		}
	} else {
		ok, err = askConfirmation("Import mirror from a different account", false)
		fatalIfError(err, "Could not request mirror details")

		if ok {
			mirror.External = &api.ExternalStream{}
			err = iu.AskOne(&survey.Input{
				Message: "Foreign account API prefix",
				Help:    "The prefix where the foreign account JetStream API has been imported",
			}, &mirror.External.ApiPrefix, survey.WithValidator(survey.Required))
			fatalIfError(err, "Could not request mirror details")

			if !askDurable {
				err = iu.AskOne(&survey.Input{
					Message: "Foreign account delivery prefix",
					Help:    "The prefix where the foreign account JetStream delivery subjects has been imported",
				}, &mirror.External.DeliverPrefix, survey.WithValidator(survey.Required))
				fatalIfError(err, "Could not request mirror details")
			}
		}
	}

	return mirror
}

func (c *streamCmd) askSource(name string, prefix string) *api.StreamSource {
	cfg := &api.StreamSource{Name: name}

	if c.acceptDefaults {
		return cfg
	}

	askDurable, err := askConfirmation(fmt.Sprintf("Configure a custom durable consumer for %q", name), false)
	fatalIfError(err, "Could not request source details")
	if askDurable {
		cfg.Consumer = &api.StreamConsumerSource{}

		err = iu.AskOne(&survey.Input{
			Message: "Durable consumer name",
			Help:    "The name of the durable to read messages from",
		}, &cfg.Consumer.Name, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request source details")

		err = iu.AskOne(&survey.Input{
			Message: "Delivery subject",
			Help:    "The delivery subject for the consumer",
		}, &cfg.Consumer.DeliverSubject, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request source details")
	}

	if !askDurable {
		ok, err := askConfirmation(fmt.Sprintf("Adjust source %q start", name), false)
		fatalIfError(err, "Could not request source details")
		if ok {
			a, err := askOneInt(fmt.Sprintf("%s Start Sequence", prefix), "0", "Start mirroring at a specific sequence")
			fatalIfError(err, "Invalid sequence")
			cfg.OptStartSeq = uint64(a)

			ts := ""
			err = iu.AskOne(&survey.Input{
				Message: fmt.Sprintf("%s UTC Time Stamp (YYYY:MM:DD HH:MM:SS)", prefix),
				Help:    "Start replicating as a specific time stamp",
			}, &ts)
			fatalIfError(err, "could not request start time")
			if ts != "" {
				t, err := time.Parse("2006:01:02 15:04:05", ts)
				fatalIfError(err, "invalid time format")
				cfg.OptStartTime = &t
			}
		}
	}

	ok, err := askConfirmation(fmt.Sprintf("Adjust source %q filter and transform", name), false)
	fatalIfError(err, "Could not request source details")
	if ok {
		var sources []string
		var destinations []string
		for {
			var source string
			var destination string

			err = iu.AskOne(&survey.Input{
				Message: "Filter source by subject (hit enter to finish)",
				Help:    "Only replicate data matching this subject",
			}, &source)
			fatalIfError(err, "could not request filter")

			if source == "" {
				break
			}

			err = iu.AskOne(&survey.Input{
				Message: "Subject transform destination",
				Help:    "Transform the subjects using this destination (hit enter for no transformation)",
			}, &destination)
			fatalIfError(err, "could not request transform destination")

			sources = append(sources, source)
			destinations = append(destinations, destination)
		}

		for i := range sources {
			cfg.SubjectTransforms = append(cfg.SubjectTransforms, api.SubjectTransformConfig{
				Source:      sources[i],
				Destination: destinations[i],
			})
		}
	}

	ok, err = askConfirmation(fmt.Sprintf("Import %q from a different JetStream domain", name), false)
	fatalIfError(err, "Could not request source details")
	if ok {
		cfg.External = &api.ExternalStream{}
		domainName := ""
		err = iu.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s foreign JetStream domain name", prefix),
			Help:    "The domain name from where to import the JetStream API",
		}, &domainName, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request source details")
		cfg.External.ApiPrefix = fmt.Sprintf("$JS.%s.API", domainName)

		if !askDurable {
			err = iu.AskOne(&survey.Input{
				Message: fmt.Sprintf("%s foreign JetStream domain delivery prefix", prefix),
				Help:    "Optional prefix of the delivery subject",
			}, &cfg.External.DeliverPrefix)
			fatalIfError(err, "Could not request source details")
		}
	} else {
		ok, err = askConfirmation(fmt.Sprintf("Import %q from a different account", name), false)
		fatalIfError(err, "Could not request source details")
		if !ok {
			return cfg
		}

		cfg.External = &api.ExternalStream{}
		err = iu.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s foreign account API prefix", prefix),
			Help:    "The prefix where the foreign account JetStream API has been imported",
		}, &cfg.External.ApiPrefix, survey.WithValidator(survey.Required))
		fatalIfError(err, "Could not request source details")

		if !askDurable {
			err = iu.AskOne(&survey.Input{
				Message: fmt.Sprintf("%s foreign account delivery prefix", prefix),
				Help:    "The prefix where the foreign account JetStream delivery subjects has been imported",
			}, &cfg.External.DeliverPrefix, survey.WithValidator(survey.Required))
			fatalIfError(err, "Could not request source details")
		}
	}
	return cfg
}

func (c *streamCmd) parseStreamSource(source string) (*api.StreamSource, error) {
	ss := &api.StreamSource{}

	if iu.IsJsonObjectString(source) {
		err := json.Unmarshal([]byte(source), ss)
		if err != nil {
			return nil, err
		}

		if ss.Name == "" {
			return nil, fmt.Errorf("name is required")
		}
	} else {
		ss.Name = source
	}

	return ss, nil
}

func (c *streamCmd) validateCfg(cfg *api.StreamConfig) (bool, []byte, []string, error) {
	if os.Getenv("NOVALIDATE") != "" {
		return true, nil, nil, nil
	}

	j, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return false, nil, nil, err
	}

	if !cfg.NoAck {
		for _, subject := range cfg.Subjects {
			if subject == ">" {
				return false, j, []string{"subjects cannot be '>' when acknowledgement is enabled"}, nil
			}
		}
	}

	valid, errs := cfg.Validate(new(SchemaValidator))

	return valid, j, errs, nil
}

func (c *streamCmd) addAction(cmd *cobra.Command, args []string) (err error) {
	c.stream = argValue(args, 0)
	c.setCreateFlagsState(cmd)

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "could not create Stream")

	requireSize, _ := mgr.IsStreamMaxBytesRequired()

	cfg := c.prepareConfig(requireSize)

	switch {
	case c.validateOnly:
		valid, j, errs, err := c.validateCfg(&cfg)
		if err != nil {
			return err
		}

		fmt.Println(string(j))
		fmt.Println()
		if !valid {
			fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Printf("Configuration is a valid Stream matching %s\n", cfg.SchemaType())
		return nil

	case c.outFile != "":
		valid, j, errs, err := c.validateCfg(&cfg)
		fatalIfError(err, "Could not validate configuration")

		if !valid {
			fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		return os.WriteFile(c.outFile, j, 0600)
	}

	if cfg.AllowAtomicPublish || cfg.AllowMsgCounter {
		err := c.checkCompatibility(mgr, &cfg)
		if err != nil {
			return err
		}
	}

	str, err := mgr.NewStreamFromDefault(c.stream, cfg)
	fatalIfError(err, "could not create Stream")

	fmt.Printf("Stream %s was created\n\n", c.stream)

	c.showStream(str)

	return nil
}

func (c *streamCmd) checkCompatibility(mgr *jsm.Manager, cfg *api.StreamConfig) error {
	if cfg.AllowAtomicPublish {
		err := iu.RequireAPILevel(mgr, 2, "Atomic Batch Publishing requires NATS Server 2.12")
		if err != nil {
			return err
		}
	}

	if cfg.AllowMsgCounter {
		err := iu.RequireAPILevel(mgr, 2, "Distributed Counters requires NATS Server 2.12")
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *streamCmd) rmAction(_ *cobra.Command, args []string) (err error) {
	c.stream = argValue(args, 0)

	if c.force {
		if c.stream == "" {
			return fmt.Errorf("--force requires a stream name")
		}

		c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
		fatalIfError(err, "setup failed")

		err = c.mgr.DeleteStream(c.stream)
		if err != nil {
			if err == context.DeadlineExceeded {
				fmt.Println("Delete failed due to timeout, the stream might not exist or be in an unmanageable state")
			}
		}

		return err
	}

	c.connectAndAskStream()

	ok, err := askConfirmation(fmt.Sprintf("Really delete Stream %s", c.stream), false)
	fatalIfError(err, "could not obtain confirmation")

	if !ok {
		return nil
	}

	stream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not remove Stream")

	err = stream.Delete()
	fatalIfError(err, "could not remove Stream")

	return nil
}

func (c *streamCmd) purgeAction(_ *cobra.Command, args []string) (err error) {
	c.stream = argValue(args, 0)

	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really purge Stream %s", c.stream), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	if jsm.IsKVBucketStream(c.stream) {
		err := c.kvAbstractionWarn(c.stream, "Really operate on the KV stream?")
		if err != nil {
			return err
		}
	}

	stream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not purge Stream")

	var req *api.JSApiStreamPurgeRequest
	if c.purgeKeep > 0 || c.purgeSubject != "" || c.purgeSequence > 0 {
		if c.purgeSequence > 0 && c.purgeKeep > 0 {
			return fmt.Errorf("sequence and keep cannot be combined when purging")
		}

		req = &api.JSApiStreamPurgeRequest{
			Sequence: c.purgeSequence,
			Subject:  c.purgeSubject,
			Keep:     c.purgeKeep,
		}
	}

	resp, err := stream.PurgeExt(req)
	fatalIfError(err, "could not purge Stream")

	fmt.Printf("Purged %d messages from %s\n\n", resp.Purged, stream.Name())

	stream.Reset()

	c.showStateOnly = true
	return c.showStream(stream)
}

func (c *streamCmd) lsNames(mgr *jsm.Manager, filter *jsm.StreamNamesFilter) error {
	names, err := mgr.StreamNames(filter)
	if err != nil {
		return err
	}

	if c.json {
		err = iu.PrintJSON(names)
		fatalIfError(err, "could not display Streams")
		return nil
	}

	for _, n := range names {
		fmt.Println(n)
	}

	return nil
}

func (c *streamCmd) lsAction(_ *cobra.Command, _ []string) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	var filter *jsm.StreamNamesFilter
	if c.filterSubject != "" {
		filter = &jsm.StreamNamesFilter{Subject: c.filterSubject}
	}

	if c.listNames {
		return c.lsNames(mgr, filter)
	}

	var streams []*jsm.Stream
	var names []string

	skipped := false

	missing, offline, err := mgr.EachStream(filter, func(s *jsm.Stream) {
		if !c.showAll && s.IsInternal() {
			skipped = true
			return
		}

		streams = append(streams, s)
		names = append(names, s.Name())
	})
	if err != nil {
		return fmt.Errorf("could not list streams: %s", err)
	}

	if c.json {
		err = iu.PrintJSON(names)
		fatalIfError(err, "could not display Streams")
		return nil
	}

	if len(streams) == 0 && len(missing) == 0 && len(offline) == 0 && skipped {
		fmt.Println("No Streams defined, pass -a to include system streams")
		return nil
	} else if len(streams) == 0 && len(missing) == 0 && len(offline) == 0 {
		fmt.Println("No Streams defined")
		return nil
	}

	out, err := c.renderStreamsAsTable(streams, missing, offline)
	if err != nil {
		return err
	}

	fmt.Println(out)

	return nil
}

func (c *streamCmd) renderStreamsAsList(streams []*jsm.Stream, missing []string) string {
	var names []string
	for _, s := range streams {
		names = append(names, s.Name())
	}
	names = append(names, missing...)

	sort.Strings(names)

	return strings.Join(names, "\n")
}

func (c *streamCmd) renderStreamsAsTable(streams []*jsm.Stream, missing []string, offline map[string]string) (string, error) {
	sort.Slice(streams, func(i, j int) bool {
		info, _ := streams[i].LatestInformation()
		jnfo, _ := streams[j].LatestInformation()

		return info.State.Bytes < jnfo.State.Bytes
	})

	var out bytes.Buffer
	var table *iu.Table
	if c.filterSubject == "" {
		table = iu.NewTableWriter(opts(), "Streams")
	} else {
		table = iu.NewTableWriterf(opts(), "Streams matching %s", c.filterSubject)
	}

	table.AddHeaders("Name", "Description", "Created", "Messages", "Size", "Last Message")
	for _, s := range streams {
		nfo, _ := s.LatestInformation()
		table.AddRow(s.Name(), s.Description(), f(nfo.Created.Local()), f(nfo.State.Msgs), humanize.IBytes(nfo.State.Bytes), f(sinceRefOrNow(nfo.TimeStamp, nfo.State.LastTime)))
	}

	fmt.Fprintln(&out, table.Render())

	c.renderMissing(&out, missing, offline)

	return out.String(), nil
}

func (c *streamCmd) renderMissing(out io.Writer, missing []string, offline map[string]string) {
	toany := func(items []string) (res []any) {
		for _, i := range items {
			res = append(res, any(i))
		}
		return res
	}

	if len(missing) > 0 {
		fmt.Fprintln(out)
		sort.Strings(missing)
		table := iu.NewTableWriterf(opts(), "Inaccessible Streams")
		iu.SliceGroups(missing, 4, func(names []string) {
			table.AddRow(toany(names)...)
		})
		fmt.Fprint(out, table.Render())
	}

	if len(offline) > 0 {
		fmt.Fprintln(out)
		table := iu.NewTableWriterf(opts(), "Offline Streams")

		var keys []string
		for k := range offline {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			table.AddRow(k, offline[k])
		}
		fmt.Fprint(out, table.Render())
	}
}

func (c *streamCmd) rmMsgAction(_ *cobra.Command, args []string) (err error) {
	c.stream = argValue(args, 0)
	if v := argValue(args, 1); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid message sequence %q: %w", v, err)
		}
		c.msgID = id
	}

	c.connectAndAskStream()

	if c.msgID == -1 {
		id := ""
		err = iu.AskOne(&survey.Input{
			Message: "Message Sequence to remove",
		}, &id, survey.WithValidator(survey.Required))
		fatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		fatalIfError(err, "invalid number")

		if idint <= 0 {
			return fmt.Errorf("positive message ID required")
		}
		c.msgID = int64(idint)
	}

	stream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not load Stream %s", c.stream)

	if jsm.IsKVBucketStream(c.stream) {
		err := c.kvAbstractionWarn(c.stream, "Really operate on the KV stream?")
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove message %d from Stream %s", c.msgID, c.stream), false)
		fatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	return stream.DeleteMessageRequest(api.JSApiMsgDeleteRequest{Seq: uint64(c.msgID)})
}

func (c *streamCmd) getAction(_ *cobra.Command, args []string) (err error) {
	c.stream = argValue(args, 0)
	if v := argValue(args, 1); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid message sequence %q: %w", v, err)
		}
		c.msgID = id
	}

	c.connectAndAskStream()

	if c.msgID == -1 && c.filterSubject == "" {
		id := ""
		err = iu.AskOne(&survey.Input{
			Message: "Message Sequence to retrieve",
			Default: "-1",
		}, &id, survey.WithValidator(survey.Required))
		fatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		fatalIfError(err, "invalid number")

		c.msgID = int64(idint)

		if c.msgID == -1 {
			err = iu.AskOne(&survey.Input{
				Message: "Subject to retrieve last message for",
			}, &c.filterSubject)
			fatalIfError(err, "invalid subject")
		}
	}

	stream, err := c.loadStream(c.stream)
	fatalIfError(err, "could not load Stream %s", c.stream)

	var item *api.StoredMsg
	if c.msgID > -1 {
		item, err = stream.ReadMessage(uint64(c.msgID))
	} else if c.filterSubject != "" {
		item, err = stream.ReadLastMessageForSubject(c.filterSubject)
	} else {
		return fmt.Errorf("no ID or subject specified")
	}
	fatalIfError(err, "could not retrieve %s#%d", c.stream, c.msgID)

	if c.json {
		iu.PrintJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v (%s) on Subject %s\n\n", c.stream, item.Sequence, item.Time, f(time.Since(item.Time)), item.Subject)

	if len(item.Header) > 0 {
		fmt.Println("Headers:")
		hdrs, err := iu.DecodeHeadersMsg(item.Header)
		if err == nil {
			for k, vals := range hdrs {
				for _, val := range vals {
					fmt.Printf("  %s: %s\n", k, val)
				}
			}
		}
		fmt.Println()
	}
	outPutMSGBody(item.Data, c.vwTranslate, item.Subject, c.stream)
	return nil
}

func (c *streamCmd) connectAndAskStream() bool {
	var err error

	shouldAsk := c.stream == ""
	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	c.stream, c.selectedStream, err = selectStream(c.mgr, c.stream, c.force, c.showAll)
	fatalIfError(err, "could not pick a Stream to operate on")

	return shouldAsk
}

func (c *streamCmd) boolReverse(v bool) bool {
	if c.reportSortReverse {
		return !v
	}

	return v
}
