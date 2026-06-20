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
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go/jetstream"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/synadia-io/orbit.go/jetstreamext"

	"github.com/dustin/go-humanize"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"

	services "github.com/nats-io/nats.go/micro"

	"github.com/nats-io/natscli/internal/bench"

	"github.com/spf13/cobra"
)

type benchCmd struct {
	subject              string
	numClients           int
	numMsg               int
	msgSizeString        string
	msgSize              int
	csvFile              string
	progressBar          bool
	storage              string
	streamOrBucketName   string
	createStream         bool
	streamMaxBytesString string
	streamMaxBytes       int64
	ackMode              string
	doubleAck            bool
	batchSize            int
	maxOutstandingAcks   uint16
	replicas             int
	persistModeAsync     bool
	purge                bool
	sleep                time.Duration
	consumerName         string
	history              uint8
	fetchTimeout         bool
	disconnected         atomic.Bool
	errored              atomic.Bool
	lessThanExpected     atomic.Bool
	multiSubject         bool
	multiSubjectRandom   bool
	multiSubjectMax      int
	multisubjectFormat   string
	deDuplication        bool
	deDuplicationWindow  time.Duration
	ack                  bool
	randomize            int
	payloadFilename      string
	hdrs                 []string
	filterSubjects       []string // used by JS consumer commands
	filterSubject        string   // used by JS get command
	throughput           int
}

// rateThrottler throttles a message loop to approximately target messages/sec.
type rateThrottler struct {
	target float64
	start  time.Time
}

// newRateThrottler returns a throttler for the given target rate, or nil if
// the target is non-positive. Callers should nil-check before calling
// throttle so the disabled case skips the hot-loop call entirely.
func newRateThrottler(target float64) *rateThrottler {
	if target <= 0 {
		return nil
	}
	return &rateThrottler{target: target, start: time.Now()}
}

// throttle blocks as needed so that `sent` messages have been produced in at
// least sent/target seconds since the throttler was created.
func (r *rateThrottler) throttle(sent int) time.Duration {
	expected := float64(sent) / r.target
	elapsed := time.Since(r.start).Seconds()
	if expected > elapsed {
		throttle := time.Duration((expected - elapsed) * float64(time.Second))
		time.Sleep(throttle)
		return throttle
	}
	return 0
}

// perClientThroughput returns the per-client target rate (msgs/sec) given the
// aggregate --throughput setting. Returns 0 if disabled.
func (c *benchCmd) perClientThroughput() float64 {
	if c.throughput <= 0 || c.numClients <= 0 {
		return 0
	}
	return float64(c.throughput) / float64(c.numClients)
}

func configureBenchCommand(app commandHost) {
	c := &benchCmd{}

	addCommonFlags := func(f *cobra.Command) {
		cmdAddTags(f, "scope:user", "impact:rw")
		f.Flags().IntVar(&c.numClients, "clients", 1, "Number of concurrent clients")
		f.Flags().IntVar(&c.numMsg, "msgs", 100000, "Number of messages to publish or subscribe to")
		negatableBoolVar(f, &c.progressBar, "progress", true, "Enable or disable the progress bar")
		f.Flags().StringVar(&c.csvFile, "csv", "", "Save benchmark data to CSV file")
		f.Flags().StringVar(&c.msgSizeString, "size", "128B", "Size of the test messages")
		// TODO: support randomized payload data
	}

	addPubFlags := func(f *cobra.Command) {
		f.Flags().BoolVar(&c.multiSubject, "multisubject", false, "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token")
		f.Flags().IntVar(&c.multiSubjectMax, "multisubjectmax", 100000, "The maximum number of subjects to use in multi-subject mode (0 means no max)")
		f.Flags().BoolVar(&c.multiSubjectRandom, "multisubjectrandomize", false, "Randomize which subjects are being used when in multisubject mode")
		f.Flags().Var(newExistingFileValue(&c.payloadFilename), "payload", "File containing a message payload to send")
		f.Flags().StringArrayVarP(&c.hdrs, "header", "H", nil, "Adds headers to the message using K:V format")
	}

	addThroughputFlag := func(f *cobra.Command) {
		f.Flags().IntVar(&c.throughput, "throughput", 0, "If set > 0, throttle aggregate message send throughput to approximately THROUGHPUT messages/second across all clients (0 disables). If --sleep is set, the achieved rate may be lower")
		flagPlaceholder(f, "throughput", "THROUGHPUT")
	}

	addJSCommonFlags := func(f *cobra.Command) {
		f.Flags().StringVar(&c.streamOrBucketName, "stream", bench.DefaultStreamName, "The name of the stream to create or use")
		f.Flags().DurationVar(&c.sleep, "sleep", 0*time.Second, "Sleep for the specified interval between publications")
		flagPlaceholder(f, "sleep", "DURATION")
	}

	addJSConsumerFlags := func(f *cobra.Command) {
		f.Flags().StringVar(&c.consumerName, "consumer", bench.DefaultDurableConsumerName, "Specify the durable consumer name to use")
		f.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the max number of messages that can be buffered in the client")
		f.Flags().Var(newEnumValue(&c.ackMode, bench.AckModeExplicit, bench.AckModeExplicit, bench.AckModeNone, bench.AckModeAll), "acks", "Acknowledgement mode for the consumer")
		negatableBoolVar(f, &c.doubleAck, "doubleack", false, "Synchronously acknowledge messages, waiting for a reply from the server")
		f.Flags().StringArrayVar(&c.filterSubjects, "filter", nil, "Filter Stream by subjects")
		flagPlaceholder(f, "filter", "SUBJECTS")
		f.Flags().BoolVar(&c.purge, "purge", false, "Purge the stream before running")
	}

	addJSPubFlags := func(f *cobra.Command) {
		f.Flags().BoolVar(&c.createStream, "create", false, "Create or update the stream first")
		f.Flags().Var(newEnumValue(&c.storage, "file", "memory", "file"), "storage", "JetStream storage (memory/file) for the \"benchstream\" stream")
		f.Flags().IntVar(&c.replicas, "replicas", 1, "Number of replicas for the \"benchstream\" stream")
		f.Flags().StringVar(&c.streamMaxBytesString, "maxbytes", "1GB", "The maximum size of the stream or KV bucket in bytes")
		f.Flags().BoolVar(&c.deDuplication, "dedup", false, "Sets a message id in the header to use JS Publish de-duplication")
		f.Flags().DurationVar(&c.deDuplicationWindow, "dedupwindow", 2*time.Minute, "Sets the duration of the stream's deduplication functionality")
		f.Flags().BoolVar(&c.purge, "purge", false, "Purge the stream before running")
		f.Flags().BoolVar(&c.persistModeAsync, "persistasync", false, "Set the persistence mode for the steam to asynchronous (only for R1 streams)")
	}

	addKVPutFlags := func(f *cobra.Command) {
		f.Flags().Var(newEnumValue(&c.storage, "file", "memory", "file"), "storage", "JetStream storage (memory/file) for the \"benchstream\" bucket")
		f.Flags().IntVar(&c.replicas, "replicas", 1, "Number of replicas for the \"benchstream\" bucket")
		f.Flags().StringVar(&c.streamMaxBytesString, "maxbytes", "1GB", "The maximum size of the stream or KV bucket in bytes")
		f.Flags().Uint8Var(&c.history, "history", 1, "History depth for the bucket in KV mode")
		f.Flags().BoolVar(&c.purge, "purge", false, "Purge the stream before running")
		f.Flags().IntVar(&c.randomize, "randomize", 0, "Randomly put messages using keys between 0 and this number (set to 0 for sequential access)")
	}

	benchCommand := addCommand(app, "bench", "Benchmark utility")
	addCheat("bench", benchCommand)

	//benchCommand.Long = benchHelp

	corePub := addCommand(benchCommand, "pub", "Publish Core NATS messages")
	corePub.RunE = c.pubAction
	addArg(corePub, "subject", "Subject to use for the benchmark", true, "string")
	corePub.Flags().DurationVar(&c.sleep, "sleep", 0*time.Second, "Sleep for the specified interval between publications")
	flagPlaceholder(corePub, "sleep", "DURATION")
	addCommonFlags(corePub)
	addPubFlags(corePub)
	addThroughputFlag(corePub)

	coreSub := addCommand(benchCommand, "sub", "Subscribe to Core NATS messages")
	coreSub.RunE = c.subAction
	addArg(coreSub, "subject", "Subject to use for the benchmark", true, "string")
	coreSub.Flags().BoolVar(&c.multiSubject, "multisubject", false, "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token")
	addCommonFlags(coreSub)

	microService := addCommand(benchCommand, "service", "Micro-service mode")
	microService.Flags().DurationVar(&c.sleep, "sleep", 0*time.Second, "Sleep for the specified interval between requests or before replying to the request")
	flagPlaceholder(microService, "sleep", "DURATION")
	addCommonFlags(microService)

	request := addCommand(microService, "request", "Send a request and wait for its reply")
	request.RunE = c.requestAction
	request.Long = "Send a request and wait for a reply"
	addArg(request, "subject", "Subject to use for the benchmark", true, "string")
	request.Flags().Var(newExistingFileValue(&c.payloadFilename), "payload", "File containing the payload to send")
	request.Flags().StringArrayVarP(&c.hdrs, "header", "H", nil, "Adds headers to the message using K:V format")
	addThroughputFlag(request)
	// TODO: support randomized payload data

	reply := addCommand(microService, "serve", "Service requests")
	reply.RunE = c.serveAction
	addArg(reply, "subject", "Subject to use for the benchmark", true, "string")

	jsCommand := addCommand(benchCommand, "js", "JetStream benchmark commands")
	addCommonFlags(jsCommand)
	addJSCommonFlags(jsCommand)

	jspub := addCommand(jsCommand, "pub", "Publish JetStream messages")
	jssyncpub := addCommand(jspub, "sync", "Use synchronous JetStream publish")
	jssyncpub.RunE = c.jspubSyncAction
	addArg(jssyncpub, "subject", "Subject to use for the benchmark", true, "string")
	addPubFlags(jssyncpub)
	addJSPubFlags(jssyncpub)
	addThroughputFlag(jssyncpub)

	jsasyncpub := addCommand(jspub, "async", "Use asynchronous JetStream publish")
	jsasyncpub.RunE = c.jspubAsyncAction
	addArg(jsasyncpub, "subject", "Subject to use for the benchmark", true, "string")
	jsasyncpub.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the number of asynchronous operations per batch")
	addPubFlags(jsasyncpub)
	addJSPubFlags(jsasyncpub)
	addThroughputFlag(jsasyncpub)

	jsbatchatomicpub := addCommand(jspub, "atomic", "Use atomic batch JetStream publish")
	jsbatchatomicpub.Aliases = []string{"batch"}
	jsbatchatomicpub.RunE = c.jspubBatchAtomicAction
	addArg(jsbatchatomicpub, "subject", "Subject to use for the benchmark", true, "string")
	jsbatchatomicpub.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the size of the batches")
	addPubFlags(jsbatchatomicpub)
	addJSPubFlags(jsbatchatomicpub)
	addThroughputFlag(jsbatchatomicpub)

	jsbatchfastpub := addCommand(jspub, "fast", "Use fast batch JetStream publish")
	jsbatchfastpub.RunE = c.jspubBatchFastAction
	addArg(jsbatchfastpub, "subject", "Subject to use for the benchmark", true, "string")
	jsbatchfastpub.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the size of the batches")
	jsbatchfastpub.Flags().Uint16Var(&c.maxOutstandingAcks, "max-outstanding-acks", 1, "Sets the max outstanding acks for fast publishing")
	addPubFlags(jsbatchfastpub)
	addJSPubFlags(jsbatchfastpub)
	addThroughputFlag(jsbatchfastpub)

	jsOrdered := addCommand(jsCommand, "ordered", "Consume JetStream messages from a consumer using an ephemeral ordered consumer")
	jsOrdered.RunE = c.jsOrderedAction
	jsOrdered.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the max number of messages that can be buffered in the client")
	jsOrdered.Flags().BoolVar(&c.purge, "purge", false, "Purge the stream before running")
	jsOrdered.Flags().StringArrayVar(&c.filterSubjects, "filter", nil, "Filter Stream by subjects")
	flagPlaceholder(jsOrdered, "filter", "SUBJECTS")

	jsConsume := addCommand(jsCommand, "consume", "Consume JetStream messages from a durable consumer using a callback")
	jsConsume.RunE = c.jsConsumeAction
	addJSConsumerFlags(jsConsume)

	jsFetch := addCommand(jsCommand, "fetch", "Consume JetStream messages from a durable consumer using fetch")
	jsFetch.RunE = c.jsFetchAction
	addJSConsumerFlags(jsFetch)

	jsGet := addCommand(jsCommand, "get", "Retrieve messages from JetStream using gets")
	jsGetSync := addCommand(jsGet, "sync", "Use synchronous JetStream get")
	jsGetSync.RunE = c.jsSyncGetAction
	jsGetBatchedDirect := addCommand(jsGet, "batch", "Use batched JetStream direct get")
	jsGetBatchedDirect.RunE = c.jsBatchedDirectAction
	jsGetBatchedDirect.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the max number of messages that can be buffered in the client")
	jsGetBatchedDirect.Flags().StringVar(&c.filterSubject, "filter", ">", "Filter for the messages")

	kvCommand := addCommand(benchCommand, "kv", "KV benchmark operations")
	addCommonFlags(kvCommand)
	kvCommand.Flags().StringVar(&c.streamOrBucketName, "bucket", bench.DefaultBucketName, "The bucket to use for the benchmark")
	kvCommand.Flags().DurationVar(&c.sleep, "sleep", 0*time.Second, "Sleep for the specified interval after putting each message")
	flagPlaceholder(kvCommand, "sleep", "DURATION")

	kvput := addCommand(kvCommand, "put", "Put messages in a KV bucket")
	kvput.RunE = c.kvPutAction
	// TODO: support randomized payload data
	addKVPutFlags(kvput)
	addThroughputFlag(kvput)

	kvget := addCommand(kvCommand, "get", "Get messages from a KV bucket")
	kvget.RunE = c.kvGetAction
	kvget.Flags().IntVar(&c.randomize, "randomize", 0, "Randomly get messages using keys between 0 and this number (set to 0 for sequential access)")

	oldJSCommand := addCommand(benchCommand, "oldjs", "JetStream benchmark commands using the old JS API")
	oldJSCommand.Hidden = true
	addCommonFlags(oldJSCommand)
	addJSCommonFlags(oldJSCommand)

	oldJSOrdered := addCommand(oldJSCommand, "ordered", "Consume JetStream messages from a consumer using an old JS API's ephemeral ordered consumer")
	oldJSOrdered.RunE = c.oldjsOrderedAction
	addArg(oldJSOrdered, "subject", "Subject to use for the benchmark", true, "string")
	oldJSOrdered.Flags().BoolVar(&c.multiSubject, "multisubject", false, "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token")

	oldJSPush := addCommand(oldJSCommand, "push", "Consume JetStream messages from a consumer using an old JS API's durable push consumer")
	oldJSPush.RunE = c.oldjsPushAction
	addArg(oldJSPush, "subject", "Subject to use for the benchmark", true, "string")
	oldJSPush.Flags().StringVar(&c.consumerName, "consumer", bench.DefaultDurableConsumerName, "Specify the durable consumer name to use")
	oldJSPush.Flags().IntVar(&c.batchSize, "maxacks", 500, "Sets the max ack pending value, adjusts for the number of clients")
	negatableBoolVar(oldJSPush, &c.ack, "ack", true, "Uses explicit message acknowledgement or not for the consumer")
	negatableBoolVar(oldJSPush, &c.doubleAck, "doubleack", false, "Synchronously acknowledge messages, waiting for a reply from the server")

	oldJSPull := addCommand(oldJSCommand, "pull", "Consume JetStream messages from a consumer using an old JS API's durable pull consumer")
	oldJSPull.RunE = c.oldjsPullAction
	addArg(oldJSPull, "subject", "Subject to use for the benchmark", true, "string")
	oldJSPull.Flags().StringVar(&c.consumerName, "consumer", bench.DefaultDurableConsumerName, "Specify the durable consumer name to use")
	oldJSPull.Flags().IntVar(&c.batchSize, "batch", 500, "Sets the fetch size for the consumer")
	negatableBoolVar(oldJSPull, &c.ack, "ack", true, "Uses explicit message acknowledgement or not for the consumer")
	negatableBoolVar(oldJSPull, &c.doubleAck, "doubleack", false, "Synchronously acknowledge messages, waiting for a reply from the server")

}

func init() {
	registerCommand("bench", 2, configureBenchCommand)
}

func (c *benchCmd) disconnectionHandler(_ *nats.Conn, err error) {
	c.disconnected.Store(true)

	if err != nil {
		log.Printf("Disconnected due to: %v, will attempt reconnect\n", err)
	}
}

func (c *benchCmd) errorHandler(_ *nats.Conn, _ *nats.Subscription, err error) {
	c.errored.Store(true)

	if err != nil {
		log.Printf("Async connection error received: %v\n", err)
	}
}

func (c *benchCmd) getJS(nc *nats.Conn) (jetstream.JetStream, error) {
	var err error
	var js jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js, err = jetstream.New(nc)
	}
	if err != nil {
		return nil, fmt.Errorf("getting the new API JetStream instance: %w", err)
	}

	return js, nil
}

func (c *benchCmd) offset(putter int, counts []int) int {
	var position = 0

	for i := 0; i < putter; i++ {
		position = position + counts[i]
	}
	return position
}

func (c *benchCmd) processActionArgs() error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	// for pubs/request/and put only
	if c.msgSizeString != "" {
		msgSize, err := iu.ParseStringAsBytes(c.msgSizeString, 32)
		if err != nil || msgSize <= 0 || msgSize > math.MaxInt {
			return fmt.Errorf("can not parse or invalid the value specified for the message size: %s", c.msgSizeString)
		} else {
			c.msgSize = int(msgSize)
		}
	}

	if opts().Config == nil {
		return fmt.Errorf("unknown context %q", opts().CfgCtx)
	}

	if c.streamMaxBytesString != "" {
		size, err := iu.ParseStringAsBytes(c.streamMaxBytesString, 64)
		if err != nil || size <= 0 {
			return fmt.Errorf("can not parse or invalid the value specified for the max stream/bucket size: %s", c.streamMaxBytesString)
		}

		c.streamMaxBytes = size
	}

	return nil
}

func (c *benchCmd) generateBanner(benchType string) string {
	// Create the banner which includes the appropriate argument names and values for the type of benchmark being run
	type nvp struct {
		name  string
		value string
	}

	var argnvps []nvp

	streamOrBucketAttribues := func() {
		if c.createStream {
			argnvps = append(argnvps, nvp{"storage", c.storage})
			argnvps = append(argnvps, nvp{"max-bytes", f(uint64(c.streamMaxBytes))})
			argnvps = append(argnvps, nvp{"replicas", f(c.replicas)})
			argnvps = append(argnvps, nvp{"deduplication", f(c.deDuplication)})
			argnvps = append(argnvps, nvp{"dedup-window", f(c.deDuplicationWindow)})
		}
	}

	jsAttributes := func() {
		argnvps = append(argnvps, nvp{"stream", f(c.streamOrBucketName)})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	}

	benchTypeLabel := bench.GetBenchTypeLabel(benchType)

	switch benchType {
	case bench.TypeCorePub:
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"multi-subject-randomize", f(c.multiSubjectRandom)})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case bench.TypeCoreSub:
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
	case bench.TypeServiceRequest:
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case bench.TypeServiceServe:
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case bench.TypeJSPubSync:
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"multi-subject-randomize", f(c.multiSubjectRandom)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		streamOrBucketAttribues()
	case bench.TypeJSPubAsync, bench.TypeJSPubBatchAtomic, bench.TypeJSPubBatchFast:
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"multi-subject-randomize", f(c.multiSubjectRandom)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		streamOrBucketAttribues()
	case bench.TypeJSOrdered:
		jsAttributes()
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		if len(c.filterSubjects) > 0 {
			argnvps = append(argnvps, nvp{"filter", strings.Join(c.filterSubjects, ",")})
		}
		streamOrBucketAttribues()
	case bench.TypeJSConsume, bench.TypeJSFetch:
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acks", c.ackMode})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		if len(c.filterSubjects) > 0 {
			argnvps = append(argnvps, nvp{"filter", strings.Join(c.filterSubjects, ",")})
		}
		jsAttributes()
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		streamOrBucketAttribues()
	case bench.TypeJSGetSync:
		jsAttributes()
	case bench.TypeJSGetDirectBatched:
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		argnvps = append(argnvps, nvp{"filter", c.filterSubject})
		jsAttributes()
	case bench.TypeKVPut:
		argnvps = append(argnvps, nvp{"bucket", c.streamOrBucketName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		streamOrBucketAttribues()
	case bench.TypeKVGet:
		argnvps = append(argnvps, nvp{"bucket", c.streamOrBucketName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"randomize", f(c.randomize)})
		streamOrBucketAttribues()
	case bench.TypeOldJSOrdered:
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		jsAttributes()
		streamOrBucketAttribues()
	case bench.TypeOldJSPush:
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acked", fmt.Sprintf("%v", c.ack)})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		jsAttributes()
		streamOrBucketAttribues()
	case bench.TypeOldJSPull:
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acked", fmt.Sprintf("%v", c.ack)})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		streamOrBucketAttribues()
	}

	argnvps = append(argnvps, nvp{"msgs", f(c.numMsg)})
	argnvps = append(argnvps, nvp{"msg-size", humanize.IBytes(uint64(c.msgSize))})
	argnvps = append(argnvps, nvp{"clients", f(c.numClients)})

	if c.throughput > 0 {
		switch benchType {
		case bench.TypeCorePub, bench.TypeServiceRequest,
			bench.TypeJSPubSync, bench.TypeJSPubAsync, bench.TypeJSPubBatchAtomic, bench.TypeJSPubBatchFast,
			bench.TypeKVPut:
			argnvps = append(argnvps, nvp{"throughput", f(c.throughput)})
		}
	}

	banner := fmt.Sprintf("Starting %s benchmark [", benchTypeLabel)

	var joinBuffer []string

	sort.Slice(argnvps, func(i, j int) bool {
		return argnvps[i].name < argnvps[j].name
	})

	for _, v := range argnvps {
		joinBuffer = append(joinBuffer, v.name+"="+v.value)
	}

	banner += strings.Join(joinBuffer, ", ") + "]"

	return banner
}

func (c *benchCmd) printResults(bm *bench.BenchmarkResults) error {
	if c.progressBar {
		uiprogress.Stop()
	}

	if c.fetchTimeout {
		log.Println("WARNING: at least one of the pull consumer Fetch operation timed out. These results are not optimal!")
	}

	if c.lessThanExpected.Load() {
		log.Println("WARNING: at least one of the clients got less than the requested number of messages in a batch get. These results may not be optimal!")
	}

	if c.disconnected.Load() || c.errored.Load() {
		log.Println("WARNING: at least one of the clients disconnected or experienced an error during the benchmark. These results are not optimal!")
	}

	fmt.Println()
	fmt.Println(bm.Report())

	if c.csvFile != "" {
		csvData := bm.CSV()
		err := os.WriteFile(c.csvFile, []byte(csvData), 0600)
		if err != nil {
			return fmt.Errorf("writing file %s: %w", c.csvFile, err)
		}
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}

func (c *benchCmd) getSubscribeSubject() string {
	if c.multiSubject {
		return c.subject + ".*"
	} else {
		return c.subject
	}
}

func (c *benchCmd) getPublishSubject(number int) string {
	if c.multiSubject {
		if c.multiSubjectMax == 0 {
			return c.subject + "." + strconv.Itoa(number)
		} else {
			if c.multiSubjectRandom {
				return c.subject + "." + fmt.Sprintf(c.multisubjectFormat, rand.IntN(c.multiSubjectMax))
			} else {
				return c.subject + "." + fmt.Sprintf(c.multisubjectFormat, number%c.multiSubjectMax)
			}
		}
	} else {
		return c.subject
	}
}

func (c *benchCmd) storageType() jetstream.StorageType {
	if c.storage == "memory" {
		return jetstream.MemoryStorage
	} else {
		return jetstream.FileStorage
	}
}

func (c *benchCmd) createOrUpdateConsumer(js jetstream.JetStream) error {
	var ack jetstream.AckPolicy

	switch c.ackMode {
	case bench.AckModeNone:
		ack = jetstream.AckNonePolicy
	case bench.AckModeAll:
		ack = jetstream.AckAllPolicy
	case bench.AckModeExplicit:
		ack = jetstream.AckExplicitPolicy
	}

	_, err := js.CreateOrUpdateConsumer(ctx, c.streamOrBucketName, jetstream.ConsumerConfig{
		Durable:           c.consumerName,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         ack,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxAckPending:     c.batchSize * c.numClients,
		InactiveThreshold: time.Second * 10,
		FilterSubjects:    c.filterSubjects,
	})
	if err != nil {
		return fmt.Errorf("creating the durable consumer '%s': %w", c.consumerName, err)
	}

	return nil
}

func (c *benchCmd) purgeStream() error {
	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	s, err := js.Stream(ctx, c.streamOrBucketName)
	if err != nil {
		return fmt.Errorf("getting stream '%s': %w", c.streamOrBucketName, err)
	}

	err = s.Purge(ctx)
	if err != nil {
		return fmt.Errorf("purging stream '%s': %w", c.streamOrBucketName, err)
	}

	return nil
}

// Actions for the various bench commands below
func (c *benchCmd) pubAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	err := c.processActionArgs()
	if err != nil {
		return err
	}

	// catch the number of clients being more than number of messages
	if c.numClients > c.numMsg {
		c.numClients = c.numMsg
	}

	banner := c.generateBanner(bench.TypeCorePub)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", bench.TypeCorePub, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	pubCounts := msgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return err
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runCorePublisher(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], c.offset(i, pubCounts), i)
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) subAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeCoreSub)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", bench.TypeCoreSub, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d failed to connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreSubscriber(bm, errChan, nc, startwg, donewg, c.numMsg, i)
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) requestAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	err := c.processActionArgs()
	if err != nil {
		return err
	}

	// catch the number of clients being more than number of messages
	if c.numClients > c.numMsg {
		c.numClients = c.numMsg
	}

	banner := c.generateBanner(bench.TypeServiceRequest)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", bench.TypeServiceRequest, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	pubCounts := msgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d failed to connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreRequester(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], c.offset(i, pubCounts), i)
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) serveAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	// reply mode is open-ended for the number of messages
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeServiceServe)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", bench.TypeServiceServe, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d failed to connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runServiceServer(nc, errChan, startwg, donewg, i)
	}

	startwg.Wait()
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jspubSyncAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]
	return c.jspubActions(bench.TypeJSPubSync)
}

func (c *benchCmd) jspubAsyncAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]
	return c.jspubActions(bench.TypeJSPubAsync)
}

func (c *benchCmd) jspubBatchAtomicAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]
	return c.jspubActions(bench.TypeJSPubBatchAtomic)
}

func (c *benchCmd) jspubBatchFastAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]
	if c.maxOutstandingAcks == 0 {
		return fmt.Errorf("--max-outstanding-acks must be >= 1")
	}
	return c.jspubActions(bench.TypeJSPubBatchFast)
}

func (c *benchCmd) jspubActions(jsPubType string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	// catch the number of clients being more than number of messages
	if c.numClients > c.numMsg {
		c.numClients = c.numMsg
	}

	if c.persistModeAsync && c.replicas != 1 && jsPubType == bench.TypeJSPubBatchAtomic {
		return fmt.Errorf("async persist mode is only supported for streams with 1 replica and incompatible with atomic batch publishing")
	}

	banner := c.generateBanner(jsPubType)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", jsPubType, c.numClients)
	benchId := strconv.FormatInt(time.Now().UnixMilli(), 16)
	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	// create the stream or purge it for the benchmark if so requested
	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return err
	}

	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	myjsm, err := jsm.New(nc)
	if err != nil {
		return err
	}

	if jsPubType == bench.TypeJSPubBatchAtomic {
		err = iu.RequireAPILevel(myjsm, 2, "Atomic Batch Publishing requires NATS Server 2.12, specify --async for async publishing instead")
		if err != nil {
			return err
		}
	}
	if jsPubType == bench.TypeJSPubBatchFast {
		err = iu.RequireAPILevel(myjsm, 4, "Fast Batch Publishing requires NATS Server 2.14, specify --async for async publishing instead")
		if err != nil {
			return err
		}
		if c.batchSize > math.MaxUint16 {
			log.Printf("WARNING: --batch %d exceeds the fast publisher flow window maximum of %d; capping at %d", c.batchSize, math.MaxUint16, math.MaxUint16)
			c.batchSize = math.MaxUint16
		}
	}

	var s jetstream.Stream

	if c.createStream {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		// uses the jsm library since it's updated with the new 2.12+ stream features and nats.go doesn't yet support them
		atomicBatch := true
		persistMode := api.DefaultPersistMode
		storage := api.MemoryStorage

		if c.storageType() == jetstream.FileStorage {
			storage = api.FileStorage
		}

		if c.replicas == 1 && jsPubType != bench.TypeJSPubBatchAtomic && c.persistModeAsync {
			persistMode = api.AsyncPersistMode
			atomicBatch = false
		}

		_, err = myjsm.NewStreamFromDefault(c.streamOrBucketName, api.StreamConfig{Name: c.streamOrBucketName, Subjects: []string{c.getSubscribeSubject()}, Retention: api.LimitsPolicy, Discard: api.DiscardNew, Storage: storage, Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow, AllowDirect: true, AllowAtomicPublish: atomicBatch, AllowBatchPublish: true, PersistMode: persistMode})
		if err != nil {
			return fmt.Errorf("could not create the stream. If you want to delete and re-define the stream use `nats stream delete %s`: %w", c.streamOrBucketName, err)
		}
	}

	s, err = js.Stream(ctx, c.streamOrBucketName)
	if err != nil {
		return fmt.Errorf("could not access stream %s: %w", c.streamOrBucketName, err)
	}
	// TODO?: maybe a way to wait for the stream to be ready (e.g. when updating the stream's config (e.g. from R1 to R3))?
	log.Printf("Using stream: %s", c.streamOrBucketName)

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			return err
		}
	}

	pubCounts := msgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return err
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSPublisher(bm, errChan, nc, startwg, donewg, trigger, jsPubType, pubCounts[i], c.offset(i, pubCounts), benchId, i)
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Fatal error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsOrderedAction(_ *cobra.Command, _ []string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeJSOrdered)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeJSOrdered, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, errChan, nc, startwg, donewg, bench.TypeJSOrdered, c.numMsg, i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsConsumeAction(_ *cobra.Command, _ []string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeJSConsume)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeJSConsume, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	if c.consumerName == bench.DefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it just delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}
	}

	subCounts := msgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, errChan, nc, startwg, donewg, bench.TypeJSConsume, subCounts[i], i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsFetchAction(_ *cobra.Command, _ []string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeJSFetch)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeJSFetch, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	if c.consumerName == bench.DefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it be just create or delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}
	}

	subCounts := msgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, errChan, nc, startwg, donewg, bench.TypeJSFetch, subCounts[i], i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsSyncGetAction(_ *cobra.Command, _ []string) error {
	return c.jsGetAction(bench.TypeJSGetSync)
}

func (c *benchCmd) jsBatchedDirectAction(_ *cobra.Command, _ []string) error {
	return c.jsGetAction(bench.TypeJSGetDirectBatched)
}

func (c *benchCmd) jsGetAction(benchType string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(benchType)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", benchType, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSGetter(bm, errChan, nc, startwg, donewg, benchType, c.numMsg, i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) kvPutAction(_ *cobra.Command, _ []string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	// catch the number of clients being more than number of messages
	if c.numClients > c.numMsg {
		c.numClients = c.numMsg
	}

	banner := c.generateBanner(bench.TypeKVPut)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeKVPut, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	// There is no way to purge all the keys in a KV bucket in a single operation so deleting the bucket instead
	if c.purge {
		err = js.DeleteKeyValue(ctx, c.streamOrBucketName)
		if err != nil {
			return err
		}
	}

	if c.streamOrBucketName == bench.DefaultBucketName {
		// create bucket
		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: c.streamOrBucketName, History: c.history, Storage: c.storageType(), Description: "nats bench bucket", Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
		if err != nil {
			return err
		}
	}

	startwg.Wait()

	pubCounts := msgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return err
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVPutter(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], c.offset(i, pubCounts), i)
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) kvGetAction(_ *cobra.Command, _ []string) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeKVGet)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeKVGet, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	subCounts := msgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d cloud not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVGetter(bm, errChan, nc, startwg, donewg, subCounts[i], c.offset(i, subCounts), i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsOrderedAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeOldJSOrdered)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeOldJSOrdered, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, errChan, nc, startwg, donewg, c.numMsg, bench.TypeOldJSOrdered, i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsPushAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeOldJSPush)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeOldJSPush, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return err
	}

	js, err := nc.JetStream(append(jsOpts(), nats.MaxWait(opts().Timeout))...)
	if err != nil {
		return err
	}

	if c.consumerName == bench.DefaultDurableConsumerName {
		ack := nats.AckNonePolicy
		if c.ack {
			ack = nats.AckExplicitPolicy
		}
		maxAckPending := 0
		if c.ack {
			maxAckPending = c.batchSize * c.numClients
		}
		_, err = js.AddConsumer(c.streamOrBucketName, &nats.ConsumerConfig{
			Durable:        c.consumerName,
			DeliverSubject: c.consumerName + "-DELIVERY",
			DeliverGroup:   c.consumerName + "-GROUP",
			DeliverPolicy:  nats.DeliverAllPolicy,
			AckPolicy:      ack,
			ReplayPolicy:   nats.ReplayInstantPolicy,
			MaxAckPending:  maxAckPending,
		})
		if err != nil {
			log.Fatal("Error creating the durable push consumer: ", err)
		}

		defer func() {
			err := js.DeleteConsumer(c.streamOrBucketName, c.consumerName)
			if err != nil {
				log.Printf("Error deleting the durable push consumer on stream %s: %v", c.streamOrBucketName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
	}

	if c.ack {
		log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
	} else {
		log.Printf("Defined durable unacked push consumer: %s\n", c.consumerName)
	}

	subCounts := msgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, errChan, nc, startwg, donewg, subCounts[i], bench.TypeOldJSPush, i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsPullAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(bench.TypeOldJSPull)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", bench.TypeOldJSPull, c.numClients)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}

	js, err := nc.JetStream(append(jsOpts(), nats.MaxWait(opts().Timeout))...)
	if err != nil {
		return fmt.Errorf("getting the JetStream context: %w", err)
	}

	ack := nats.AckNonePolicy
	if c.ack {
		ack = nats.AckExplicitPolicy
	}

	if c.consumerName == bench.DefaultDurableConsumerName {
		_, err = js.AddConsumer(c.streamOrBucketName, &nats.ConsumerConfig{
			Durable:       c.consumerName,
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     ack,
			ReplayPolicy:  nats.ReplayInstantPolicy,
			MaxAckPending: min(c.numClients*c.batchSize, 10000),
		})
		if err != nil {
			return fmt.Errorf("creating the durable consumer '%s': %w", c.consumerName, err)
		}
		defer func() {
			err := js.DeleteConsumer(c.streamOrBucketName, c.consumerName)
			if err != nil {
				log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamOrBucketName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
		log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
	}

	subCounts := msgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}

		defer nc.Close()

		nc.SetDisconnectErrHandler(c.disconnectionHandler)
		nc.SetErrorHandler(c.errorHandler)

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, errChan, nc, startwg, donewg, subCounts[i], bench.TypeOldJSPull, i)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) getPayload(msgSize int) ([]byte, error) {
	if len(c.payloadFilename) > 0 {

		buffer, err := os.ReadFile(c.payloadFilename)
		if err != nil {
			return nil, fmt.Errorf("reading the payload file: %w", err)
		}

		return buffer, nil
	}

	buffer := make([]byte, msgSize)
	return buffer, nil
}

func (c *benchCmd) coreNATSPublisher(nc *nats.Conn, progress *uiprogress.Bar, payloadSize int, numMsg int, offset int) ([]uint64, error) {
	state := "Publishing"
	payload, err := c.getPayload(payloadSize)
	if err != nil {
		return nil, err
	}

	headers, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return nil, err
	}

	message := nats.Msg{Data: payload, Header: headers}

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	c.multisubjectFormat = fmt.Sprintf("%%0%dd", len(strconv.Itoa(c.multiSubjectMax)))

	latencies := make([]uint64, numMsg)
	throttler := newRateThrottler(c.perClientThroughput())

	for i := 0; i < numMsg; i++ {
		message.Subject = c.getPublishSubject(i + offset)
		start := time.Now()

		err := nc.PublishMsg(&message)
		if err != nil {
			return nil, fmt.Errorf("publishing: %w", err)
		}

		latencies[i] = uint64(time.Since(start).Nanoseconds())

		if progress != nil {
			progress.Incr()
		}
		time.Sleep(c.sleep)
		if throttler != nil {
			throttler.throttle(i + 1)
		}
	}

	state = "Finished  "
	return latencies, nil
}

func (c *benchCmd) coreNATSRequester(nc *nats.Conn, progress *uiprogress.Bar, payloadSize int, numMsg int, offset int) ([]uint64, error) {
	errBytes := []byte("error")
	minusByte := byte('-')
	state := "Requesting"
	payload, err := c.getPayload(payloadSize)
	if err != nil {
		return nil, err
	}

	headers, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return nil, err
	}

	message := nats.Msg{Data: payload, Header: headers}

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	c.multisubjectFormat = fmt.Sprintf("%%0%dd", len(strconv.Itoa(c.multiSubjectMax)))

	latencies := make([]uint64, numMsg)
	throttler := newRateThrottler(c.perClientThroughput())

	for i := 0; i < numMsg; i++ {
		message.Subject = c.getPublishSubject(i + offset)

		start := time.Now()
		m, err := nc.RequestMsg(&message, opts().Timeout)
		if err != nil {
			return nil, fmt.Errorf("requesting: %w", err)
		}

		latencies[i] = uint64(time.Since(start).Nanoseconds())

		if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
			log.Fatalf("Request did not receive a good reply: %q", m.Data)
		}

		if progress != nil {
			progress.Incr()
		}
		time.Sleep(c.sleep)
		if throttler != nil {
			throttler.throttle(i + 1)
		}
	}

	state = "Finished  "
	return latencies, nil
}

func (c *benchCmd) jsPublisher(nc *nats.Conn, progress *uiprogress.Bar, jsPubType string, payloadSize int, numMsg int, idPrefix string, offset int, clientNumber int) ([]uint64, error) {
	js, err := c.getJS(nc)
	if err != nil {
		return nil, err
	}

	var state string
	payload, err := c.getPayload(payloadSize)
	if err != nil {
		return nil, err
	}

	headers, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return nil, err
	}

	message := nats.Msg{Data: payload, Header: headers}

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	c.multisubjectFormat = fmt.Sprintf("%%0%dd", len(strconv.Itoa(c.multiSubjectMax)))

	var latencies []uint64
	throttler := newRateThrottler(c.perClientThroughput())

	// Asynchronous publish
	if jsPubType == bench.TypeJSPubAsync {
		latencies = make([]uint64, uint64(math.Ceil(float64(numMsg)/float64(c.batchSize))))
		// attempts counts every PublishMsgAsync call including retries after
		// ack failures, since `i` only advances on successful acks. Driving
		// the throttler off attempts keeps pacing honest across retries.
		attempts := 0

		for i := 0; i < numMsg; {
			state = "Publishing"
			futures := make([]jetstream.PubAckFuture, min(c.batchSize, numMsg-i))
			start := time.Now()

			for j := 0; j < c.batchSize && (i+j) < numMsg; j++ {
				if c.deDuplication {
					message.Header.Set(nats.MsgIdHdr, idPrefix+"-"+strconv.Itoa(clientNumber)+"-"+strconv.Itoa(i+j+offset))
				}

				message.Subject = c.getPublishSubject(i + j + offset)
				futures[j], err = js.PublishMsgAsync(&message)

				if err != nil {
					return nil, fmt.Errorf("publishing asynchronously: %w", err)
				}

				attempts++

				if progress != nil {
					progress.Incr()
				}
				// Account any sleeps for the latency tracking, since the latency is tracked per batch size.
				if c.sleep > 0 {
					time.Sleep(c.sleep)
					start = start.Add(c.sleep)
				}
				if throttler != nil {
					start = start.Add(throttler.throttle(attempts))
				}
			}

			state = "AckWait   "

			select {
			case <-js.PublishAsyncComplete():
				state = "ProcessAck"
				for future := range futures {
					select {
					case <-futures[future].Ok():
						i++
					case err := <-futures[future].Err():
						return nil, fmt.Errorf("async publish acknowledgement is an error: %w", err)
					}
				}
			case <-time.After(opts().Timeout):
				return nil, fmt.Errorf("JS PubAsync ack timeout (pending=%d)", js.PublishAsyncPending())
			}

			latencies[uint64(math.Ceil(float64(i)/float64(c.batchSize)))-1] = uint64(time.Since(start).Nanoseconds())
		}

		state = "Finished  "
	} else if jsPubType == bench.TypeJSPubBatchAtomic {
		// Atomic batch publish
		latencies = make([]uint64, uint64(math.Ceil(float64(numMsg)/float64(c.batchSize))))
		batch := 0
		var msgs int
		batchId := idPrefix + "-" + strconv.Itoa(clientNumber)

		for i := 0; i < numMsg; {
			state = "Batching  "
			msgs = min(c.batchSize, numMsg-i)
			message.Header.Del("Nats-Batch-Commit")
			start := time.Now()

			for j := 0; j < msgs; j++ {
				if c.deDuplication {
					message.Header.Set(nats.MsgIdHdr, batchId+"-"+strconv.Itoa(i+j+offset))
				}

				message.Header.Set("Nats-Batch-Id", batchId)
				message.Header.Set("Nats-Batch-Sequence", strconv.Itoa(j+1))
				message.Subject = c.getPublishSubject(i + j + offset)

				if j == msgs-1 {
					state = "Committing"
					message.Header.Set("Nats-Batch-Commit", "1")

					_, err := js.PublishMsg(ctx, &message)
					if err != nil {
						return nil, fmt.Errorf("publishing with batch commit: %w", err)
					}
				} else {
					err = nc.PublishMsg(&message)
					if err != nil {
						return nil, fmt.Errorf("publishing: %w", err)
					}
				}

				if progress != nil {
					progress.Incr()
				}
				// Account any sleeps for the latency tracking, since the latency is tracked per batch size.
				if c.sleep > 0 {
					time.Sleep(c.sleep)
					start = start.Add(c.sleep)
				}
				if throttler != nil {
					start = start.Add(throttler.throttle(i + j + 1))
				}
			}

			latencies[batch] = uint64(time.Since(start).Nanoseconds())
			batch++
			i += msgs
		}
		state = "Finished  "
	} else if jsPubType == bench.TypeJSPubBatchFast {
		// Fast batch publish

		// Worst-case: ack-every-message, but batching should normally be more optimal.
		latencies = make([]uint64, numMsg)
		batch := 0
		batchId := idPrefix + "-" + strconv.Itoa(clientNumber)

		fc := jetstreamext.FastPublishFlowControl{
			Flow:               uint16(min(c.batchSize, math.MaxUint16)),
			MaxOutstandingAcks: c.maxOutstandingAcks,
		}
		fp, err := jetstreamext.NewFastPublisher(js, fc)
		if err != nil {
			return nil, fmt.Errorf("fast batch publishing: %w", err)
		}

		start := time.Now()
		ackSeq := uint64(0)

		for i := 0; i < numMsg; i++ {
			state = "Batching  "

			if c.deDuplication {
				message.Header.Set(nats.MsgIdHdr, batchId+"-"+strconv.Itoa(i+offset))
			}
			message.Subject = c.getPublishSubject(i + offset)

			if ack, err := fp.AddMsg(&message); err != nil {
				return nil, fmt.Errorf("fast batch publishing: %w", err)
			} else if ack.AckSequence > ackSeq {
				ackSeq = ack.AckSequence
				// Track latency for a successful batch.
				// The library is in charge of batching, not us, so this is the best tracking we can do.
				latencies[batch] = uint64(time.Since(start).Nanoseconds())
				batch++
				start = time.Now()
			}
			if progress != nil {
				progress.Incr()
			}
			// Account any sleeps for the latency tracking, since the latency is tracked per batch size.
			if c.sleep > 0 {
				time.Sleep(c.sleep)
				start = start.Add(c.sleep)
			}
			if throttler != nil {
				start = start.Add(throttler.throttle(i + 1))
			}
		}

		state = "Committing"
		if ack, err := fp.Close(ctx); err != nil {
			return nil, fmt.Errorf("fast batch publishing: %w", err)
		} else if ack.BatchSize > ackSeq {
			latencies[batch] = uint64(time.Since(start).Nanoseconds())
			batch++
		}

		// Only keep the latencies for the batches.
		latencies = latencies[:batch]

		state = "Finished  "
	} else if jsPubType == bench.TypeJSPubSync {
		// Synchronous publish
		latencies = make([]uint64, numMsg)
		state = "Publishing"

		for i := 0; i < numMsg; i++ {
			if c.deDuplication {
				message.Header.Set(nats.MsgIdHdr, idPrefix+"-"+strconv.Itoa(clientNumber)+"-"+strconv.Itoa(i+offset))
			}

			message.Subject = c.getPublishSubject(i + offset)

			start := time.Now()
			_, err = js.PublishMsg(ctx, &message)
			if err != nil {
				return nil, fmt.Errorf("publishing synchronously: %w", err)
			}

			latencies[i] = uint64(time.Since(start).Nanoseconds())

			if progress != nil {
				progress.Incr()
			}
			time.Sleep(c.sleep)
			if throttler != nil {
				throttler.throttle(i + 1)
			}
		}
	} else {
		return nil, fmt.Errorf("unknown js publish type: %s", jsPubType)
	}

	return latencies, nil
}

func (c *benchCmd) kvPutter(nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) ([]uint64, error) {
	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		return nil, err
	}

	kvBucket, err := js.KeyValue(ctx, c.streamOrBucketName)
	if err != nil {
		return nil, fmt.Errorf("getting the kv bucket '%s': %w", c.streamOrBucketName, err)
	}
	var state = "Putting   "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	latencies := make([]uint64, numMsg)
	throttler := newRateThrottler(c.perClientThroughput())

	for i := 0; i < numMsg; i++ {
		key := offset + i

		if c.randomize > 0 {
			key = rand.IntN(c.randomize)
		}

		start := time.Now()
		_, err = kvBucket.Put(ctx, fmt.Sprintf("%d", key), msg)
		if err != nil {
			return nil, fmt.Errorf("putting: %w", err)
		}

		latencies[i] = uint64(time.Since(start).Nanoseconds())

		if progress != nil {
			progress.Incr()
		}
		time.Sleep(c.sleep)
		if throttler != nil {
			throttler.throttle(i + 1)
		}
	}
	return latencies, nil
}

func (c *benchCmd) runCorePublisher(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, clientNumber int) {
	startwg.Done()
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, publishing %s messages", clientNumber+1, bench.GetBenchTypeLabel(bench.TypeCorePub), f(numMsg))

	if c.progressBar {
		barTotal := numMsg
		if barTotal == 0 {
			barTotal = 1
		}

		progress = uiprogress.AddBar(barTotal).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()

		if numMsg == 0 {
			progress.PrependFunc(func(b *uiprogress.Bar) string {
				return "Finished  "
			})
			progress.Incr()
		}
	}

	if numMsg == 0 {
		donewg.Done()
		errChan <- nil
		return
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && clientNumber != 0 {
		n := rand.Int64N(c.sleep.Nanoseconds())
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	latencies, err := c.coreNATSPublisher(nc, progress, c.msgSize, numMsg, offset)
	if err != nil {
		errChan <- fmt.Errorf("publishing: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runCoreSubscriber(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, clientNumber int) {
	received := 0
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, expecting %s messages", clientNumber+1, bench.GetBenchTypeLabel(bench.TypeCoreSub), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Core NATS Message handler
	mh := func(msg *nats.Msg) {
		received++

		if received == 1 {
			ch <- time.Now()
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	state = "Receiving "

	sub, err := nc.Subscribe(c.getSubscribeSubject(), mh)
	if err != nil {
		errChan <- fmt.Errorf("subscribing to '%s': %w", c.getSubscribeSubject(), err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		errChan <- fmt.Errorf("setting pending limits on the subscriber: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, end, []uint64{}, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runCoreRequester(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, clientNumber int) {
	startwg.Done()
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, requesting %s messages", clientNumber+1, bench.GetBenchTypeLabel(bench.TypeServiceRequest), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && clientNumber != 0 {
		n := rand.Int64N(c.sleep.Nanoseconds())
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	latencies, err := c.coreNATSRequester(nc, progress, c.msgSize, numMsg, offset)
	if err != nil {
		errChan <- fmt.Errorf("requesting: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runServiceServer(nc *nats.Conn, errChan chan error, startwg *sync.WaitGroup, donewg *sync.WaitGroup, clientNumber int) {
	ch := make(chan struct{}, 1)

	log.Printf("[%d] Starting %s, hit control-c to stop", clientNumber+1, bench.GetBenchTypeLabel(bench.TypeServiceServe))

	reqHandler := func(request services.Request) {
		time.Sleep(c.sleep)

		err := request.Respond([]byte("ok"))
		if err != nil {
			errChan <- fmt.Errorf("replying to the request: %w", err)
			donewg.Done()
			return
		}
	}

	_, err := services.AddService(nc, services.Config{
		Name:    bench.DefaultServiceName,
		Version: bench.DefaultServiceVersion,
		Endpoint: &services.EndpointConfig{
			Subject: c.subject,
			Handler: services.HandlerFunc(reqHandler),
		},
	})
	if err != nil {
		errChan <- fmt.Errorf("adding the service: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	<-ch

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runJSPublisher(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, benchType string, numMsg int, offset int, idPrefix string, clientNumber int) {
	startwg.Done()
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, publishing %s messages", clientNumber+1, bench.GetBenchTypeLabel(benchType), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && clientNumber != 0 {
		n := rand.Int64N(c.sleep.Nanoseconds())
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	latencies, err := c.jsPublisher(nc, progress, benchType, c.msgSize, numMsg, idPrefix, offset, clientNumber)
	if err != nil {
		errChan <- fmt.Errorf("publishing: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runJSSubscriber(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, benchType string, numMsg int, clientNumber int) {
	received := 0
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, expecting %s messages", clientNumber+1, bench.GetBenchTypeLabel(benchType), humanize.Comma(int64(numMsg)))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Message handler
	mh := func(msg jetstream.Msg) {
		received++
		time.Sleep(c.sleep)

		if benchType != bench.TypeJSOrdered {
			if c.ackMode == bench.AckModeExplicit || c.ackMode == bench.AckModeAll {
				var err error
				if c.doubleAck {
					err = msg.DoubleAck(ctx)
				} else {
					err = msg.Ack()
				}
				if err != nil {
					errChan <- fmt.Errorf("acknowledging the message: %w", err)
					donewg.Done()
					return
				}
			}
		}

		if received == 1 {
			startTime := time.Now()
			ch <- startTime

			if progress != nil {
				progress.TimeStarted = startTime
			}
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	var consumer jetstream.Consumer
	var err error
	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		errChan <- fmt.Errorf("getting the JetStream instance: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	s, err := js.Stream(ctx, c.streamOrBucketName)
	if err != nil {
		errChan <- fmt.Errorf("getting stream '%s': %w", c.streamOrBucketName, err)
		startwg.Done()
		donewg.Done()
		return
	}

	switch benchType {
	case bench.TypeJSOrdered:
		state = "Receiving "
		consumer, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: c.filterSubjects, InactiveThreshold: time.Second * 10})
		if err != nil {
			errChan <- fmt.Errorf("creating the ephemeral ordered consumer: %w", err)
			startwg.Done()
			donewg.Done()
			return
		}

		cc, err := consumer.Consume(mh, jetstream.PullMaxMessages(c.batchSize))
		if err != nil {
			errChan <- fmt.Errorf("calling Consume() on the ordered consumer: %w", err)
			startwg.Done()
			donewg.Done()
			return
		}
		defer cc.Stop()
	case bench.TypeJSConsume:
		state = "Consuming"
		consumer, err = s.Consumer(ctx, c.consumerName)
		if err != nil {
			errChan <- fmt.Errorf("getting durable consumer '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}

		cc, err := consumer.Consume(mh, jetstream.PullMaxMessages(c.batchSize), jetstream.StopAfter(numMsg))
		if err != nil {
			errChan <- fmt.Errorf("calling Consume() on the durable consumer '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}
		defer cc.Stop()
	case bench.TypeJSFetch:
		state = "Fetching"
		consumer, err = s.Consumer(ctx, c.consumerName)
		if err != nil {
			errChan <- fmt.Errorf("getting durable consumer '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	var latencies []uint64

	// Fetch messages if in fetch mode
	if benchType == bench.TypeJSFetch {
		latencies = make([]uint64, int(math.Ceil(float64(numMsg)/float64(c.batchSize))))

		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.batchSize <= (numMsg - i) {
					return c.batchSize
				} else {
					return numMsg - i
				}
			}()

			start := time.Now()
			msgs, err := consumer.Fetch(batchSize)
			end := time.Now()
			if err != nil {
				if !c.progressBar {
					if errors.Is(err, nats.ErrTimeout) {
						log.Print("Fetch  timeout!")
					} else {
						errChan <- fmt.Errorf("fetching from the consumer '%s': %w", c.consumerName, err)
						donewg.Done()
						return
					}
					c.fetchTimeout = true
				}
			} else {
				for msg := range msgs.Messages() {
					mh(msg)
					i++
				}

				if msgs.Error() != nil {
					errChan <- fmt.Errorf("getting fetched messages: %w", msgs.Error())
					c.fetchTimeout = true
					donewg.Done()
					return
				}

				latencies[uint64(math.Ceil(float64(i)/float64(c.batchSize)))-1] = uint64(end.Sub(start).Nanoseconds())
			}
		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, end, latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runJSGetter(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, benchType string, numMsg int, clientNumber int) {
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, expecting %s messages", clientNumber+1, bench.GetBenchTypeLabel(benchType), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	var err error
	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		errChan <- fmt.Errorf("getting the JetStream instance: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	state = "Getting   "

	var latencies []uint64
	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()
	switch benchType {
	case bench.TypeJSGetSync:
		stream, err := js.Stream(ctx, c.streamOrBucketName)
		if err != nil {
			errChan <- fmt.Errorf("getting stream '%s': %w", c.streamOrBucketName, err)
			donewg.Done()
			return
		}

		si, err := stream.Info(ctx)
		if err != nil {
			errChan <- fmt.Errorf("getting stream info for '%s': %w", c.streamOrBucketName, err)
			donewg.Done()
			return
		}

		latencies = make([]uint64, numMsg)
		startingSeq := si.State.FirstSeq

		for i := uint64(0); i < uint64(numMsg); i++ {
			start := time.Now()

			_, err := stream.GetMsg(ctx, i+startingSeq)
			if err != nil {
				errChan <- fmt.Errorf("getting message sequence number %d from the stream: %w", i+startingSeq, err)
				donewg.Done()
				return
			}

			latencies[i] = uint64(time.Since(start).Nanoseconds())

			if i == 0 {
				ch <- start

				if progress != nil {
					progress.TimeStarted = start
				}
			}

			if i == uint64(numMsg-1) {
				ch <- time.Now()
			}

			if progress != nil {
				progress.Incr()
			}

			time.Sleep(c.sleep)
		}
	case bench.TypeJSGetDirectBatched:
		var msgs iter.Seq2[*jetstream.RawStreamMsg, error]
		var nextSeq uint64 = 1
		latencies = make([]uint64, int(math.Ceil(float64(numMsg)/float64(c.batchSize))))

		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.batchSize <= (numMsg - i) {
					return c.batchSize
				} else {
					return numMsg - i
				}
			}()

			start := time.Now()
			msgs, err = jetstreamext.GetBatch(ctx, js, c.streamOrBucketName, batchSize, jetstreamext.GetBatchSeq(nextSeq), jetstreamext.GetBatchSubject(c.filterSubject))
			end := time.Now()
			if err != nil {
				errChan <- fmt.Errorf("doing a direct get on the stream: %w", err)
				donewg.Done()
				return
			}

			if i == 0 {
				ch <- start

				if progress != nil {
					progress.TimeStarted = start
				}
			}

			// Count how many we actually got

			gotten := 0

			for msg, err := range msgs {
				if err != nil {
					errChan <- fmt.Errorf("getting message from the stream: %w", err)
					donewg.Done()
					return
				}

				i++
				gotten++
				nextSeq = msg.Sequence + 1

				if progress != nil {
					progress.Incr()
				}
			}

			if gotten != batchSize {
				log.Printf("[%d] Warning: Got %d (expected %d) messages in this batch\n", clientNumber+1, gotten, batchSize)
				c.lessThanExpected.Store(true)
			}

			latencies[uint64(math.Ceil(float64(i)/float64(c.batchSize)))-1] = uint64(end.Sub(start).Nanoseconds())

			if i >= numMsg {
				ch <- end
			}

			time.Sleep(c.sleep)
		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, end, latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runKVPutter(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, clientNumber int) {
	startwg.Done()
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, publishing %s messages", clientNumber+1, bench.GetBenchTypeLabel(bench.TypeKVPut), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.sleep != 0 && clientNumber != 0 {
		n := rand.Int64N(c.sleep.Nanoseconds())
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	latencies, err := c.kvPutter(nc, progress, msg, numMsg, offset)
	if err != nil {
		errChan <- fmt.Errorf("putting: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runKVGetter(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int, clientNumber int) {
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, trying to get %s messages", clientNumber+1, bench.GetBenchTypeLabel(bench.TypeKVGet), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		errChan <- fmt.Errorf("getting the JetStream instance: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	kvBucket, err := js.KeyValue(ctx, c.streamOrBucketName)
	if err != nil {
		errChan <- fmt.Errorf("finding kv bucket '%s': %w", c.streamOrBucketName, err)
		donewg.Done()
		return
	}

	latencies := make([]uint64, numMsg)

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime

	if progress != nil {
		progress.TimeStarted = startTime
	}

	state = "Getting   "

	for i := 0; i < numMsg; i++ {
		var key string

		if c.randomize == 0 {
			key = fmt.Sprintf("%d", offset+i)
		} else {
			key = fmt.Sprintf("%d", rand.IntN(c.randomize))
		}
		start := time.Now()

		entry, err := kvBucket.Get(ctx, key)
		if err != nil {
			errChan <- fmt.Errorf("getting key '%s': %w", key, err)
			donewg.Done()
			return
		}

		latencies[i] = uint64(time.Since(start).Nanoseconds())

		if entry.Value() == nil {
			log.Printf("Warning: got no value for key '%d'", offset+i)
		}

		if progress != nil {
			progress.Incr()
		}

		time.Sleep(c.sleep)
	}

	ch <- time.Now()
	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, end, latencies, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runOldJSSubscriber(bm *bench.BenchmarkResults, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, benchType string, clientNumber int) {
	received := 0
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	log.Printf("[%d] Starting %s, expecting %s messages", clientNumber+1, bench.GetBenchTypeLabel(benchType), f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Message handler
	var mh func(msg *nats.Msg)

	if benchType == bench.TypeOldJSPush || benchType == bench.TypeOldJSPull {
		mh = func(msg *nats.Msg) {
			received++

			time.Sleep(c.sleep)
			if c.ack {
				var err error
				if c.doubleAck {
					err = msg.AckSync()
				} else {
					err = msg.Ack()
				}
				if err != nil {
					errChan <- fmt.Errorf("acknowledging the message: %w", err)
					donewg.Done()
					return
				}
			}

			if received >= numMsg {
				ch <- time.Now()
			}

			if progress != nil {
				progress.Incr()
			}
		}

	} else {
		mh = func(msg *nats.Msg) {
			received++
			time.Sleep(c.sleep)

			if received >= numMsg {
				ch <- time.Now()
			}

			if progress != nil {
				progress.Incr()
			}
		}
	}

	var sub *nats.Subscription
	var err error
	var js nats.JetStreamContext

	// create the subscriber
	js, err = nc.JetStream(jsOpts()...)
	if err != nil {
		errChan <- fmt.Errorf("getting the JetStream context: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	if benchType == bench.TypeOldJSPull {
		sub, err = js.PullSubscribe(c.getSubscribeSubject(), c.consumerName, nats.BindStream(c.streamOrBucketName))
		if err != nil {
			errChan <- fmt.Errorf("PullSubscribe: %w", err)
			startwg.Done()
			donewg.Done()
			return
		}
		defer func(sub *nats.Subscription) {
			err := sub.Drain()
			if err != nil {
				log.Printf("draining the subscription at the end of the run: %v", err)
			}
		}(sub)
	} else if benchType == bench.TypeOldJSPush {
		state = "Receiving "
		sub, err = js.QueueSubscribe(c.getSubscribeSubject(), c.consumerName+"-GROUP", mh, nats.Bind(c.streamOrBucketName, c.consumerName), nats.ManualAck())
		if err != nil {
			errChan <- fmt.Errorf("subscribing to the push durable '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}
		_ = sub.AutoUnsubscribe(numMsg)

	} else { // benchType == benchTypeOldJSOrdered
		state = "Consuming "
		// ordered push consumer
		sub, err = js.Subscribe(c.getSubscribeSubject(), mh, nats.OrderedConsumer())
		if err != nil {
			errChan <- fmt.Errorf("subscribing to the ordered consumer on subject '%s': %w", c.getSubscribeSubject(), err)
			startwg.Done()
			donewg.Done()
			return
		}
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		errChan <- fmt.Errorf("setting pending limits on the subscriber: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	if benchType == bench.TypeOldJSPull {
		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.batchSize <= (numMsg - i) {
					return c.batchSize
				} else {
					return numMsg - i
				}
			}()

			if progress != nil {
				state = "Pulling   "
			}

			msgs, err := sub.Fetch(batchSize, nats.MaxWait(opts().Timeout))
			if err == nil {
				if progress != nil {
					state = "Handling  "
				}

				for _, msg := range msgs {
					mh(msg)
					i++
				}
			} else {
				if !c.progressBar {
					if errors.Is(err, nats.ErrTimeout) {
						log.Print("Fetch timeout!")
					} else {
						errChan <- fmt.Errorf("fetching: %w", err)
						donewg.Done()
						return
					}
				}
				c.fetchTimeout = true
			}

		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSample(bench.NewSample(numMsg, c.msgSize, start, end, []uint64{}, nc))

	donewg.Done()
	errChan <- nil
}

// msgsPerClient divides the number of messages by the number of clients and tries to distribute them as evenly as possible
func msgsPerClient(numMsgs, numClients int) []int {
	var counts []int
	if numClients == 0 || numMsgs == 0 {
		return counts
	}
	counts = make([]int, numClients)
	mc := numMsgs / numClients
	for i := 0; i < numClients; i++ {
		counts[i] = mc
	}
	extra := numMsgs % numClients
	for i := 0; i < extra; i++ {
		counts[i]++
	}
	return counts
}
