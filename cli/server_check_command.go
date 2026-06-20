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
	"fmt"
	"regexp"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/monitor"

	"github.com/spf13/cobra"
)

type SrvCheckCmd struct {
	connectWarning  time.Duration
	connectCritical time.Duration
	rttWarning      time.Duration
	rttCritical     time.Duration
	reqWarning      time.Duration
	reqCritical     time.Duration

	sourcesStream            string
	sourcesLagCritical       uint64
	sourcesLagCriticalIsSet  bool
	sourcesSeenCritical      time.Duration
	sourcesSeenCriticalIsSet bool
	sourcesMinSources        int
	sourcesMinSourcesIsSet   bool
	sourcesMaxSources        int
	sourcesMaxSourcesIsSet   bool
	streamMessagesWarn       uint64
	streamMessagesWarnIsSet  bool
	streamMessagesCrit       uint64
	streamMessagesCritIsSet  bool
	subjectsWarn             int
	subjectsWarnIsSet        bool
	subjectsCrit             int
	subjectsCritIsSet        bool

	consumerName                        string
	consumerAckOutstandingCritical      int
	consumerAckOutstandingCriticalIsSet bool
	consumerWaitingCritical             int
	consumerWaitingCriticalIsSet        bool
	consumerUnprocessedCritical         int
	consumerUnprocessedCriticalIsSet    bool
	consumerLastDeliveryCritical        time.Duration
	consumerLastDeliveryCriticalIsSet   bool
	consumerLastAckCritical             time.Duration
	consumerLastAckCriticalIsSet        bool
	consumerRedeliveryCritical          int
	consumerRedeliveryCriticalIsSet     bool
	consumerPinned                      bool

	raftExpect            int
	raftExpectIsSet       bool
	raftLagCritical       uint64
	raftLagCriticalIsSet  bool
	raftSeenCritical      time.Duration
	raftSeenCriticalIsSet bool

	jsMemWarn             int
	jsMemCritical         int
	jsStoreWarn           int
	jsStoreCritical       int
	jsStreamsWarn         int
	jsStreamsCritical     int
	jsConsumersWarn       int
	jsConsumersCritical   int
	jsReplicas            bool
	jsReplicaSeenCritical time.Duration
	jsReplicaLagCritical  uint64

	srvName           string
	srvCPUWarn        int
	srvCPUCrit        int
	srvMemWarn        int
	srvMemCrit        int
	srvConnWarn       int
	srvConnCrit       int
	srvSubsWarn       int
	srvSubCrit        int
	srvUptimeWarn     time.Duration
	srvUptimeCrit     time.Duration
	srvAuthRequired   bool
	srvTLSRequired    bool
	srvJSRequired     bool
	srvtlsExpiredWarn time.Duration
	srvtlsExpiredCrit time.Duration

	msgSubject      string
	msgAgeWarn      time.Duration
	msgAgeCrit      time.Duration
	msgRegexp       *regexp.Regexp
	msgBodyAsTs     bool
	msgHeaders      map[string]string
	msgHeadersMatch map[string]string
	msgPayload      string
	msgCrit         time.Duration
	msgWarn         time.Duration

	kvBucket                 string
	kvValuesCrit             int64
	kvValuesWarn             int64
	kvKey                    string
	credentialValidityCrit   time.Duration
	credentialValidityWarn   time.Duration
	credentialRequiresExpire bool
	credential               string

	exporterConfigFile  string
	exporterPort        int
	exporterCertificate string
	exporterKey         string
}

func configureServerCheckCommand(srv *cobra.Command) {
	c := &SrvCheckCmd{
		msgHeaders:      make(map[string]string),
		msgHeadersMatch: make(map[string]string),
	}

	const multipleChecks = "Multiple checks and thresholds can be passed in one command\n\n"
	const warnAndCritical = "You should set both warn and critical thresholds where applicable\n\n"
	const inversion = "For most flags setting critical to a smaller value than warn will invert the check from >= to <=\n\n"

	check := addCommand(srv, "check", "Health check for NATS servers")
	check.PersistentFlags().Var(newEnumValue(&checkRenderFormatText, "nagios", "nagios", "json", "prometheus", "text"), "format", "Render the check in a specific format (nagios, json, prometheus, text)")
	check.PersistentFlags().StringVar(&opts().PrometheusNamespace, "namespace", opts().PrometheusNamespace, "The prometheus namespace to use in output")
	check.PersistentFlags().StringVar(&checkRenderOutFile, "outfile", "", "Save output to a file rather than STDOUT")
	check.PersistentPreRunE = c.parseRenderFormat

	conn := addCommand(check, "connection", "Checks basic server connection")
	conn.Aliases = []string{"conn"}
	conn.RunE = c.checkConnection
	cmdAddTags(conn, "scope:user", "impact:ro")
	conn.Long = multipleChecks + warnAndCritical
	conn.Flags().DurationVar(&c.connectWarning, "connect-warn", 500*time.Millisecond, "Warning threshold to allow for establishing connections")
	conn.Flags().DurationVar(&c.connectCritical, "connect-critical", time.Second, "Critical threshold to allow for establishing connections")
	conn.Flags().DurationVar(&c.rttWarning, "rtt-warn", 500*time.Millisecond, "Warning threshold to allow for server RTT")
	conn.Flags().DurationVar(&c.rttCritical, "rtt-critical", time.Second, "Critical threshold to allow for server RTT")
	conn.Flags().DurationVar(&c.reqWarning, "req-warn", 500*time.Millisecond, "Warning threshold to allow for full round trip test")
	conn.Flags().DurationVar(&c.reqCritical, "req-critical", time.Second, "Critical threshold to allow for full round trip test")

	stream := addCommand(check, "stream", "Checks the health of mirrored streams, streams with sources or clustered streams")
	stream.RunE = c.checkStream
	cmdAddTags(stream, "scope:user", "impact:ro")
	stream.Long = multipleChecks + warnAndCritical + inversion + `These settings can be set using Stream Metadata in the following form:

	io.nats.monitor.lag-critical: 200

When set these settings will be used, but can be overridden using --lag-critical.`
	stream.Flags().StringVar(&c.sourcesStream, "stream", "", "The streams to check")
	_ = stream.MarkFlagRequired("stream")
	stream.Flags().Uint64Var(&c.sourcesLagCritical, "lag-critical", 0, "Critical threshold to allow for lag on any source or mirror")
	flagPlaceholder(stream, "lag-critical", "MSGS")
	stream.Flags().DurationVar(&c.sourcesSeenCritical, "seen-critical", 0, "Critical threshold for how long ago the source or mirror should have been seen")
	flagPlaceholder(stream, "seen-critical", "DURATION")
	stream.Flags().IntVar(&c.sourcesMinSources, "min-sources", 0, "Minimum number of sources to expect")
	flagPlaceholder(stream, "min-sources", "SOURCES")
	stream.Flags().IntVar(&c.sourcesMaxSources, "max-sources", 0, "Maximum number of sources to expect")
	flagPlaceholder(stream, "max-sources", "SOURCES")
	stream.Flags().IntVar(&c.raftExpect, "peer-expect", 0, "Number of cluster replicas to expect")
	flagPlaceholder(stream, "peer-expect", "SERVERS")
	stream.Flags().Uint64Var(&c.raftLagCritical, "peer-lag-critical", 0, "Critical threshold to allow for cluster peer lag")
	flagPlaceholder(stream, "peer-lag-critical", "OPS")
	stream.Flags().DurationVar(&c.raftSeenCritical, "peer-seen-critical", 0, "Critical threshold for how long ago a cluster peer should have been seen")
	flagPlaceholder(stream, "peer-seen-critical", "DURATION")
	stream.Flags().Uint64Var(&c.streamMessagesWarn, "msgs-warn", 0, "Warn if there are fewer than this many messages in the stream")
	flagPlaceholder(stream, "msgs-warn", "MSGS")
	stream.Flags().Uint64Var(&c.streamMessagesCrit, "msgs-critical", 0, "Critical if there are fewer than this many messages in the stream")
	flagPlaceholder(stream, "msgs-critical", "MSGS")
	stream.Flags().IntVar(&c.subjectsWarn, "subjects-warn", 0, "Critical threshold for subjects in the stream")
	flagPlaceholder(stream, "subjects-warn", "SUBJECTS")
	stream.Flags().IntVar(&c.subjectsCrit, "subjects-critical", 0, "Warning threshold for subjects in the stream")
	flagPlaceholder(stream, "subjects-critical", "SUBJECTS")

	consumer := addCommand(check, "consumer", "Checks the health of a consumer")
	consumer.RunE = c.checkConsumer
	cmdAddTags(consumer, "scope:user", "impact:ro")
	consumer.Long = multipleChecks + `These settings can be set using Consumer Metadata in the following form:

	io.nats.monitor.waiting-critical: 20

When set these settings will be used, but can be overridden using --waiting-critical.`
	consumer.Flags().StringVar(&c.sourcesStream, "stream", "", "The streams to check")
	_ = consumer.MarkFlagRequired("stream")
	consumer.Flags().StringVar(&c.consumerName, "consumer", "", "The consumer to check")
	_ = consumer.MarkFlagRequired("consumer")
	consumer.Flags().IntVar(&c.consumerAckOutstandingCritical, "outstanding-ack-critical", -1, "Maximum number of outstanding acks to allow")
	consumer.Flags().IntVar(&c.consumerWaitingCritical, "waiting-critical", -1, "Maximum number of waiting pulls to allow")
	consumer.Flags().IntVar(&c.consumerUnprocessedCritical, "unprocessed-critical", -1, "Maximum number of unprocessed messages to allow")
	consumer.Flags().DurationVar(&c.consumerLastDeliveryCritical, "last-delivery-critical", 0, "Time to allow since the last delivery")
	consumer.Flags().DurationVar(&c.consumerLastAckCritical, "last-ack-critical", 0, "Time to allow since the last ack")
	consumer.Flags().IntVar(&c.consumerRedeliveryCritical, "redelivery-critical", -1, "Maximum number of redeliveries to allow")
	consumer.Flags().BoolVar(&c.consumerPinned, "pinned", false, "Requires Pinned Client priority with all groups having a pinned client")

	msg := addCommand(check, "message", "Checks properties of a message stored in a stream")
	msg.RunE = c.checkMsg
	cmdAddTags(msg, "scope:user", "impact:ro")
	msg.Long = multipleChecks + warnAndCritical
	msg.Flags().StringVar(&c.sourcesStream, "stream", "", "The streams to check")
	_ = msg.MarkFlagRequired("stream")
	msg.Flags().StringVar(&c.msgSubject, "subject", ">", "The subject to fetch a message from")
	msg.Flags().DurationVar(&c.msgAgeWarn, "age-warn", 0, "Warning threshold for message age as a duration")
	flagPlaceholder(msg, "age-warn", "DURATION")
	msg.Flags().DurationVar(&c.msgAgeCrit, "age-critical", 0, "Critical threshold for message age as a duration")
	flagPlaceholder(msg, "age-critical", "DURATION")
	c.msgRegexp = regexp.MustCompile(".")
	msg.Flags().Var(newRegexpValue(&c.msgRegexp), "content", "Regular expression to check the content against")
	msg.Flags().BoolVar(&c.msgBodyAsTs, "body-timestamp", false, "Use message body as a unix timestamp instead of message metadata")

	meta := addCommand(check, "meta", "Check JetStream cluster state")
	meta.Aliases = []string{"raft"}
	meta.RunE = c.checkRaft
	cmdAddTags(meta, "scope:user", "impact:ro")
	meta.Long = multipleChecks
	meta.Flags().IntVar(&c.raftExpect, "expect", 0, "Number of servers to expect")
	_ = meta.MarkFlagRequired("expect")
	flagPlaceholder(meta, "expect", "SERVERS")
	meta.Flags().Uint64Var(&c.raftLagCritical, "lag-critical", 0, "Critical threshold to allow for lag")
	flagPlaceholder(meta, "lag-critical", "OPS")
	_ = meta.MarkFlagRequired("lag-critical")
	meta.Flags().DurationVar(&c.raftSeenCritical, "seen-critical", 0, "Critical threshold for how long ago a peer should have been seen")
	_ = meta.MarkFlagRequired("seen-critical")
	flagPlaceholder(meta, "seen-critical", "DURATION")

	req := addCommand(check, "request", "Checks a request-reply service")
	req.Aliases = []string{"req"}
	req.RunE = c.checkRequest
	cmdAddTags(req, "scope:user", "impact:rw")
	req.Long = multipleChecks + warnAndCritical
	req.Flags().StringVar(&c.msgSubject, "subject", "", "The subject to send the request to")
	_ = req.MarkFlagRequired("subject")
	req.Flags().StringVar(&c.msgPayload, "payload", "", "Payload to send in the request")
	req.Flags().Var(newStringMapValue(&c.msgHeaders), "headers", "Headers to publish in the request")
	req.Flags().Var(newRegexpValue(&c.msgRegexp), "match-payload", "Regular expression the response should match")
	req.Flags().Var(newStringMapValue(&c.msgHeaders), "match-headers", "Headers to publish in the request")
	req.Flags().DurationVar(&c.msgCrit, "response-critical", 0, "Critical threshold for response time")
	req.Flags().DurationVar(&c.msgWarn, "response-warn", 0, "Warning threshold for response time")

	js := addCommand(check, "jetstream", "Check JetStream account state")
	js.Aliases = []string{"js"}
	js.RunE = c.checkJS
	cmdAddTags(js, "scope:user", "impact:ro")
	js.Long = multipleChecks + warnAndCritical + inversion
	js.Flags().IntVar(&c.jsMemWarn, "mem-warn", 75, "Warning threshold for memory storage, in percent of limit")
	js.Flags().IntVar(&c.jsMemCritical, "mem-critical", 90, "Critical threshold for memory storage, in percent of limit")
	js.Flags().IntVar(&c.jsStoreWarn, "store-warn", 75, "Warning threshold for disk storage, in percent of limit")
	js.Flags().IntVar(&c.jsStoreCritical, "store-critical", 90, "Critical threshold for disk storage, in percent of limit")
	js.Flags().IntVar(&c.jsStreamsWarn, "streams-warn", -1, "Warning threshold for number of streams used, in percent of limit")
	js.Flags().IntVar(&c.jsStreamsCritical, "streams-critical", -1, "Critical threshold for number of streams used, in percent of limit")
	js.Flags().IntVar(&c.jsConsumersWarn, "consumers-warn", -1, "Warning threshold for number of consumers used, in percent of limit")
	js.Flags().IntVar(&c.jsConsumersCritical, "consumers-critical", -1, "Critical threshold for number of consumers used, in percent of limit")
	negatableBoolVar(js, &c.jsReplicas, "replicas", true, "Checks if all streams have healthy replicas")
	js.Flags().DurationVar(&c.jsReplicaSeenCritical, "replica-seen-critical", 5*time.Second, "Critical threshold for when a stream replica should have been seen, as a duration")
	js.Flags().Uint64Var(&c.jsReplicaLagCritical, "replica-lag-critical", 200, "Critical threshold for how many operations behind a peer can be")

	serv := addCommand(check, "server", "Checks a NATS Server health")
	serv.RunE = c.checkSrv
	cmdAddTags(serv, "scope:system", "impact:ro")
	serv.Long = multipleChecks + warnAndCritical + inversion
	serv.Flags().StringVar(&c.srvName, "name", "", "Server name to require in the result")
	_ = serv.MarkFlagRequired("name")
	serv.Flags().IntVar(&c.srvCPUWarn, "cpu-warn", 0, "Warning threshold for CPU usage, in percent")
	serv.Flags().IntVar(&c.srvCPUCrit, "cpu-critical", 0, "Critical threshold for CPU usage, in percent")
	serv.Flags().IntVar(&c.srvMemWarn, "mem-warn", 0, "Warning threshold for Memory usage, in bytes")
	serv.Flags().IntVar(&c.srvMemCrit, "mem-critical", 0, "Critical threshold Memory CPU usage, in bytes")
	serv.Flags().IntVar(&c.srvConnWarn, "conn-warn", 0, "Warning threshold for connections, supports inversion")
	serv.Flags().IntVar(&c.srvConnCrit, "conn-critical", 0, "Critical threshold for connections, supports inversion")
	serv.Flags().IntVar(&c.srvSubsWarn, "subs-warn", 0, "Warning threshold for number of active subscriptions, supports inversion")
	serv.Flags().IntVar(&c.srvSubCrit, "subs-critical", 0, "Critical threshold for number of active subscriptions, supports inversion")
	serv.Flags().DurationVar(&c.srvUptimeWarn, "uptime-warn", 0, "Warning threshold for server uptime as duration")
	serv.Flags().DurationVar(&c.srvUptimeCrit, "uptime-critical", 0, "Critical threshold for server uptime as duration")
	serv.Flags().BoolVar(&c.srvAuthRequired, "auth-required", false, "Checks that authentication is enabled")
	serv.Flags().BoolVar(&c.srvTLSRequired, "tls-required", false, "Checks that TLS is required")
	serv.Flags().BoolVar(&c.srvJSRequired, "js-required", false, "Checks that JetStream is enabled")
	serv.Flags().DurationVar(&c.srvtlsExpiredWarn, "tls-cert-warn", 0, "Warning threshold for TLS certificate expiry like 1d3h5m")
	serv.Flags().DurationVar(&c.srvtlsExpiredCrit, "tls-cert-crit", 0, "Critical threshold for TLS certificate expiry like 1d3h5m")

	kv := addCommand(check, "kv", "Checks a NATS KV Bucket")
	kv.RunE = c.checkKV
	cmdAddTags(kv, "scope:user", "impact:ro")
	kv.Long = multipleChecks + warnAndCritical + inversion
	kv.Flags().StringVar(&c.kvBucket, "bucket", "", "Checks a specific bucket")
	_ = kv.MarkFlagRequired("bucket")
	kv.Flags().Int64Var(&c.kvValuesCrit, "values-critical", -1, "Critical threshold for number of values in the bucket")
	kv.Flags().Int64Var(&c.kvValuesWarn, "values-warn", -1, "Warning threshold for number of values in the bucket")
	kv.Flags().StringVar(&c.kvKey, "key", "", "Requires a key to have any non-delete value set")

	cred := addCommand(check, "credential", "Checks the validity of a NATS credential file")
	cred.RunE = c.checkCredentialAction
	cmdAddTags(cred, "scope:system", "impact:ro")
	cred.Long = multipleChecks + warnAndCritical + inversion
	cred.Flags().StringVar(&c.credential, "credential", "", "The file holding the NATS credential")
	_ = cred.MarkFlagRequired("credential")
	cred.Flags().DurationVar(&c.credentialValidityWarn, "validity-warn", 0, "Warning threshold for time before expiry")
	cred.Flags().DurationVar(&c.credentialValidityCrit, "validity-critical", 0, "Critical threshold for time before expiry")
	negatableBoolVar(cred, &c.credentialRequiresExpire, "require-expiry", true, "Requires the credential to have expiry set")

	exporter := addCommand(check, "exporter", "Prometheus exporter for server checks")
	exporter.Hidden = true
	exporter.RunE = c.exporterAction
	cmdAddTags(exporter, "scope:system", "impact:rw")
	exporter.Flags().Var(newExistingFileValue(&c.exporterConfigFile), "config", "Exporter configuration")
	_ = exporter.MarkFlagRequired("config")
	exporter.Flags().IntVar(&c.exporterPort, "port", 8080, "Port to listen on")
	exporter.Flags().Var(newExistingFileValue(&c.exporterKey), "https-key", "Key for HTTPS")
	exporter.Flags().Var(newExistingFileValue(&c.exporterCertificate), "https-certificate", "Certificate for HTTPS")
}

var (
	checkRenderFormatText = "nagios"
	checkRenderFormat     = monitor.NagiosFormat
	checkRenderOutFile    = ""
)

func (c *SrvCheckCmd) parseRenderFormat(_ *cobra.Command, _ []string) error {
	switch checkRenderFormatText {
	case "prometheus":
		checkRenderFormat = monitor.PrometheusFormat
	case "text":
		checkRenderFormat = monitor.TextFormat
	case "json":
		checkRenderFormat = monitor.JSONFormat
	}

	return nil
}

func (c *SrvCheckCmd) checkRequest(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: c.msgSubject, Check: "request", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckRequestOptions{
		Subject:              c.msgSubject,
		Payload:              c.msgPayload,
		Header:               c.msgHeaders,
		HeaderMatch:          c.msgHeadersMatch,
		ResponseTimeWarn:     c.msgWarn.Seconds(),
		ResponseTimeCritical: c.msgCrit.Seconds(),
	}

	if c.msgRegexp != nil {
		checkOpts.ResponseMatch = c.msgRegexp.String()
	}

	var err error
	nc := opts().Conn

	if nc == nil {
		err = monitor.CheckRequest(opts().Config.ServerURL(), natsOpts(), check, opts().Timeout, checkOpts)
	} else {
		err = monitor.CheckRequestWithConnection(nc, check, opts().Timeout, checkOpts)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkConsumer(cmd *cobra.Command, _ []string) error {
	c.consumerAckOutstandingCriticalIsSet = cmd.Flags().Changed("outstanding-ack-critical")
	c.consumerWaitingCriticalIsSet = cmd.Flags().Changed("waiting-critical")
	c.consumerUnprocessedCriticalIsSet = cmd.Flags().Changed("unprocessed-critical")
	c.consumerLastDeliveryCriticalIsSet = cmd.Flags().Changed("last-delivery-critical")
	c.consumerLastAckCriticalIsSet = cmd.Flags().Changed("last-ack-critical")
	c.consumerRedeliveryCriticalIsSet = cmd.Flags().Changed("redelivery-critical")

	check := &monitor.Result{Name: fmt.Sprintf("%s_%s", c.sourcesStream, c.consumerName), Check: "consumer", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckConsumerHealthOptions{
		StreamName:   c.sourcesStream,
		ConsumerName: c.consumerName,
		Pinned:       c.consumerPinned,
	}

	if c.consumerAckOutstandingCriticalIsSet {
		checkOpts.AckOutstandingCritical = c.consumerAckOutstandingCritical
	}
	if c.consumerWaitingCriticalIsSet {
		checkOpts.WaitingCritical = c.consumerWaitingCritical
	}
	if c.consumerUnprocessedCriticalIsSet {
		checkOpts.UnprocessedCritical = c.consumerUnprocessedCritical
	}
	if c.consumerLastDeliveryCriticalIsSet {
		checkOpts.LastDeliveryCritical = c.consumerLastDeliveryCritical.Seconds()
	}
	if c.consumerLastAckCriticalIsSet {
		checkOpts.LastAckCritical = c.consumerLastAckCritical.Seconds()
	}
	if c.consumerRedeliveryCriticalIsSet {
		checkOpts.RedeliveryCritical = c.consumerRedeliveryCritical
	}

	logger := api.NewDiscardLogger()
	if opts().Trace {
		logger = api.NewDefaultLogger(api.TraceLevel)
	}

	var err error
	mgr := opts().Mgr

	if mgr == nil {
		err = monitor.CheckConsumerHealth(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, checkOpts, logger)
	} else {
		err = monitor.CheckConsumerHealthWithConnection(mgr, check, checkOpts, logger)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkKV(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: c.kvBucket, Check: "kv", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckKVBucketAndKeyOptions{
		Bucket:         c.kvBucket,
		Key:            c.kvKey,
		ValuesCritical: c.kvValuesCrit,
		ValuesWarning:  c.kvValuesWarn,
	}

	var err error
	nc := opts().Conn

	if nc == nil {
		err = monitor.CheckKVBucketAndKey(opts().Config.ServerURL(), natsOpts(), check, checkOpts)
	} else {
		err = monitor.CheckKVBucketAndKeyWithConnection(nc, check, checkOpts)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkSrv(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: c.srvName, Check: "server", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckServerOptions{
		Name:                   c.srvName,
		CPUWarning:             c.srvCPUWarn,
		CPUCritical:            c.srvCPUCrit,
		MemoryWarning:          c.srvMemWarn,
		MemoryCritical:         c.srvMemCrit,
		ConnectionsWarning:     c.srvConnWarn,
		ConnectionsCritical:    c.srvConnCrit,
		SubscriptionsWarning:   c.srvSubsWarn,
		SubscriptionsCritical:  c.srvSubCrit,
		UptimeWarning:          c.srvUptimeWarn.Seconds(),
		UptimeCritical:         c.srvUptimeCrit.Seconds(),
		AuthenticationRequired: c.srvAuthRequired,
		TLSRequired:            c.srvTLSRequired,
		JetStreamRequired:      c.srvJSRequired,
		TLSExpireWarning:       c.srvtlsExpiredWarn.String(),
		TLSExpireCritical:      c.srvtlsExpiredCrit.String(),
	}

	var err error
	nc := opts().Conn

	if nc == nil {
		err = monitor.CheckServer(opts().Config.ServerURL(), natsOpts(), check, opts().Timeout, checkOpts)
	} else {
		err = monitor.CheckServerWithConnection(nc, check, opts().Timeout, checkOpts)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkJS(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: "JetStream", Check: "jetstream", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckJetStreamAccountOptions{
		MemoryWarning:       c.jsMemWarn,
		MemoryCritical:      c.jsMemCritical,
		FileWarning:         c.jsStoreWarn,
		FileCritical:        c.jsStoreCritical,
		StreamWarning:       c.jsStreamsWarn,
		StreamCritical:      c.jsStreamsCritical,
		ConsumersWarning:    c.jsConsumersWarn,
		ConsumersCritical:   c.jsConsumersCritical,
		CheckReplicas:       c.jsReplicas,
		ReplicaSeenCritical: c.jsReplicaSeenCritical.Seconds(),
		ReplicaLagCritical:  c.jsReplicaLagCritical,
	}

	var err error
	mgr := opts().Mgr

	if mgr == nil {
		err = monitor.CheckJetStreamAccount(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, checkOpts)
	} else {
		err = monitor.CheckJetStreamAccountWithConnection(mgr, check, checkOpts)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkRaft(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: "JetStream Meta Cluster", Check: "meta", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckJetstreamMetaOptions{
		ExpectServers: c.raftExpect,
		LagCritical:   c.raftLagCritical,
		SeenCritical:  c.raftSeenCritical.Seconds(),
	}

	var err error
	nc := opts().Conn

	if nc == nil {
		err = monitor.CheckJetstreamMeta(opts().Config.ServerURL(), natsOpts(), check, checkOpts)
	} else {
		err = monitor.CheckJetstreamMetaWithConnection(nc, check, checkOpts)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkStream(cmd *cobra.Command, _ []string) error {
	c.sourcesLagCriticalIsSet = cmd.Flags().Changed("lag-critical")
	c.sourcesSeenCriticalIsSet = cmd.Flags().Changed("seen-critical")
	c.sourcesMinSourcesIsSet = cmd.Flags().Changed("min-sources")
	c.sourcesMaxSourcesIsSet = cmd.Flags().Changed("max-sources")
	c.raftExpectIsSet = cmd.Flags().Changed("peer-expect")
	c.raftLagCriticalIsSet = cmd.Flags().Changed("peer-lag-critical")
	c.raftSeenCriticalIsSet = cmd.Flags().Changed("peer-seen-critical")
	c.streamMessagesWarnIsSet = cmd.Flags().Changed("msgs-warn")
	c.streamMessagesCritIsSet = cmd.Flags().Changed("msgs-critical")
	c.subjectsWarnIsSet = cmd.Flags().Changed("subjects-warn")
	c.subjectsCritIsSet = cmd.Flags().Changed("subjects-critical")

	check := &monitor.Result{Name: c.sourcesStream, Check: "stream", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckStreamHealthOptions{
		StreamName: c.sourcesStream,
	}

	if c.sourcesLagCriticalIsSet {
		checkOpts.SourcesLagCritical = c.sourcesLagCritical
	}
	if c.sourcesSeenCriticalIsSet {
		checkOpts.SourcesSeenCritical = c.sourcesSeenCritical.Seconds()
	}
	if c.sourcesMinSourcesIsSet {
		checkOpts.MinSources = c.sourcesMinSources
	}
	if c.sourcesMaxSourcesIsSet {
		checkOpts.MaxSources = c.sourcesMaxSources
	}
	if c.raftExpectIsSet {
		checkOpts.ClusterExpectedPeers = c.raftExpect
	}
	if c.raftLagCriticalIsSet {
		checkOpts.ClusterLagCritical = c.raftLagCritical
	}
	if c.raftSeenCriticalIsSet {
		checkOpts.ClusterSeenCritical = c.raftSeenCritical.Seconds()
	}
	if c.streamMessagesWarnIsSet {
		checkOpts.MessagesWarn = c.streamMessagesWarn
	}
	if c.streamMessagesCritIsSet {
		checkOpts.MessagesCrit = c.streamMessagesCrit
	}
	if c.subjectsWarnIsSet {
		checkOpts.SubjectsWarn = c.subjectsWarn
	}
	if c.subjectsCritIsSet {
		checkOpts.SubjectsCrit = c.subjectsCrit
	}

	logger := api.NewDiscardLogger()
	if opts().Trace {
		logger = api.NewDefaultLogger(api.TraceLevel)
	}

	var err error
	mgr := opts().Mgr

	if mgr == nil {
		err = monitor.CheckStreamHealth(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, checkOpts, logger)
	} else {
		err = monitor.CheckStreamHealthWithConnection(mgr, check, checkOpts, logger)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkMsg(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: "Stream Message", Check: "message", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckStreamMessageOptions{
		StreamName:      c.sourcesStream,
		Subject:         c.msgSubject,
		AgeWarning:      c.msgAgeWarn.Seconds(),
		AgeCritical:     c.msgAgeCrit.Seconds(),
		Content:         c.msgRegexp.String(),
		BodyAsTimestamp: c.msgBodyAsTs,
	}

	var err error
	mgr := opts().Mgr

	if mgr == nil {
		err = monitor.CheckStreamMessage(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, checkOpts)
	} else {
		err = monitor.CheckStreamMessageWithConnection(mgr, check, checkOpts)
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkConnection(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: "Connection", Check: "connections", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	if opts().Config == nil {
		err := loadContext(false)
		if check.CriticalIfErrf(err, "loading context failed: %v", err) {
			return nil
		}
	}

	checkOpts := monitor.CheckConnectionOptions{
		ConnectTimeWarning:  c.connectWarning.Seconds(),
		ConnectTimeCritical: c.connectCritical.Seconds(),
		ServerRttWarning:    c.rttWarning.Seconds(),
		ServerRttCritical:   c.rttCritical.Seconds(),
		RequestRttWarning:   c.reqWarning.Seconds(),
		RequestRttCritical:  c.reqCritical.Seconds(),
	}

	var err error
	nc := opts().Conn

	if nc == nil {
		err = monitor.CheckConnection(opts().Config.ServerURL(), natsOpts(), opts().Timeout, check, checkOpts)
	} else {
		err = fmt.Errorf("connection checks are not supported when a connection is supplied")
	}
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}

func (c *SrvCheckCmd) checkCredentialAction(_ *cobra.Command, _ []string) error {
	check := &monitor.Result{Name: "Credential", Check: "credential", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	err := monitor.CheckCredential(check, monitor.CheckCredentialOptions{
		File:             c.credential,
		ValidityWarning:  c.credentialValidityWarn.Seconds(),
		ValidityCritical: c.credentialValidityCrit.Seconds(),
		RequiresExpiry:   c.credentialRequiresExpire,
	})
	check.CriticalIfErrf(err, "Check failed: %v", err)

	return nil
}
