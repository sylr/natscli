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
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// appName is the program name used in error output, matching the cobra root command name.
const appName = "nats"

// terminate is the exit hook, overridable in tests.
var terminate = os.Exit

// errorf prints a formatted error message to stderr using the same layout fisk used.
func errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, appName+": error: "+format+"\n", args...)
}

// fatalf prints a formatted error message to stderr and exits with status 1.
func fatalf(format string, args ...any) {
	errorf(format, args...)
	terminate(1)
}

// fatalIfError prints an error and exits if err is not nil. The error is printed
// with the given prefix, matching fisk.FatalIfError.
func fatalIfError(err error, format string, args ...any) {
	if err == nil {
		return
	}

	prefix := ""
	if format != "" {
		prefix = fmt.Sprintf(format, args...) + ": "
	}
	errorf(prefix+"%s", err)
	terminate(1)
}

var (
	durationMatcher    = regexp.MustCompile(`([-+]?)(([\d\.]+)([a-zA-Z]+))`)
	errInvalidDuration = fmt.Errorf("invalid duration")
)

// parseDuration parses durations with additional units over those from the
// standard go parser. In addition to the normal go units it supports:
//
//   - "w", "W" - a week based on 7 days of exactly 24 hours
//   - "d", "D" - a day based on 24 hours
//   - "M"      - a month made of 30 days of 24 hours
//   - "y", "Y" - a year made of 365 days of 24 hours each
//
// This is a port of fisk.ParseDuration. It makes no attempt to correct for leap
// years or leap seconds.
func parseDuration(d string) (time.Duration, error) {
	// golang time.ParseDuration has a special case for 0
	if d == "0" {
		return 0 * time.Second, nil
	}

	var (
		r   time.Duration
		neg = 1
	)

	d = strings.TrimSpace(d)

	if len(d) == 0 {
		return r, errInvalidDuration
	}

	parts := durationMatcher.FindAllStringSubmatch(d, -1)
	if len(parts) == 0 {
		return r, errInvalidDuration
	}

	for i, p := range parts {
		if len(p) != 5 {
			return 0, errInvalidDuration
		}

		if i == 0 && p[1] == "-" {
			neg = -1
		}

		switch p[4] {
		case "w", "W":
			val, err := strconv.ParseFloat(p[3], 32)
			if err != nil {
				return 0, fmt.Errorf("%w: %v", errInvalidDuration, err)
			}

			r += time.Duration(val*7*24) * time.Hour

		case "d", "D":
			val, err := strconv.ParseFloat(p[3], 32)
			if err != nil {
				return 0, fmt.Errorf("%w: %v", errInvalidDuration, err)
			}

			r += time.Duration(val*24) * time.Hour

		case "M":
			val, err := strconv.ParseFloat(p[3], 32)
			if err != nil {
				return 0, fmt.Errorf("%w: %v", errInvalidDuration, err)
			}

			r += time.Duration(val*24*30) * time.Hour

		case "Y", "y":
			val, err := strconv.ParseFloat(p[3], 32)
			if err != nil {
				return 0, fmt.Errorf("%w: %v", errInvalidDuration, err)
			}

			r += time.Duration(val*24*365) * time.Hour

		case "ns", "us", "µs", "ms", "s", "m", "h":
			dur, err := time.ParseDuration(p[2])
			if err != nil {
				return 0, fmt.Errorf("%w: %v", errInvalidDuration, err)
			}

			r += dur
		default:
			return 0, fmt.Errorf("%w: invalid unit %v", errInvalidDuration, p[4])
		}
	}

	return time.Duration(neg) * r, nil
}
