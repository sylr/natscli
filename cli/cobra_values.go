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
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	iu "github.com/nats-io/natscli/internal/util"
)

// These implement spf13/pflag's Value interface (String/Set/Type) for the value
// kinds that fisk supported but pflag does not provide natively. They are
// registered via cmd.Flags().Var(...) / VarP(...). The Type() string is used by
// the help renderers as the human readable type hint, matching fisk's output.

// enumValue restricts a string flag/arg to a fixed set of options.
type enumValue struct {
	target  *string
	options []string
}

// newEnumValue presets the target to dflt (use "" for no default) and limits Set to options.
func newEnumValue(target *string, dflt string, options ...string) *enumValue {
	*target = dflt
	return &enumValue{target: target, options: options}
}

func (e *enumValue) Set(s string) error {
	for _, o := range e.options {
		if s == o {
			*e.target = s
			return nil
		}
	}
	return fmt.Errorf("must be one of %s but got %q", strings.Join(e.options, ","), s)
}

func (e *enumValue) String() string { return *e.target }
func (e *enumValue) Type() string   { return "enum(" + strings.Join(e.options, "|") + ")" }

// enumsValue restricts each value of a repeatable string flag/arg to a fixed set.
type enumsValue struct {
	target  *[]string
	options []string
	set     bool
}

func newEnumsValue(target *[]string, options ...string) *enumsValue {
	return &enumsValue{target: target, options: options}
}

func (e *enumsValue) Set(s string) error {
	found := false
	for _, o := range e.options {
		if s == o {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("must be one of %s but got %q", strings.Join(e.options, ","), s)
	}

	// First user-supplied value replaces any default, subsequent values accumulate.
	if !e.set {
		*e.target = []string{}
		e.set = true
	}
	*e.target = append(*e.target, s)
	return nil
}

func (e *enumsValue) String() string { return strings.Join(*e.target, ",") }
func (e *enumsValue) Type() string   { return "enum(" + strings.Join(e.options, "|") + ")" }

// existingFileValue is a string that must reference an existing file.
type existingFileValue struct{ target *string }

func newExistingFileValue(target *string) *existingFileValue { return &existingFileValue{target} }

func (f *existingFileValue) Set(s string) error {
	if s != "" {
		stat, err := os.Stat(s)
		if err != nil {
			return fmt.Errorf("path %q does not exist", s)
		}
		if stat.IsDir() {
			return fmt.Errorf("%q is a directory", s)
		}
	}
	*f.target = s
	return nil
}

func (f *existingFileValue) String() string { return *f.target }
func (f *existingFileValue) Type() string   { return "path" }

// existingFilesValue is a repeatable string where each value must reference an existing file.
type existingFilesValue struct {
	target *[]string
	set    bool
}

func newExistingFilesValue(target *[]string) *existingFilesValue {
	return &existingFilesValue{target: target}
}

func (f *existingFilesValue) Set(s string) error {
	if s != "" {
		stat, err := os.Stat(s)
		if err != nil {
			return fmt.Errorf("path %q does not exist", s)
		}
		if stat.IsDir() {
			return fmt.Errorf("%q is a directory", s)
		}
	}
	if !f.set {
		*f.target = []string{}
		f.set = true
	}
	*f.target = append(*f.target, s)
	return nil
}

func (f *existingFilesValue) String() string { return strings.Join(*f.target, ",") }
func (f *existingFilesValue) Type() string   { return "path" }

// existingDirValue is a string that must reference an existing directory.
type existingDirValue struct{ target *string }

func newExistingDirValue(target *string) *existingDirValue { return &existingDirValue{target} }

func (d *existingDirValue) Set(s string) error {
	if s != "" {
		stat, err := os.Stat(s)
		if err != nil {
			return fmt.Errorf("path %q does not exist", s)
		}
		if !stat.IsDir() {
			return fmt.Errorf("%q is not a directory", s)
		}
	}
	*d.target = s
	return nil
}

func (d *existingDirValue) String() string { return *d.target }
func (d *existingDirValue) Type() string   { return "path" }

// stringMapValue collects KEY=VALUE (or KEY:VALUE) pairs into a map.
type stringMapValue struct {
	target *map[string]string
	set    bool
}

func newStringMapValue(target *map[string]string) *stringMapValue {
	return &stringMapValue{target: target}
}

var stringMapSplit = regexp.MustCompile(`[:=]`)

func (m *stringMapValue) Set(s string) error {
	parts := stringMapSplit.Split(s, 2)
	if len(parts) != 2 {
		return fmt.Errorf("expected KEY=VALUE got %q", s)
	}
	if !m.set {
		*m.target = map[string]string{}
		m.set = true
	}
	if *m.target == nil {
		*m.target = map[string]string{}
	}
	(*m.target)[parts[0]] = parts[1]
	return nil
}

func (m *stringMapValue) String() string {
	if m.target == nil || *m.target == nil {
		return ""
	}
	var pairs []string
	for k, v := range *m.target {
		pairs = append(pairs, k+"="+v)
	}
	return strings.Join(pairs, ",")
}

func (m *stringMapValue) Type() string { return "key=value" }

// urlValue parses a single URL.
type urlValue struct{ target **url.URL }

func newURLValue(target **url.URL) *urlValue { return &urlValue{target} }

func (u *urlValue) Set(s string) error {
	parsed, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", s, err)
	}
	*u.target = parsed
	return nil
}

func (u *urlValue) String() string {
	if u.target == nil || *u.target == nil {
		return ""
	}
	return (*u.target).String()
}

func (u *urlValue) Type() string { return "url" }

// urlListValue parses a repeatable list of URLs.
type urlListValue struct {
	target *[]*url.URL
	set    bool
}

func newURLListValue(target *[]*url.URL) *urlListValue { return &urlListValue{target: target} }

func (u *urlListValue) Set(s string) error {
	parsed, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", s, err)
	}
	if !u.set {
		*u.target = []*url.URL{}
		u.set = true
	}
	*u.target = append(*u.target, parsed)
	return nil
}

func (u *urlListValue) String() string {
	if u.target == nil {
		return ""
	}
	var out []string
	for _, v := range *u.target {
		out = append(out, v.String())
	}
	return strings.Join(out, ",")
}

func (u *urlListValue) Type() string { return "url" }

// regexpValue compiles a regular expression.
type regexpValue struct{ target **regexp.Regexp }

func newRegexpValue(target **regexp.Regexp) *regexpValue { return &regexpValue{target} }

func (r *regexpValue) Set(s string) error {
	re, err := regexp.Compile(s)
	if err != nil {
		return fmt.Errorf("invalid regular expression %q: %w", s, err)
	}
	*r.target = re
	return nil
}

func (r *regexpValue) String() string {
	if r.target == nil || *r.target == nil {
		return ""
	}
	return (*r.target).String()
}

func (r *regexpValue) Type() string { return "regexp" }

// validatedInt64Value is an int64 flag that runs an OptionValidator before accepting a value.
type validatedInt64Value struct {
	target    *int64
	validator iu.OptionValidator
}

func newValidatedInt64Value(target *int64, dflt int64, validator iu.OptionValidator) *validatedInt64Value {
	*target = dflt
	return &validatedInt64Value{target: target, validator: validator}
}

func (v *validatedInt64Value) Set(s string) error {
	if v.validator != nil {
		if err := v.validator(s); err != nil {
			return err
		}
	}
	iv, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*v.target = iv
	return nil
}

func (v *validatedInt64Value) String() string { return strconv.FormatInt(*v.target, 10) }
func (v *validatedInt64Value) Type() string   { return "int" }
