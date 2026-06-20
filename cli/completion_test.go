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
	"testing"
	"time"

	"github.com/nats-io/natscli/options"
)

func TestCompletionCache(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	options.DefaultOptions = &options.Options{}

	const key = "ctx-streams"

	t.Run("miss then hit within TTL", func(t *testing.T) {
		t.Setenv("NATS_COMPLETION_CACHE_TTL", "1h")
		if _, ok := readCompletionCache(key); ok {
			t.Fatal("expected miss on empty cache")
		}

		writeCompletionCache(key, []string{"ORDERS", "SHIPMENTS"})

		v, ok := readCompletionCache(key)
		if !ok {
			t.Fatal("expected hit after write")
		}
		if len(v) != 2 || v[0] != "ORDERS" || v[1] != "SHIPMENTS" {
			t.Fatalf("unexpected cached values: %v", v)
		}
	})

	t.Run("expires after TTL", func(t *testing.T) {
		t.Setenv("NATS_COMPLETION_CACHE_TTL", "1ms")
		writeCompletionCache(key, []string{"ORDERS"})
		time.Sleep(3 * time.Millisecond)
		if _, ok := readCompletionCache(key); ok {
			t.Fatal("expected miss after TTL expiry")
		}
	})

	t.Run("disabled when TTL is zero", func(t *testing.T) {
		t.Setenv("NATS_COMPLETION_CACHE_TTL", "0")
		writeCompletionCache("disabled-key", []string{"ORDERS"})
		if _, ok := readCompletionCache("disabled-key"); ok {
			t.Fatal("expected cache to be disabled with TTL=0")
		}
	})
}

func TestMatchCompletionPrefix(t *testing.T) {
	names := []string{"ORDERS", "ORDERS_NEW", "SHIPMENTS"}

	if got := matchCompletionPrefix(names, ""); len(got) != 3 {
		t.Fatalf("empty prefix should return all, got %v", got)
	}

	got := matchCompletionPrefix(names, "ORD")
	if len(got) != 2 || got[0] != "ORDERS" || got[1] != "ORDERS_NEW" {
		t.Fatalf("prefix ORD: unexpected %v", got)
	}

	if got := matchCompletionPrefix(names, "X"); len(got) != 0 {
		t.Fatalf("non-matching prefix should return none, got %v", got)
	}
}

func TestCompletionContextKeySanitized(t *testing.T) {
	options.DefaultOptions = &options.Options{CfgCtx: "my/weird ctx"}
	if key := completionContextKey(); key != "my_weird_ctx" {
		t.Fatalf("expected sanitized key, got %q", key)
	}
}
