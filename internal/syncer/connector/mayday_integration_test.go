//
// Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//go:build integration

package connector

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"
)

// TestMaydayConnectorLiveAPI runs test connection, a full sync, a resume, and a
// prune snapshot against a real Mayday account. It needs MAYDAY_CLIENT_ID and
// MAYDAY_CLIENT_SECRET; MAYDAY_LANGUAGES, MAYDAY_COLLECTIONS, MAYDAY_BASE_URL,
// and MAYDAY_TOKEN_URL are optional.
func TestMaydayConnectorLiveAPI(t *testing.T) {
	clientID, clientSecret := os.Getenv("MAYDAY_CLIENT_ID"), os.Getenv("MAYDAY_CLIENT_SECRET")
	if clientID == "" || clientSecret == "" {
		t.Skip("MAYDAY_CLIENT_ID and MAYDAY_CLIENT_SECRET are not set")
	}
	conn, err := NewMaydayConnector(map[string]any{
		"credentials": map[string]any{"client_id": clientID, "client_secret": clientSecret},
		"languages":   os.Getenv("MAYDAY_LANGUAGES"),
		"collections": os.Getenv("MAYDAY_COLLECTIONS"),
		"base_url":    os.Getenv("MAYDAY_BASE_URL"),
		"token_url":   os.Getenv("MAYDAY_TOKEN_URL"),
		"batch_size":  5,
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := conn.ValidateConnectorSetting(ctx, nil); err != nil {
		t.Fatalf("ValidateConnectorSetting: %v", err)
	}

	session, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	defer session.Close()
	first, err := session.NextBatch(ctx)
	if err != nil {
		t.Fatalf("first NextBatch: %v", err)
	}
	docs := append(slices.Clone(first.Documents), drainMaydaySync(t, session)...)
	synced := map[string]struct{}{}
	for _, doc := range docs {
		if _, dup := synced[doc.SourceID]; dup {
			t.Fatalf("source ID %s emitted twice", doc.SourceID)
		}
		synced[doc.SourceID] = struct{}{}
		if len(doc.Blob) == 0 || doc.UpdatedAt.IsZero() || doc.Fingerprint == "" {
			t.Fatalf("incomplete document %s: blob=%d updatedAt=%v", doc.SourceID, len(doc.Blob), doc.UpdatedAt)
		}
		if outdated := doc.Metadata["is_outdated"]; outdated != "true" && outdated != "false" {
			t.Fatalf("document %s has is_outdated %#v, want \"true\" or \"false\"", doc.SourceID, outdated)
		}
	}
	t.Logf("full sync: %d documents", len(docs))

	if len(docs) > len(first.Documents) {
		resumed, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true, Resume: first.Checkpoint})
		if err != nil {
			t.Fatalf("OpenSync with resume: %v", err)
		}
		defer resumed.Close()
		resumedIDs := maydayDocIDs(drainMaydaySync(t, resumed))
		if anchor := first.Documents[len(first.Documents)-1].SourceID; slices.Contains(resumedIDs, anchor) {
			t.Fatalf("resume re-emitted the committed anchor %s", anchor)
		}
		for _, doc := range docs[len(first.Documents):] {
			if !slices.Contains(resumedIDs, doc.SourceID) {
				t.Fatalf("resume missed uncommitted document %s", doc.SourceID)
			}
		}
	}

	prune, err := conn.OpenPrune(ctx, PruneRequest{})
	if err != nil {
		t.Fatalf("OpenPrune: %v", err)
	}
	defer prune.Close()
	retained := drainMaydayPrune(t, prune)
	for sourceID := range synced {
		if !slices.Contains(retained, sourceID) {
			t.Fatalf("prune snapshot is missing synced document %s", sourceID)
		}
	}
	t.Logf("prune snapshot: %d documents", len(retained))
}
