// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxrecommendations

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
)

// IndexRecCache stores the map of index recommendations keys (fingerprint, database, planHash) and
// information (lastGeneratedTs, recommendations, executionCount).
type IndexRecCache struct {
	st *cluster.Settings
}

// NewIndexRecommendationsCache creates a new map to be used as a cache for index recommendations.
func NewIndexRecommendationsCache(
	setting *cluster.Settings, uniqueIdxRecInfoLimit *settings.IntSetting,
) *IndexRecCache {
	idxRecCache := &IndexRecCache{
		st: setting,
	}
	return idxRecCache
}

// ShouldGenerateIndexRecommendation returns true if the stmt is DML and the cluster setting is enabled
func (idxRec *IndexRecCache) ShouldGenerateIndexRecommendation(stmtType tree.StatementType) bool {
	if !sqlstats.SampleIndexRecommendation.Get(&idxRec.st.SV) || stmtType != tree.TypeDML {
		return false
	}

	return true
}
