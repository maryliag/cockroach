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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// uniqueIndexRecInfoLimit is the limit on number of unique index
// recommendations info we can store in memory.
var uniqueIndexRecInfoLimit *settings.IntSetting

// IndexRecKey the key used for the recommendations cache.
type IndexRecKey struct {
	StmtNoConstants string
	Database        string
	PlanHash        uint64
}

// IndexRecInfo is the information saved on cache per recommendation.
type IndexRecInfo struct {
	LastGeneratedTs time.Time
	Recommendations []string
}

// IndexRecCache stores the map of index recommendations keys (fingerprint, database, planHash) and
// information (lastGeneratedTs, recommendations, executionCount).
type IndexRecCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex

		// idxRecommendations stores index recommendations per indexRecKey.
		idxRecommendations map[IndexRecKey]IndexRecInfo
	}

	atomic struct {
		// uniqueIndexRecInfo is the number of unique index recommendations info
		// we are storing in memory.
		uniqueIndexRecInfo int64
	}
}

// NewIndexRecommendationsCache creates a new map to be used as a cache for index recommendations.
func NewIndexRecommendationsCache(
	setting *cluster.Settings, uniqueIdxRecInfoLimit *settings.IntSetting,
) *IndexRecCache {
	idxRecCache := &IndexRecCache{
		st: setting,
	}
	idxRecCache.mu.idxRecommendations = make(map[IndexRecKey]IndexRecInfo)
	uniqueIndexRecInfoLimit = uniqueIdxRecInfoLimit
	return idxRecCache
}

// ShouldGenerateIndexRecommendation returns true if there was no generation in the past hour
// and there is at least 5 executions of the same fingerprint/database/planHash combination.
// TODO (marylia) update comment with final decided count
func (idxRec *IndexRecCache) ShouldGenerateIndexRecommendation(
	fingerprint string, planHash uint64, database string, stmtType tree.StatementType,
) bool {
	if !idxRec.statementCanHaveRecommendation(stmtType) {
		return false
	}

	idxKey := IndexRecKey{
		StmtNoConstants: fingerprint,
		Database:        database,
		PlanHash:        planHash,
	}
	recInfo, found := idxRec.GetOrCreateIndexRecommendation(idxKey)
	// If we couldn't find or create, don't generate recommendations.
	if !found {
		return false
	}

	return timeutil.Since(recInfo.LastGeneratedTs).Hours() >= 1
}

// UpdateIndexRecommendations updates the values for index recommendations.
// If reset is true, a new recommendation was generated, so reset the execution counter and
// lastGeneratedTs, otherwise just increment the executionCount.
func (idxRec *IndexRecCache) UpdateIndexRecommendations(
	fingerprint string,
	planHash uint64,
	database string,
	stmtType tree.StatementType,
	recommendations []string,
) []string {
	if !idxRec.statementCanHaveRecommendation(stmtType) {
		return recommendations
	}

	idxKey := IndexRecKey{
		StmtNoConstants: fingerprint,
		Database:        database,
		PlanHash:        planHash,
	}

	idxRec.setIndexRecommendations(idxKey, timeutil.Now(), recommendations)
	return recommendations
}

// statementCanHaveRecommendation returns true if that type of statement can have recommendations
// generated for it. We only want to recommend if the statement is DML and recommendations are enabled.
func (idxRec *IndexRecCache) statementCanHaveRecommendation(stmtType tree.StatementType) bool {
	if !sqlstats.SampleIndexRecommendation.Get(&idxRec.st.SV) || stmtType != tree.TypeDML {
		return false
	}

	return true
}

func (idxRec *IndexRecCache) getIndexRecommendation(key IndexRecKey) (IndexRecInfo, bool) {
	idxRec.mu.RLock()
	defer idxRec.mu.RUnlock()

	recInfo, found := idxRec.mu.idxRecommendations[key]

	return recInfo, found
}

// GetOrCreateIndexRecommendation gets the value from cache or creates one if it doesn't exists.
func (idxRec *IndexRecCache) GetOrCreateIndexRecommendation(key IndexRecKey) (IndexRecInfo, bool) {
	recInfo, found := idxRec.getIndexRecommendation(key)
	if found {
		return recInfo, true
	}

	// If it was not found, check if a new entry can be created, without
	// passing the limit of unique index recommendations from the cache.
	limit := uniqueIndexRecInfoLimit.Get(&idxRec.st.SV)
	incrementedCount :=
		atomic.AddInt64(&idxRec.atomic.uniqueIndexRecInfo, int64(1))

	if incrementedCount > limit {
		// If we have exceeded limit of unique index recommendations, then delete data.
		idxRec.clearOldIdxRecommendations()
	}

	idxRec.mu.Lock()
	defer idxRec.mu.Unlock()
	// For a new entry, we want the lastGeneratedTs to be in the past, in case we reach
	// the execution count, we should generate new recommendations.
	recInfo = IndexRecInfo{
		LastGeneratedTs: timeutil.Now().Add(-time.Hour),
		Recommendations: []string{},
	}
	idxRec.mu.idxRecommendations[key] = recInfo

	return recInfo, true
}

func (idxRec *IndexRecCache) setIndexRecommendations(
	key IndexRecKey, time time.Time, recommendations []string,
) {
	_, found := idxRec.GetOrCreateIndexRecommendation(key)

	if found {
		idxRec.mu.Lock()
		defer idxRec.mu.Unlock()

		idxRec.mu.idxRecommendations[key] = IndexRecInfo{
			LastGeneratedTs: time,
			Recommendations: recommendations,
		}
	}
}

// clearOldIdxRecommendations clear entries that was last updated
// more than a day ago. Returns the total deleted entries.
func (idxRec *IndexRecCache) clearOldIdxRecommendations() {
	idxRec.mu.Lock()
	defer idxRec.mu.Unlock()

	deleted := 0
	for key, value := range idxRec.mu.idxRecommendations {
		if timeutil.Since(value.LastGeneratedTs).Hours() >= 24 {
			delete(idxRec.mu.idxRecommendations, key)
			deleted++
		}
	}
	if deleted < 1000 {
		for key := range idxRec.mu.idxRecommendations {
			delete(idxRec.mu.idxRecommendations, key)
			deleted++
			if deleted > 1000 {
				break
			}
		}
	}
	atomic.AddInt64(&idxRec.atomic.uniqueIndexRecInfo, int64(-deleted))
}
