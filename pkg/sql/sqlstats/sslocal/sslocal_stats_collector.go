// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

type statsCollector struct {
	sqlstats.WriterIterator

	// aggregatedStorage has aggregated data ready to be flushed.
	aggregatedStorage sqlstats.WriterIterator

	// phaseTimes tracks session-level phase times.
	phaseTimes *sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes *sessionphase.Times

	// isImplicit tracks is the transaction is explicit or implicit.
	isImplicit bool

	// TODO marylia
	settings *cluster.Settings
}

var _ sqlstats.StatsCollector = &statsCollector{}

// NewStatsCollector returns an instance of sqlstats.StatsCollector.
func NewStatsCollector(
	writer sqlstats.WriterIterator, phaseTime *sessionphase.Times, st *cluster.Settings,
) sqlstats.StatsCollector {
	return &statsCollector{
		WriterIterator:    writer,
		phaseTimes:        phaseTime.Clone(),
		aggregatedStorage: nil,
		isImplicit:        true,
		settings:          st,
	}
}

// PhaseTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) PhaseTimes() *sessionphase.Times {
	return s.phaseTimes
}

// PreviousPhaseTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) PreviousPhaseTimes() *sessionphase.Times {
	return s.previousPhaseTimes
}

// IsImplicit implements sqlstats.StatsCollector interface.
func (s *statsCollector) IsImplicit() bool {
	return s.isImplicit
}

// SetImplicit implements sqlstats.StatsCollector interface.
func (s *statsCollector) SetImplicit(isImplicit bool, st *cluster.Settings) {
	s.isImplicit = isImplicit

	if !isImplicit {
		s.aggregatedStorage = s.WriterIterator
		// TODO marylia
		s.WriterIterator = ssmemstorage.NewTemporaryContainer(st)
	}
}

//func (s *statsCollector) RecordTransaction(
//	ctx context.Context, key roachpb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
//) error {
//	err := s.Container.RecordTransaction(ctx, key, value)
//	if err != nil {
//		return err
//	}
//	if s.isImplicit {
//		return nil
//	}
//
//	// TODO marylia rewrite txnid for all statements
//
//	return s.aggregatedStorage.Add(ctx, s.Container)
//}

// ResetWriter implements sqlstats.StatsCollector interface.
func (s *statsCollector) ResetWriter(ctx context.Context, writer sqlstats.WriterIterator) {
	if s.isImplicit {
		*s = statsCollector{
			WriterIterator:     writer,
			previousPhaseTimes: s.previousPhaseTimes,
			phaseTimes:         s.phaseTimes,
			aggregatedStorage:  s.aggregatedStorage,
			isImplicit:         true,
		}
	} else {
		s.Finalize(ctx)
		*s = statsCollector{
			WriterIterator:     ssmemstorage.NewTemporaryContainer(s.settings),
			previousPhaseTimes: s.previousPhaseTimes,
			phaseTimes:         s.phaseTimes,
			aggregatedStorage:  writer,
			isImplicit:         false,
		}
	}
}

// ResetTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) ResetTimes(phaseTime *sessionphase.Times) {
	*s = statsCollector{
		WriterIterator:     s.WriterIterator,
		previousPhaseTimes: s.phaseTimes,
		phaseTimes:         phaseTime.Clone(),
		aggregatedStorage:  s.aggregatedStorage,
		isImplicit:         s.isImplicit,
	}
}

// Finalize implements sqlstats.StatsCollector interface.
func (s *statsCollector) Finalize(ctx context.Context) error {
	if !s.isImplicit {
		s.aggregatedStorage.Merge(s.WriterIterator, ctx)

		*s = statsCollector{
			WriterIterator:     s.aggregatedStorage,
			previousPhaseTimes: s.previousPhaseTimes,
			phaseTimes:         s.phaseTimes,
			aggregatedStorage:  s.aggregatedStorage,
			isImplicit:         s.isImplicit,
		}
	}
	return nil
}
