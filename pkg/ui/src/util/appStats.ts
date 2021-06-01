// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file significantly duplicates the algorithms available in
// pkg/roachpb/app_stats.go, in particular the functions on NumericStats
// to compute variance and add together NumericStats.

import _ from "lodash";
import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";
import { uniqueLong } from "src/util/arrays";

export type ISensitiveInfo = protos.cockroach.sql.ISensitiveInfo;
export type StatementStatistics = protos.cockroach.sql.IStatementStatistics;
export type ExecStats = protos.cockroach.sql.IExecStats;
export type CollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
export type CollectedTransactionStatistics = protos.cockroach.sql.CollectedTransactionStatistics;
export type ICollectedTransactionStatistics = protos.cockroach.sql.ICollectedTransactionStatistics;
export type IExtendedCollectedTransactionStatistics = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
export type IExtendedStatementStatisticsKey = protos.cockroach.server.serverpb.StatementsResponse.IExtendedStatementStatisticsKey;

export interface NumericStat {
  mean?: number;
  squared_diffs?: number;
}

// aggregateNumericStats computes a new `NumericStat` instance from 2 arguments by using the counts
// to generate new `mean` and `squared_diffs` values.
export function aggregateNumericStats(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
) {
  const total = countA + countB;
  const delta = b.mean - a.mean;

  return {
    mean: (a.mean * countA + b.mean * countB) / total,
    squared_diffs:
      a.squared_diffs +
      b.squared_diffs +
      (delta * delta * countA * countB) / total,
  };
}

export interface Transaction extends IExtendedCollectedTransactionStatistics {
  fingerprint: string;
  statements: CollectedStatementStatistics[];
}

export function variance(stat: NumericStat, count: number) {
  return stat.squared_diffs / (count - 1);
}

export function stdDev(stat: NumericStat, count: number) {
  return Math.sqrt(variance(stat, count)) || 0;
}

export function stdDevLong(stat: NumericStat, count: number | Long) {
  return stdDev(stat, FixLong(count).toInt());
}

export function addNumericStats(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
) {
  const total = countA + countB;
  const delta = b.mean - a.mean;

  return {
    mean: (a.mean * countA + b.mean * countB) / total,
    squared_diffs:
      a.squared_diffs +
      b.squared_diffs +
      (delta * delta * countA * countB) / total,
  };
}

export function addStatementStats(
  a: StatementStatistics,
  b: StatementStatistics,
): Required<StatementStatistics> {
  const countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  return {
    count: a.count.add(b.count),
    first_attempt_count: a.first_attempt_count.add(b.first_attempt_count),
    max_retries: a.max_retries.greaterThan(b.max_retries)
      ? a.max_retries
      : b.max_retries,
    num_rows: addNumericStats(a.num_rows, b.num_rows, countA, countB),
    parse_lat: addNumericStats(a.parse_lat, b.parse_lat, countA, countB),
    plan_lat: addNumericStats(a.plan_lat, b.plan_lat, countA, countB),
    run_lat: addNumericStats(a.run_lat, b.run_lat, countA, countB),
    service_lat: addNumericStats(a.service_lat, b.service_lat, countA, countB),
    overhead_lat: addNumericStats(
      a.overhead_lat,
      b.overhead_lat,
      countA,
      countB,
    ),
    bytes_read: addNumericStats(a.bytes_read, b.bytes_read, countA, countB),
    rows_read: addNumericStats(a.rows_read, b.rows_read, countA, countB),
    sensitive_info: coalesceSensitiveInfo(a.sensitive_info, b.sensitive_info),
    legacy_last_err: "",
    legacy_last_err_redacted: "",
    exec_stats: addExecStats(a.exec_stats, b.exec_stats),
    sql_type: a.sql_type,
    last_exec_timestamp:
      a.last_exec_timestamp.seconds > b.last_exec_timestamp.seconds
        ? a.last_exec_timestamp
        : b.last_exec_timestamp,
    nodes: uniqueLong([...a.nodes, ...b.nodes]),
  };
}

function addExecStats(a: ExecStats, b: ExecStats): Required<ExecStats> {
  let countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  if (countA === 0 && countB === 0) {
    // If both counts are zero, artificially set the one count to one to avoid
    // division by zero when calculating the mean in addNumericStats.
    countA = 1;
  }
  return {
    count: a.count.add(b.count),
    network_bytes: addMaybeUnsetNumericStat(
      a.network_bytes,
      b.network_bytes,
      countA,
      countB,
    ),
    max_mem_usage: addMaybeUnsetNumericStat(
      a.max_mem_usage,
      b.max_mem_usage,
      countA,
      countB,
    ),
    contention_time: addMaybeUnsetNumericStat(
      a.contention_time,
      b.contention_time,
      countA,
      countB,
    ),
    network_messages: addMaybeUnsetNumericStat(
      a.network_messages,
      b.network_messages,
      countA,
      countB,
    ),
    max_disk_usage: addMaybeUnsetNumericStat(
      a.max_disk_usage,
      b.max_disk_usage,
      countA,
      countB,
    ),
  };
}

function addMaybeUnsetNumericStat(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
): NumericStat {
  return a && b ? addNumericStats(a, b, countA, countB) : null;
}

export function coalesceSensitiveInfo(a: ISensitiveInfo, b: ISensitiveInfo) {
  return {
    last_err: a.last_err || b.last_err,
    most_recent_plan_description:
      a.most_recent_plan_description || b.most_recent_plan_description,
  };
}

export function aggregateStatementStats(
  statementStats: CollectedStatementStatistics[],
) {
  const statementsMap: {
    [statement: string]: CollectedStatementStatistics[];
  } = {};
  statementStats.forEach((statement: CollectedStatementStatistics) => {
    const matches =
      statementsMap[statement.key.key_data.query] ||
      (statementsMap[statement.key.key_data.query] = []);
    matches.push(statement);
  });

  return _.values(statementsMap).map((statements) =>
    _.reduce(
      statements,
      (a: CollectedStatementStatistics, b: CollectedStatementStatistics) => ({
        key: a.key,
        stats: addStatementStats(a.stats, b.stats),
      }),
    ),
  );
}

export interface ExecutionStatistics {
  statement: string;
  app: string;
  database: string;
  distSQL: boolean;
  vec: boolean;
  opt: boolean;
  implicit_txn: boolean;
  full_scan: boolean;
  failed: boolean;
  node_id: number;
  stats: StatementStatistics;
}

export function flattenStatementStats(
  statementStats: CollectedStatementStatistics[],
): ExecutionStatistics[] {
  return statementStats.map((stmt) => ({
    statement: stmt.key.key_data.query,
    app: stmt.key.key_data.app,
    database: stmt.key.key_data.database,
    distSQL: stmt.key.key_data.distSQL,
    vec: stmt.key.key_data.vec,
    opt: stmt.key.key_data.opt,
    implicit_txn: stmt.key.key_data.implicit_txn,
    full_scan: stmt.key.key_data.full_scan,
    failed: stmt.key.key_data.failed,
    node_id: stmt.key.node_id,
    txnID: stmt.key.key_data.txnID,
    stats: stmt.stats,
  }));
}

export function combineStatementStats(
  statementStats: StatementStatistics[],
): StatementStatistics {
  return _.reduce(statementStats, addStatementStats);
}

// This function returns a key based on all parameters
// that should be used to group statements.
// Parameters being used: node_id, implicit_txn and database.
export function statementKey(stmt: ExecutionStatistics): string {
  return stmt.statement + stmt.implicit_txn + stmt.database;
}

export function collectedStatementKey(
  stmt: CollectedStatementStatistics,
): string {
  return (
    stmt.key.key_data.query +
    stmt.key.key_data.implicit_txn +
    stmt.key.key_data.database
  );
}

export interface StatementsCount {
  query: string;
  count: number;
}

export interface TransactionSummaryData {
  fingerprint: string;
  statements: CollectedStatementStatistics[];
  stats_data: ICollectedTransactionStatistics[];
}

// Returns a string with the fingerprint of each statement executed
// in order and with the repetitions count (when is more than one)
// joined by a new line `\n`.
// E.g. `SELECT * FROM test\nINSERT INTO test VALUES (_) (x3)\nSELECT * FROM test`
export const collectStatementsTextWithReps = (
  statements: CollectedStatementStatistics[],
): string => {
  const statementsInfo: StatementsCount[] = [];
  let index = 0;

  statements.forEach((s) => {
    if (index > 0 && statementsInfo[index - 1].query === s.key.key_data.query) {
      statementsInfo[index - 1].count++;
    } else {
      statementsInfo.push({ query: s.key.key_data.query, count: 1 });
      index++;
    }
  });

  return statementsInfo
    .map((s) => {
      if (s.count > 1) return s.query + " (x" + s.count + ")";
      return s.query;
    })
    .join("\n");
};

// Returns a list of all executed statements in order
// with the repetitions.
export const getStatementsByIdInOrder = (
  statementsIds: Long[],
  statements: CollectedStatementStatistics[],
): CollectedStatementStatistics[] => {
  const allStatements: CollectedStatementStatistics[] = [];
  statementsIds.forEach((id) => {
    allStatements.push(statements.filter((s) => id.eq(s.id))[0]);
  });
  return allStatements;
};

export interface SummaryData {
  key: IExtendedStatementStatisticsKey;
  stats: StatementStatistics[];
}

export function combineStatements(
  statements: CollectedStatementStatistics[],
): CollectedStatementStatistics[] {
  const statsByStatementKey: {
    [statement: string]: SummaryData;
  } = {};
  statements.forEach((stmt: CollectedStatementStatistics) => {
    const key = collectedStatementKey(stmt);
    if (!(key in statsByStatementKey)) {
      statsByStatementKey[key] = {
        key: stmt.key,
        stats: [],
      };
    }
    statsByStatementKey[key].stats.push(stmt.stats);
  });

  return Object.keys(statsByStatementKey).map((key) => {
    const stmt = statsByStatementKey[key];
    return {
      key: stmt.key,
      stats: combineStatementStats(stmt.stats),
    };
  });

  return statements;
}

export function addTransactionStats(
  a: ICollectedTransactionStatistics,
  b: ICollectedTransactionStatistics,
): Required<ICollectedTransactionStatistics> {
  const countA = FixLong(a.stats.count).toInt();
  const countB = FixLong(b.stats.count).toInt();
  return {
    app: a.app,
    statement_ids: a.statement_ids.concat(b.statement_ids),
    stats: {
      bytes_read: aggregateNumericStats(
        a.stats.bytes_read,
        b.stats.bytes_read,
        countA,
        countB,
      ),
      commit_lat: aggregateNumericStats(
        a.stats.commit_lat,
        b.stats.commit_lat,
        countA,
        countB,
      ),
      count: a.stats.count.add(b.stats.count),
      max_retries: a.stats.max_retries.greaterThan(b.stats.max_retries)
        ? a.stats.max_retries
        : b.stats.max_retries,
      num_rows: aggregateNumericStats(
        a.stats.num_rows,
        b.stats.num_rows,
        countA,
        countB,
      ),
      retry_lat: aggregateNumericStats(
        a.stats.retry_lat,
        b.stats.retry_lat,
        countA,
        countB,
      ),
      rows_read: aggregateNumericStats(
        a.stats.rows_read,
        b.stats.rows_read,
        countA,
        countB,
      ),
      service_lat: aggregateNumericStats(
        a.stats.service_lat,
        b.stats.service_lat,
        countA,
        countB,
      ),
      exec_stats: addExecStats(a.stats.exec_stats, b.stats.exec_stats),
    },
  };
}

export function combineTransactionStatsData(
  statsData: ICollectedTransactionStatistics[],
): ICollectedTransactionStatistics {
  return _.reduce(statsData, addTransactionStats);
}

// This function returns a key based on all parameters
// that should be used to group transactions.
// Parameters being used: fingerprint, app.
export function transactionKey(txn: Transaction): string {
  return txn.fingerprint + txn.stats_data.app;
}

export function aggregateTransactions(
  statements: CollectedStatementStatistics[],
  transactions: IExtendedCollectedTransactionStatistics[],
): Transaction[] {
  const fullTransactions: Transaction[] = [];
  transactions.forEach((t) => {
    const orderedStatements = getStatementsByIdInOrder(
      t.stats_data.statement_ids,
      statements,
    );

    const txn: Transaction = {
      ...t,
      fingerprint: collectStatementsTextWithReps(orderedStatements),
      statements: statements.filter((s) =>
        t.stats_data.statement_ids.some((id) => id.eq(s.id)),
      ),
    };
    fullTransactions.push(txn);
  });

  const statsByTransactionKey: {
    [transaction: string]: TransactionSummaryData;
  } = {};
  fullTransactions.forEach((txn) => {
    const key = transactionKey(txn);
    if (!(key in statsByTransactionKey)) {
      statsByTransactionKey[key] = {
        fingerprint: txn.fingerprint,
        statements: [],
        stats_data: [],
      };
    }
    statsByTransactionKey[key].statements = statsByTransactionKey[
      key
    ].statements.concat(txn.statements);
    statsByTransactionKey[key].stats_data.push(txn.stats_data);
  });
  const aggregatedTransactions: Transaction[] = [];

  Object.keys(statsByTransactionKey).map((key) => {
    const txn = statsByTransactionKey[key];
    aggregatedTransactions.push({
      fingerprint: txn.fingerprint,
      statements: combineStatements(txn.statements),
      stats_data: combineTransactionStatsData(txn.stats_data),
    });
  });
  return aggregatedTransactions;
}
