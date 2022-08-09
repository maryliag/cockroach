// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import { Helmet } from "react-helmet";
import { ArrowLeft } from "@cockroachlabs/icons";
import {
  PlansSortedTable,
  makeExplainPlanColumns,
  PlanHashStats,
} from "./plansTable";
import { Button } from "../../button";
import { SqlBox, SqlBoxSize } from "../../sql";
import { SortSetting } from "../../sortedtable";
import { Heading } from "@cockroachlabs/ui-components";
import { SummaryCard } from "src/summaryCard";
import { Row } from "antd";
import "antd/lib/row/style";
import classNames from "classnames/bind";
import styles from "../statementDetails.module.scss";
import {
  makeIdxRecColumns,
  IdxInsightsSortedTable,
  IdxRecommendation,
  IdxRecommendationType,
} from "../../indexRecommendationsTable/indexRecommendationsTable";

const cx = classNames.bind(styles);

interface PlanDetailsProps {
  plans: PlanHashStats[];
  plansSortSetting: SortSetting;
  onChangePlansSortSetting: (ss: SortSetting) => void;
  insightsSortSetting: SortSetting;
  onChangeInsightsSortSetting: (ss: SortSetting) => void;
}

export function PlanDetails({
  plans,
  plansSortSetting,
  onChangePlansSortSetting,
  insightsSortSetting,
  onChangeInsightsSortSetting,
}: PlanDetailsProps): React.ReactElement {
  const [plan, setPlan] = useState<PlanHashStats | null>(null);
  const handleDetails = (plan: PlanHashStats): void => {
    setPlan(plan);
  };
  const backToPlanTable = (): void => {
    setPlan(null);
  };

  if (plan) {
    return renderExplainPlan(
      plan,
      backToPlanTable,
      insightsSortSetting,
      onChangeInsightsSortSetting,
    );
  } else {
    return renderPlanTable(
      plans,
      handleDetails,
      plansSortSetting,
      onChangePlansSortSetting,
    );
  }
}

function renderPlanTable(
  plans: PlanHashStats[],
  handleDetails: (plan: PlanHashStats) => void,
  sortSetting: SortSetting,
  onChangeSortSetting: (ss: SortSetting) => void,
): React.ReactElement {
  const columns = makeExplainPlanColumns(handleDetails);
  return (
    <PlansSortedTable
      columns={columns}
      data={plans}
      className="statements-table"
      sortSetting={sortSetting}
      onChangeSortSetting={onChangeSortSetting}
    />
  );
}

function renderExplainPlan(
  plan: PlanHashStats,
  backToPlanTable: () => void,
  sortSetting: SortSetting,
  onChangeSortSetting: (ss: SortSetting) => void,
): React.ReactElement {
  const explainPlan =
    plan.explain_plan === "" ? "unavailable" : plan.explain_plan;
  const ins = [
    "creation : creable blabla",
    "replacement : create blabla; drop bleble;",
  ];
  // const hasInsights = plan.index_recommendations?.length > 0;
  const hasInsights = true;
  return (
    <div>
      <Helmet title="Plan Details" />
      <Button
        onClick={backToPlanTable}
        type="unstyled-link"
        size="small"
        icon={<ArrowLeft fontSize={"10px"} />}
        iconPosition="left"
        className="small-margin"
      >
        All Plans
      </Button>
      <SqlBox value={explainPlan} size={SqlBoxSize.large} />
      {hasInsights &&
        renderInsights(ins, plan, sortSetting, onChangeSortSetting)}
    </div>
  );
}

function formatIdxRecommendations(
  idxRecs: string[],
  plan: PlanHashStats,
): IdxRecommendation[] {
  const recs = [];
  for (let i = 0; i < idxRecs.length; i++) {
    const rec = idxRecs[i];
    let idxType: IdxRecommendationType;
    const t = rec.split(" : ")[0];
    switch (t) {
      case "creation":
        idxType = "CREATE";
        break;
      case "replacement":
        idxType = "REPLACE";
        break;
      case "drop":
        idxType = "DROP";
        break;
    }
    const idxRec: IdxRecommendation = {
      type: idxType,
      database: plan.metadata.databases[0],
      table: "",
      index_id: 0,
      query: rec.split(" : ")[1],
    };
    recs.push(idxRec);
  }

  return recs;
}

function renderInsights(
  idxRecommendations: string[],
  plan: PlanHashStats,
  sortSetting: SortSetting,
  onChangeSortSetting: (ss: SortSetting) => void,
): React.ReactElement {
  const columns = makeIdxRecColumns();
  const data = formatIdxRecommendations(idxRecommendations, plan);
  return (
    <Row gutter={24}>
      <SummaryCard className={cx("summary-card", "index-stats__summary-card")}>
        <div className={cx("index-stats__header")}>
          <Heading type="h5">Insights</Heading>
          <IdxInsightsSortedTable
            columns={columns}
            data={data}
            sortSetting={sortSetting}
            onChangeSortSetting={onChangeSortSetting}
          />
        </div>
      </SummaryCard>
    </Row>
  );
}
