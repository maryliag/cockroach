// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import styles from "../statementsPage/statementsPage.module.scss";
const cx = classNames.bind(styles);

interface TState {
  tabSelected: string;
}

export type SQLActivityPageProps = RouteComponentProps;

export class SQLActivityPage extends React.Component<
  SQLActivityPageProps,
  TState
> {
  state: TState = {
    tabSelected: "statements",
  };

  renderSQLActivityPage() {
    return (
      <div className={cx("table-area")}>
        <section className={baseHeadingClasses.wrapper}>
          <h1 className={baseHeadingClasses.tableName}>SQL Activity</h1>
        </section>
        AAHHHHH
      </div>
    );
  }

  render() {
    return this.renderSQLActivityPage();
  }
}
