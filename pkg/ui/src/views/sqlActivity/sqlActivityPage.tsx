// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { withRouter } from "react-router-dom";

import { SQLActivityPage } from "@cockroachlabs/cluster-ui";

const SQLActivityPageConnected = withRouter(
  connect(() => ({}))(SQLActivityPage),
);

export default SQLActivityPageConnected;
