/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.integration.tpc.command;

import picocli.CommandLine;

public class QueriesMixin {
  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
  private String[] queries = new String[0];

  @CommandLine.Option(names = {"--excluded-queries"}, description = "Set a comma-separated list of query IDs to exclude. Example: --exclude-queries=q1,q6", split = ",")
  private String[] excludedQueries = new String[0];

  @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
  private boolean explain;

  @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
  private int iterations;

  public String[] queries() {
    return queries;
  }

  public String[] excludedQueries() {
    return excludedQueries;
  }

  public boolean explain() {
    return explain;
  }

  public int iterations() {
    return iterations;
  }
}
