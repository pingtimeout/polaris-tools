/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.benchmarks.parameters

/**
 * Case class to hold the parameters for the TableCommitsCreator simulation.
 *
 * @param commitsThroughput The number of commits to create per second.
 * @param readThroughput The number of reads to perform per second.
 * @param durationInMinutes The duration of the simulation in minutes.
 */
case class TableCommitsCreatorParameters(
    commitsThroughput: Int,
    readThroughput: Int,
    durationInMinutes: Int
) {
  require(commitsThroughput >= 0, "Commits throughput cannot be negative")
  require(readThroughput >= 0, "Read throughput cannot be negative")
  require(durationInMinutes > 0, "Duration in minutes must be positive")
}
