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

package org.apache.polaris.benchmarks.simulations

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import org.apache.polaris.benchmarks.actions._
import org.apache.polaris.benchmarks.parameters.BenchmarkConfig.config
import org.apache.polaris.benchmarks.parameters.{
  ConnectionParameters,
  DatasetParameters,
  WorkloadParameters
}
import org.apache.polaris.benchmarks.util.CircularIterator
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration._

class Issue1388 extends Simulation {
  private val logger = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------------
  // Load parameters
  // --------------------------------------------------------------------------------
  val cp: ConnectionParameters = config.connectionParameters
  val dp: DatasetParameters = config.datasetParameters

  // --------------------------------------------------------------------------------
  // Helper values
  // --------------------------------------------------------------------------------
  private val accessToken: AtomicReference[String] = new AtomicReference()
  private val reproducedIssue: AtomicBoolean = new AtomicBoolean(false)

  private val authActions = AuthenticationActions(cp, accessToken)
  private val catalogActions = CatalogActions(dp, accessToken)
  private val catalogFeeder: Feeder[String] = Iterator
    .from(0)
    .map { i =>
      val catalogName = s"C_$i"
      Map(
        "catalogName" -> catalogName,
        "defaultBaseLocation" -> s"${dp.defaultBaseLocation}/$catalogName"
      )
    }

  // --------------------------------------------------------------------------------
  // Authentication related workloads:
  // * Authenticate and store the access token for later use
  // * Wait for an OAuth token to be available
  // --------------------------------------------------------------------------------
  val authenticate: ScenarioBuilder =
    scenario("Authenticate using the Iceberg REST API")
      .feed(authActions.feeder())
      .exec(authActions.authenticateAndSaveAccessToken)

  val waitForAuthentication: ScenarioBuilder =
    scenario("Wait for the authentication token to be available")
      .asLongAs(_ => accessToken.get() == null)(
        pause(1.second)
      )

  // --------------------------------------------------------------------------------
  // Catalog related workloads:
  // * Create and delete catalogs
  // * List catalogs
  // --------------------------------------------------------------------------------
  val createAndDeleteCatalogs: ScenarioBuilder =
    scenario("Create and delete catalogs using the Iceberg REST API")
      .asLongAs(_ => !reproducedIssue.get())(
        exec(authActions.restoreAccessTokenInSession)
          .feed(catalogFeeder)
          .exec(catalogActions.createCatalog)
          .exec(catalogActions.deleteCatalog)
          .exitHereIfFailed
      )

  val listCatalogs: ScenarioBuilder =
    scenario("List catalogs using the Iceberg REST API")
      .asLongAs(_ => !reproducedIssue.get())(
        exec(authActions.restoreAccessTokenInSession)
          .exec(catalogActions.fetchAllCatalogs)
          .exitHereIfFailed
      )

  val stopExecution: ScenarioBuilder =
    scenario("Stop the scenarios after the issue is reproduced")
      .exec { session =>
        // Either this scenario or the other scenario reproduced the issue
        reproducedIssue.set(true)
        session
      }

  // --------------------------------------------------------------------------------
  // Build up the HTTP protocol configuration and set up the simulation
  // --------------------------------------------------------------------------------
  private val httpProtocol = http
    .baseUrl(cp.baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  // Get the configured throughput and duration

  setUp(
    authenticate.inject(atOnceUsers(1)).protocols(httpProtocol),
    waitForAuthentication
      .inject(atOnceUsers(1))
      .andThen(
        createAndDeleteCatalogs.inject(atOnceUsers(1)).protocols(httpProtocol),
        listCatalogs
          .inject(atOnceUsers(1))
          .protocols(httpProtocol)
          .andThen(stopExecution.inject(atOnceUsers(1)).protocols(httpProtocol))
      )
  )
}
