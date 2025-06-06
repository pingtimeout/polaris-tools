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

plugins {
  `java-library`
  `maven-publish`
  signing
  `build-conventions`
}

dependencies {
  implementation(libs.guava)
  implementation(libs.hadoop.common) {
    exclude("org.apache.avro", "avro")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("javax.servlet", "servlet-api")
    exclude("com.google.code.gson", "gson")
    exclude("commons-beanutils")
  }
  implementation(libs.iceberg.spark.runtime)
  implementation(platform(libs.junit.bom))
  implementation("org.junit.jupiter:junit-jupiter-api")
  implementation("org.apache.iceberg:iceberg-hive-metastore:${libs.versions.iceberg.get()}:tests")
}
