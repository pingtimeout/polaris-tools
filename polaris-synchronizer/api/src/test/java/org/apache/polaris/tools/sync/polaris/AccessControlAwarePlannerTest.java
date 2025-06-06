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
package org.apache.polaris.tools.sync.polaris;

import java.util.List;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.tools.sync.polaris.access.AccessControlConstants;
import org.apache.polaris.tools.sync.polaris.planning.AccessControlAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.NoOpSyncPlanner;
import org.apache.polaris.tools.sync.polaris.planning.SynchronizationPlanner;
import org.apache.polaris.tools.sync.polaris.planning.plan.SynchronizationPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AccessControlAwarePlannerTest {

  private static final Principal omnipotentPrincipalSource =
          new Principal()
                  .name("omnipotent-principal-XXXXX")
                  .putPropertiesItem(AccessControlConstants.OMNIPOTENCE_PROPERTY, "");

  private static final Principal omnipotentPrincipalTarget =
          new Principal()
                  .name("omnipotent-principal-YYYYY")
                  .putPropertiesItem(AccessControlConstants.OMNIPOTENCE_PROPERTY, "");

  @Test
  public void filtersOmnipotentPrincipal() {
    SynchronizationPlanner accessControlAwarePlanner
            = new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<Principal> plan = accessControlAwarePlanner
            .planPrincipalSync(List.of(omnipotentPrincipalSource), List.of(omnipotentPrincipalTarget));

    Assertions.assertTrue(plan.entitiesToSkipAndSkipChildren().contains(omnipotentPrincipalSource));
    Assertions.assertTrue(plan.entitiesToSkipAndSkipChildren().contains(omnipotentPrincipalTarget));
  }

  private static final Principal rootPrincipalSource = new Principal().name("root");

  private static final Principal rootPrincipalTarget = new Principal().name("root");

  @Test
  public void filtersRootPrincipal() {
    SynchronizationPlanner accessControlAwarePlanner
            = new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<Principal> plan = accessControlAwarePlanner
            .planPrincipalSync(List.of(rootPrincipalSource), List.of(rootPrincipalTarget));

    Assertions.assertTrue(plan.entitiesToSkipAndSkipChildren().contains(rootPrincipalSource));
    Assertions.assertTrue(plan.entitiesToSkipAndSkipChildren().contains(rootPrincipalTarget));
  }

  @Test
  public void filtersPrincipalAssignmentToOmnipotentPrincipalRole() {
    SynchronizationPlanner accessControlAwarePlanner
            = new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<PrincipalRole> plan = accessControlAwarePlanner.planAssignPrincipalsToPrincipalRolesSync(
            "principal", List.of(omnipotentPrincipalRoleSource), List.of(omnipotentPrincipalRoleTarget));

    Assertions.assertTrue(plan.entitiesToSkip().contains(omnipotentPrincipalRoleSource));
    Assertions.assertTrue(plan.entitiesToSkip().contains(omnipotentPrincipalRoleTarget));
  }

  @Test
  public void filtersAssignmentToServiceAdmin() {
    SynchronizationPlanner accessControlAwarePlanner
            = new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<PrincipalRole> plan = accessControlAwarePlanner.planAssignPrincipalsToPrincipalRolesSync(
            "principal", List.of(serviceAdminSource), List.of(serviceAdminTarget));

    Assertions.assertTrue(plan.entitiesToSkip().contains(serviceAdminSource));
    Assertions.assertTrue(plan.entitiesToSkip().contains(serviceAdminTarget));
  }

  private static final PrincipalRole omnipotentPrincipalRoleSource =
      new PrincipalRole()
          .name("omnipotent-principal-XXXXX")
          .putPropertiesItem(AccessControlConstants.OMNIPOTENCE_PROPERTY, "");

  private static final PrincipalRole omnipotentPrincipalRoleTarget =
      new PrincipalRole()
          .name("omnipotent-principal-YYYYY")
          .putPropertiesItem(AccessControlConstants.OMNIPOTENCE_PROPERTY, "");

  @Test
  public void filtersOmnipotentPrincipalRoles() {
    SynchronizationPlanner accessControlAwarePlanner =
        new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<PrincipalRole> plan =
        accessControlAwarePlanner.planPrincipalRoleSync(
            List.of(omnipotentPrincipalRoleSource), List.of(omnipotentPrincipalRoleTarget));

    Assertions.assertTrue(plan.entitiesToSkip().contains(omnipotentPrincipalRoleSource));
    Assertions.assertTrue(plan.entitiesToSkip().contains(omnipotentPrincipalRoleTarget));
  }

  private static final PrincipalRole serviceAdminSource = new PrincipalRole().name("service_admin");

  private static final PrincipalRole serviceAdminTarget = new PrincipalRole().name("service_admin");

  @Test
  public void filtersServiceAdmin() {
    SynchronizationPlanner accessControlAwarePlanner =
        new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<PrincipalRole> plan =
        accessControlAwarePlanner.planPrincipalRoleSync(
            List.of(serviceAdminSource), List.of(serviceAdminTarget));

    Assertions.assertTrue(plan.entitiesToSkip().contains(serviceAdminSource));
    Assertions.assertTrue(plan.entitiesToSkip().contains(serviceAdminTarget));
  }

  private static final CatalogRole omnipotentCatalogRoleSource =
      new CatalogRole()
          .name("omnipotent-principal-XXXXX")
          .putPropertiesItem(AccessControlConstants.OMNIPOTENCE_PROPERTY, "");

  private static final CatalogRole omnipotentCatalogRoleTarget =
      new CatalogRole()
          .name("omnipotent-principal-YYYYY")
          .putPropertiesItem(AccessControlConstants.OMNIPOTENCE_PROPERTY, "");

  @Test
  public void filtersOmnipotentCatalogRolesAndChildren() {
    SynchronizationPlanner accessControlAwarePlanner =
        new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<CatalogRole> plan =
        accessControlAwarePlanner.planCatalogRoleSync(
            "catalogName",
            List.of(omnipotentCatalogRoleSource),
            List.of(omnipotentCatalogRoleTarget));

    Assertions.assertTrue(
        plan.entitiesToSkipAndSkipChildren().contains(omnipotentCatalogRoleSource));
    Assertions.assertTrue(
        plan.entitiesToSkipAndSkipChildren().contains(omnipotentCatalogRoleTarget));
  }

  private static final CatalogRole catalogAdminSource = new CatalogRole().name("catalog_admin");

  private static final CatalogRole catalogAdminTarget = new CatalogRole().name("catalog_admin");

  @Test
  public void filtersCatalogAdminAndChildren() {
    SynchronizationPlanner accessControlAwarePlanner =
        new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<CatalogRole> plan =
        accessControlAwarePlanner.planCatalogRoleSync(
            "catalogName", List.of(catalogAdminSource), List.of(catalogAdminTarget));

    Assertions.assertTrue(plan.entitiesToSkipAndSkipChildren().contains(catalogAdminSource));
    Assertions.assertTrue(plan.entitiesToSkipAndSkipChildren().contains(catalogAdminTarget));
  }

  @Test
  public void filtersOutAssignmentOfOmnipotentPrincipalRoles() {
    SynchronizationPlanner accessControlAwarePlanner =
        new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<PrincipalRole> plan =
        accessControlAwarePlanner.planAssignPrincipalRolesToCatalogRolesSync(
            "catalogName",
            "catalogRoleName",
            List.of(omnipotentPrincipalRoleSource),
            List.of(omnipotentPrincipalRoleTarget));

    Assertions.assertTrue(plan.entitiesToSkip().contains(omnipotentPrincipalRoleSource));
    Assertions.assertTrue(plan.entitiesToSkip().contains(omnipotentPrincipalRoleTarget));
  }

  @Test
  public void filtersOutAssignmentOfServiceAdmin() {
    SynchronizationPlanner accessControlAwarePlanner =
        new AccessControlAwarePlanner(new NoOpSyncPlanner());

    SynchronizationPlan<PrincipalRole> plan =
        accessControlAwarePlanner.planAssignPrincipalRolesToCatalogRolesSync(
            "catalogName",
            "catalogRoleName",
            List.of(serviceAdminSource),
            List.of(serviceAdminTarget));

    Assertions.assertTrue(plan.entitiesToSkip().contains(serviceAdminSource));
    Assertions.assertTrue(plan.entitiesToSkip().contains(serviceAdminTarget));
  }
}
