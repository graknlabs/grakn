/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.test.integration;

import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.rocks.RocksGrakn;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.test.integration.Util.assertNotNulls;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

public class QueryTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static String database = "query-test";

    @Test
    public void test_query_define() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parse(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    AttributeType.String name = tx.concepts().getAttributeType("name").asString();
                    AttributeType.String symbol = tx.concepts().getAttributeType("symbol").asString();
                    AttributeType.Boolean active = tx.concepts().getAttributeType("active").asBoolean();
                    AttributeType.Long priority = tx.concepts().getAttributeType("priority").asLong();
                    assertNotNulls(name, symbol, active, priority);

                    EntityType organisation = tx.concepts().getEntityType("organisation");
                    EntityType team = tx.concepts().getEntityType("team");
                    EntityType user = tx.concepts().getEntityType("user");
                    EntityType repository = tx.concepts().getEntityType("repository");
                    EntityType branchRule = tx.concepts().getEntityType("branch-rule");
                    EntityType commit = tx.concepts().getEntityType("commit");
                    assertNotNulls(organisation, team, user, repository, branchRule, commit);

                    assertTrue(organisation.getOwns().anyMatch(a -> a.equals(name)));
                    assertTrue(team.getOwns().anyMatch(a -> a.equals(symbol)));
                    assertTrue(user.getOwns().anyMatch(a -> a.equals(name)));
                    assertTrue(repository.getOwns().anyMatch(a -> a.equals(active)));
                    assertTrue(branchRule.getOwns().anyMatch(a -> a.equals(priority)));
                    assertTrue(commit.getOwns().anyMatch(a -> a.equals(symbol)));

                    RelationType orgTeam = tx.concepts().getRelationType("org-team");
                    RelationType teamMember = tx.concepts().getRelationType("team-member");
                    RelationType repoDependency = tx.concepts().getRelationType("repo-dependency");
                    assertNotNulls(orgTeam, teamMember, repoDependency);

                    RoleType orgTeam_org = orgTeam.getRelates("org");
                    RoleType orgTeam_team = orgTeam.getRelates("team");
                    RoleType teamMember_team = teamMember.getRelates("team");
                    RoleType teamMember_member = teamMember.getRelates("member");
                    assertNotNulls(orgTeam_org, orgTeam_team, teamMember_team, teamMember_member);

                    assertTrue(organisation.getPlays().anyMatch(r -> r.equals(orgTeam_org)));
                    assertTrue(team.getPlays().anyMatch(r -> r.equals(orgTeam_team)));
                    assertTrue(team.getPlays().anyMatch(r -> r.equals(teamMember_team)));
                    assertTrue(user.getPlays().anyMatch(r -> r.equals(teamMember_member)));
                }
            }
        }
    }

    @Test
    public void test_query_undefine() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parse(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {

                }
            }
        }
    }
}
