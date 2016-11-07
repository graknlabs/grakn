/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;

public class TailRecursionGraph extends TestGraph {

    final static String key = "index";
    final static String gqlFile = "tail-recursion-test.gql";

    public TailRecursionGraph(int n, int m) {
        super(key, gqlFile);
        buildExtensionalDB(n, m);
        commit();
    }

    public static MindmapsGraph getGraph(int n, int m) {
        return new TailRecursionGraph(n, m).graph();
    }

    private void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = mindmaps.getRoleType("Q-from");
        RoleType Qto = mindmaps.getRoleType("Q-to");

        EntityType aEntity = mindmaps.getEntityType("a-entity");
        EntityType bEntity = mindmaps.getEntityType("b-entity");
        RelationType Q = mindmaps.getRelationType("Q");

        String a0Id = putEntity("a0", aEntity).getId();
        String[][] bInstancesIds = new String[m + 2][n + 2];
        for (int i = 1; i <= m; i++)
            for (int j = 1; j <= n; j++)
                bInstancesIds[i][j] = putEntity("b" + i + j, bEntity).getId();

        for (int j = 1; j <= n; j++) {
            mindmaps.addRelation(Q)
                    .putRolePlayer(Qfrom, mindmaps.getInstance(a0Id))
                    .putRolePlayer(Qto, mindmaps.getInstance(bInstancesIds[1][j]));
            for (int i = 1; i <= m; i++) {
                mindmaps.addRelation(Q)
                        .putRolePlayer(Qfrom, mindmaps.getInstance(bInstancesIds[i][j]))
                        .putRolePlayer(Qto, mindmaps.getInstance(bInstancesIds[i + 1][j]));
            }
        }
    }
}
