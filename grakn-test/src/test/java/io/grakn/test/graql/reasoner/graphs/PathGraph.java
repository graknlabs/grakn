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

package io.grakn.test.graql.reasoner.graphs;

import io.grakn.GraknGraph;
import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;

import static com.google.common.math.IntMath.pow;

public class PathGraph extends GenericGraph {

    public static GraknGraph getGraph(int n, int children) {
        final String gqlFile = "path-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, children);
        commit();
        return grakn;
    }

    protected static void buildExtensionalDB(int n, int children) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = grakn.getEntityType("vertex");
        EntityType startVertex = grakn.getEntityType("start-vertex");
        RoleType arcFrom = grakn.getRoleType("arc-from");
        RoleType arcTo = grakn.getRoleType("arc-to");

        RelationType arc = grakn.getRelationType("arc");
        grakn.putEntity("a0", startVertex);

        for(int i = 1 ; i <= n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                grakn.putEntity("a" + i + "," + j, vertex);
                if (j != 0 && j % 100 ==0)
                    System.out.println(j + " entities out of " + m + " inserted");
            }
        }

        for (int j = 0; j < children; j++) {
            grakn.addRelation(arc)
                    .putRolePlayer(arcFrom, grakn.getInstance("a0"))
                    .putRolePlayer(arcTo, grakn.getInstance("a1," + j));
        }

        for(int i = 1 ; i < n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    grakn.addRelation(arc)
                            .putRolePlayer(arcFrom, grakn.getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, grakn.getInstance("a" + (i + 1) + "," + (j * children + c)));

                }
                if (j!= 0 && j % 100 == 0)
                    System.out.println("level " + i + "/" + (n-1) + ": " + j + " entities out of " + m + " connected");
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraph loading time: " + loadTime + " ms");
    }
}
