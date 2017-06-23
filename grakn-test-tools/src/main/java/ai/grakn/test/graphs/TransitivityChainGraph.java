/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.test.GraphContext;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class TransitivityChainGraph extends TestGraph {

    private final static Label key = Label.of("index");
    private final static String gqlFile = "simple-transitivity.gql";

    private final int n;

    public TransitivityChainGraph(int n){
        this.n = n;
    }

    public static Consumer<GraknGraph> get(int n) {
        return new TransitivityChainGraph(n).build();
    }

    @Override
    public Consumer<GraknGraph> build(){
        return (GraknGraph graph) -> {
            GraphContext.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n);
        };
    }

    private void buildExtensionalDB(GraknGraph graph, int n) {
        RoleType qfrom = graph.getRoleType("Q-from");
        RoleType qto = graph.getRoleType("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        RelationType q = graph.getRelationType("Q");
        Thing aInst = putEntity(graph, "a", graph.getEntityType("entity2"), key);
        ConceptId[] aInstanceIds = new ConceptId[n];
        for(int i = 0 ; i < n ;i++) {
            aInstanceIds[i] = putEntity(graph, "a" + i, aEntity, key).getId();
        }

        q.addRelation()
                .addRolePlayer(qfrom, aInst)
                .addRolePlayer(qto, graph.getConcept(aInstanceIds[0]));

        for(int i = 0 ; i < n - 1 ; i++) {
                    q.addRelation()
                            .addRolePlayer(qfrom, graph.getConcept(aInstanceIds[i]))
                            .addRolePlayer(qto, graph.getConcept(aInstanceIds[i+1]));
        }
    }
}
