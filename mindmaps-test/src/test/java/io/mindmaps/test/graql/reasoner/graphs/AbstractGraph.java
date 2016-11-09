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
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.graql.Pattern;

public class AbstractGraph extends TestGraph{

    private static EntityType P, Q, p, q, r, s, t, u;
    private static RelationType rel, REL;
    private static RoleType relRoleA, relRoleB, RELRoleA, RELRoleB;

    private static Instance instanceU, instanceT, instanceP;

    public static MindmapsGraph getGraph() {
        return new AbstractGraph().graph();
    }

    @Override
    protected void buildOntology() {
        relRoleA = mindmaps.putRoleType("rel-roleA");
        relRoleB = mindmaps.putRoleType("rel-roleB");
        rel = mindmaps.putRelationType("rel").hasRole(relRoleA).hasRole(relRoleB);

        RELRoleA = mindmaps.putRoleType("REL-roleA");
        RELRoleB = mindmaps.putRoleType("REL-roleB");
        REL = mindmaps.putRelationType("REL").hasRole(RELRoleA).hasRole(RELRoleB);

        P = mindmaps.putEntityType("P").playsRole(RELRoleA).playsRole(RELRoleB);
        Q = mindmaps.putEntityType("Q").playsRole(RELRoleB).playsRole(RELRoleA);
        p = mindmaps.putEntityType("p").playsRole(relRoleA).playsRole(RELRoleA);
        q = mindmaps.putEntityType("q").playsRole(relRoleB).playsRole(RELRoleB);
        r = mindmaps.putEntityType("r").playsRole(relRoleA).playsRole(RELRoleA);
        s = mindmaps.putEntityType("s").playsRole(relRoleB).playsRole(RELRoleB);
        u = mindmaps.putEntityType("u").playsRole(relRoleA).playsRole(RELRoleA);
        t = mindmaps.putEntityType("t").playsRole(relRoleB).playsRole(RELRoleB);
    }

    @Override
    protected void buildInstances() {
        instanceU = putEntity("instanceU", u);
        instanceT = putEntity("instanceT", t);
        instanceP = putEntity("instanceP", P);
    }

    @Override
    protected void  buildRelations() {
        mindmaps.addRelation(rel)
                .putRolePlayer(relRoleA, instanceU)
                .putRolePlayer(relRoleB, instanceT);
        mindmaps.addRelation(REL)
                .putRolePlayer(RELRoleA, instanceU)
                .putRolePlayer(RELRoleB, instanceP);

    }
    @Override
    protected void buildRules() {
        RuleType inferenceRule = mindmaps.getMetaRuleInference();

        Pattern R1_LHS = mindmaps.graql().parsePattern("$x isa p;$y isa q;($x, $y) isa rel;");
        Pattern R1_RHS = mindmaps.graql().parsePattern("$x isa Q;");
        mindmaps.addRule(R1_LHS, R1_RHS, inferenceRule);

        Pattern R2_LHS = mindmaps.graql().parsePattern("$x isa r;");
        Pattern R2_RHS = mindmaps.graql().parsePattern("$x isa p;");
        mindmaps.addRule(R2_LHS, R2_RHS, inferenceRule);

        Pattern R3_LHS = mindmaps.graql().parsePattern("$x isa s;");
        Pattern R3_RHS = mindmaps.graql().parsePattern("$x isa p;");
        mindmaps.addRule(R3_LHS, R3_RHS, inferenceRule);

        Pattern R4_LHS = mindmaps.graql().parsePattern("$x isa t;");
        Pattern R4_RHS = mindmaps.graql().parsePattern("$x isa q;");
        mindmaps.addRule(R4_LHS, R4_RHS, inferenceRule);

        Pattern R5_LHS = mindmaps.graql().parsePattern("$x isa u;");
        Pattern R5_RHS = mindmaps.graql().parsePattern("$x isa r;");
        mindmaps.addRule(R5_LHS, R5_RHS, inferenceRule);
    }
}
