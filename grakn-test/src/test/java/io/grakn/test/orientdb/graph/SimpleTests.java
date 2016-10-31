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

package io.grakn.test.orientdb.graph;

import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.concept.Entity;
import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;
import io.grakn.engine.postprocessing.Cache;
import io.grakn.exception.GraknValidationException;
import io.grakn.test.AbstractRollbackGraphTest;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class SimpleTests extends AbstractRollbackGraphTest {

    @Ignore //Failing due to inconsistent clears.
    @Test
    public void testOrientDBConstructionThroughEngine() throws GraknValidationException {
        GraknGraph graknGraph = Grakn.factory(Grakn.DEFAULT_URI, "memory").getGraph();

        //Create Ontology
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        graknGraph.putEntityType("et1").playsRole(role1);
        graknGraph.putEntityType("et2").playsRole(role2);
        graknGraph.putRelationType("rel").hasRole(role1).hasRole(role2);

        graknGraph.commit();

        //Check Ontology is there:
        role1 = graknGraph.getRoleType("role1");
        role2 = graknGraph.getRoleType("role2");
        EntityType et1 = graknGraph.getEntityType("et1").playsRole(role1);
        EntityType et2 = graknGraph.getEntityType("et2").playsRole(role2);
        RelationType rel = graknGraph.getRelationType("rel").hasRole(role1).hasRole(role2);

        assertNotNull(role1);
        assertNotNull(role2);
        assertNotNull(et1);
        assertNotNull(et2);
        assertNotNull(rel);

        //Create Some Data
        Entity e1 = graknGraph.addEntity(et1);
        Entity e2 = graknGraph.addEntity(et2);
        graknGraph.addRelation(rel).putRolePlayer(role1, e1).putRolePlayer(role2, e2);

        graknGraph.commit();

        //Check the Data is there
        graknGraph = Grakn.factory(Grakn.DEFAULT_URI, "memory").getGraph();
        assertEquals(1, graknGraph.getEntityType("et1").instances().size());
        assertEquals(1, graknGraph.getEntityType("et2").instances().size());

        //Check Engine has Castings
        Cache cache = Cache.getInstance();
        assertEquals(2, cache.getCastingJobs(graknGraph.getKeyspace()).size());
    }
}
