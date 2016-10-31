/*
 * GraknDB - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Research Ltd
 *
 * GraknDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GraknDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GraknDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package test.io.grakn.migration.owl;

import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.concept.Concept;
import io.grakn.concept.Entity;
import io.grakn.concept.Instance;
import io.grakn.concept.Relation;
import io.grakn.concept.RelationType;
import io.grakn.concept.Resource;
import io.grakn.concept.RoleType;
import io.grakn.migration.owl.OWLMigrator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for OWL migrator unit tests: create and holds OWL manager and
 * MM graph, statically because they don't need to be re-initialized on a per
 * test basis.
 * 
 * @author borislav
 *
 */
public class TestOwlGraknBase {
    public static final String OWL_TEST_GRAPH = "owltestgraph";
 
    GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, "graph-" + this.getClass().getName()).getGraph();
    OWLOntologyManager manager;
    
    @Before
    public void init() {
        manager = OWLManager.createOWLOntologyManager();
    }
    
    @After
    public void closeGraph() {   
        graph.close();
    }
    
    OWLMigrator migrator;

    @Before
    public void initMigrator() {
         migrator = new OWLMigrator();
    }

    OWLOntologyManager owlManager() {
        return manager;
    }
    
    OWLOntology loadOntologyFromResource(String resource) {
        try (InputStream in = this.getClass().getResourceAsStream(resource)) {
            if (in == null)
                throw new NullPointerException("Resource : " + resource + " not found.");
            return owlManager().loadOntologyFromOntologyDocument(in);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }   

    <T extends Concept> Optional<T> findById(Collection<T> C, String id) {
        return C.stream().filter(x -> x.getId().equals(id)).findFirst();
    }
    
    void checkResource(final Entity e, final String resourceTypeId, final Object value) {
        Optional<Resource<?>> r = e.resources().stream().filter(x -> x.type().getId().equals(resourceTypeId)).findFirst();
        Assert.assertTrue(r.isPresent());
        Assert.assertEquals(value, r.get().getValue());
    }
    
    void checkRelation(Entity subject, String relationTypeId, Entity object) {
        RelationType relationType = graph.getRelationType(relationTypeId);
        final RoleType subjectRole = graph.getRoleType(migrator.namer().subjectRole(relationType.getId()));
        final RoleType objectRole = graph.getRoleType(migrator.namer().objectRole(relationType.getId()));
        Assert.assertNotNull(subjectRole);
        Assert.assertNotNull(objectRole);
        Optional<Relation> relation = relationType.instances().stream().filter(rel -> {
            Map<RoleType, Instance> players = rel.rolePlayers();
            return subject.equals(players.get(subjectRole)) && object.equals(players.get(objectRole)); 
        }).findFirst();
        Assert.assertTrue(relation.isPresent());
    }
}