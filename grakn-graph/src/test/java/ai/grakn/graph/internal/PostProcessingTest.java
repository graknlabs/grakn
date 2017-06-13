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

package ai.grakn.graph.internal;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PostProcessingTest extends GraphTestBase{
    private RoleType roleType1;
    private RoleType roleType2;
    private RelationType relationType;
    private InstanceImpl instance1;
    private InstanceImpl instance2;

    @Before
    public void buildSampleGraph(){
        roleType1 = graknGraph.putRoleType("role 1");
        roleType2 = graknGraph.putRoleType("role 2");
        relationType = graknGraph.putRelationType("rel type").relates(roleType1).relates(roleType2);
        EntityType thing = graknGraph.putEntityType("thing").plays(roleType1).plays(roleType2);
        instance1 = (InstanceImpl) thing.addEntity();
        instance2 = (InstanceImpl) thing.addEntity();
        thing.addEntity();
        thing.addEntity();

        relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);
    }

    @Test
    public void whenMergingDuplicateResources_EnsureSingleResourceRemains(){
        ResourceTypeImpl<String> resourceType = (ResourceTypeImpl<String>) graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        //Create fake resources
        Set<ConceptId> resourceIds = new HashSet<>();
        ResourceImpl<String> mainResource = createFakeResource(resourceType, "1");
        resourceIds.add(mainResource.getId());
        resourceIds.add(createFakeResource(resourceType, "1").getId());
        resourceIds.add(createFakeResource(resourceType, "1").getId());

        //Check we have duplicate resources
        assertEquals(3, resourceType.instances().size());

        //Fix duplicates
        graknGraph.fixDuplicateResources(mainResource.getIndex(), resourceIds);

        //Check we no longer have duplicates
        assertEquals(1, resourceType.instances().size());
    }

    @Test
    public void whenMergingDuplicateResourcesWithRelations_EnsureSingleResourceRemainsAndNoDuplicateRelationsAreCreated(){
        RoleType roleEntity = graknGraph.putRoleType("Entity Role");
        RoleType roleResource = graknGraph.putRoleType("Resource Role");
        RelationType relationType = graknGraph.putRelationType("Relation Type").relates(roleEntity).relates(roleResource);
        ResourceTypeImpl<String> resourceType = (ResourceTypeImpl<String>) graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING).plays(roleResource);
        EntityType entityType = graknGraph.putEntityType("Entity Type").plays(roleEntity);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Entity e3 = entityType.addEntity();

        //Create fake resources
        Set<ConceptId> resourceIds = new HashSet<>();
        ResourceImpl<?> r1 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r11 = createFakeResource(resourceType, "1");
        ResourceImpl<?> r111 = createFakeResource(resourceType, "1");

        resourceIds.add(r1.getId());
        resourceIds.add(r11.getId());
        resourceIds.add(r111.getId());

        //Give resources some relationships
        RelationImpl rel1 = ((RelationImpl) relationType.addRelation().
                addRolePlayer(roleResource, r1).addRolePlayer(roleEntity, e1));
        RelationImpl rel2 = ((RelationImpl) relationType.addRelation().
                addRolePlayer(roleResource, r11).addRolePlayer(roleEntity, e1)); //When merging this relation should not be absorbed
        RelationImpl rel3 = ((RelationImpl) relationType.addRelation().
                addRolePlayer(roleResource, r11).addRolePlayer(roleEntity, e2)); //Absorb
        RelationImpl rel4 = ((RelationImpl) relationType.addRelation().
                addRolePlayer(roleResource, r111).addRolePlayer(roleEntity, e2)); //Don't Absorb
        RelationImpl rel5 = ((RelationImpl) relationType.addRelation().
                addRolePlayer(roleResource, r111).addRolePlayer(roleEntity, e3)); //Absorb

        rel1.setHash();
        rel2.setHash();
        rel3.setHash();
        rel4.setHash();
        rel5.setHash();

        //Check everything is broken
        assertEquals(3, resourceType.instances().size());
        assertEquals(1, r1.relations().size());
        assertEquals(2, r11.relations().size());
        assertEquals(1, r1.relations().size());
        assertEquals(6, graknGraph.getTinkerTraversal().hasLabel(Schema.BaseType.RELATION.name()).toList().size());

        r1.relations().forEach(rel -> assertTrue(rel.rolePlayers().contains(e1)));

        //Now fix everything
        graknGraph.fixDuplicateResources(r1.getIndex(), resourceIds);

        //Check everything is in order
        assertEquals(1, resourceType.instances().size());

        //Get back the surviving resource
        Resource<String> foundR1 = null;
        for (Resource<String> resource : resourceType.instances()) {
            if(resource.getValue().equals("1")){
                foundR1 = resource;
                break;
            }
        }

        assertNotNull(foundR1);
        assertThat(foundR1.ownerInstances(), containsInAnyOrder(e1, e2, e3));
        assertEquals(4, graknGraph.admin().getMetaRelationType().instances().size());
    }


    private ResourceImpl<String> createFakeResource(ResourceTypeImpl<String> type, String value){
        String index = Schema.generateResourceIndex(type.getLabel(), value);
        Vertex resourceVertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), type.currentShard().vertex().element());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(), index);
        resourceVertex.property(Schema.VertexProperty.VALUE_STRING.name(), value);
        resourceVertex.property(Schema.VertexProperty.ID.name(), resourceVertex.id().toString());

        return new ResourceImpl<>(new VertexElement(graknGraph, resourceVertex));
    }

    @Test
    public void whenUpdatingTheCountsOfTypes_TheTypesHaveNewCounts() {
        Map<ConceptId, Long> types = new HashMap<>();
        //Create Some Types;
        EntityTypeImpl t1 = (EntityTypeImpl) graknGraph.putEntityType("t1");
        ResourceTypeImpl t2 = (ResourceTypeImpl)  graknGraph.putResourceType("t2", ResourceType.DataType.STRING);
        RelationTypeImpl t3 = (RelationTypeImpl) graknGraph.putRelationType("t3");

        //Lets Set Some Counts
        types.put(t1.getId(), 5L);
        types.put(t2.getId(), 6L);
        types.put(t3.getId(), 2L);

        graknGraph.admin().updateConceptCounts(types);
        types.entrySet().forEach(entry ->
                assertEquals((long) entry.getValue(), ((ConceptImpl) graknGraph.getConcept(entry.getKey())).getShardCount()));

        //Lets Set Some Counts
        types.put(t1.getId(), -5L);
        types.put(t2.getId(), -2L);
        types.put(t3.getId(), 3L);
        graknGraph.admin().updateConceptCounts(types);

        assertEquals(0L, t1.getShardCount());
        assertEquals(4L, t2.getShardCount());
        assertEquals(5L, t3.getShardCount());
    }
}
