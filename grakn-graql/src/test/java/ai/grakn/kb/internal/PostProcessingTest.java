/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.kb.internal;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.kb.internal.concept.AttributeImpl;
import ai.grakn.kb.internal.concept.AttributeTypeImpl;
import ai.grakn.kb.internal.concept.ThingImpl;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PostProcessingTest extends TxTestBase {
    private Role role1;
    private Role role2;
    private RelationshipType relationshipType;
    private ThingImpl instance1;
    private ThingImpl instance2;

    @Before
    public void buildSampleGraph(){
        role1 = tx.putRole("role 1");
        role2 = tx.putRole("role 2");
        relationshipType = tx.putRelationshipType("rel type").relates(role1).relates(role2);
        EntityType thing = tx.putEntityType("thingy").plays(role1).plays(role2);
        instance1 = (ThingImpl) thing.create();
        instance2 = (ThingImpl) thing.create();
        thing.create();
        thing.create();

        relationshipType.create().assign(role1, instance1).assign(role2, instance2);
    }

    @Test
    public void whenMergingDuplicateResources_EnsureSingleResourceRemains(){
        AttributeTypeImpl<String> resourceType = (AttributeTypeImpl<String>) tx.putAttributeType("Attribute Type", AttributeType.DataType.STRING);

        //Create fake resources
        Set<ConceptId> resourceIds = new HashSet<>();
        AttributeImpl<String> mainResource = createFakeResource(resourceType, "1");
        resourceIds.add(mainResource.id());
        resourceIds.add(createFakeResource(resourceType, "1").id());
        resourceIds.add(createFakeResource(resourceType, "1").id());

        //Check we have duplicate resources
        assertEquals(3, resourceType.instances().count());

        //Fix duplicates
        tx.fixDuplicateResources(mainResource.getIndex(), resourceIds);

        //Check we no longer have duplicates
        assertEquals(1, resourceType.instances().count());
    }

    @Test
    public void whenMergingDuplicateResourcesWithRelations_EnsureSingleResourceRemainsAndNoDuplicateRelationsAreCreated(){
        Role roleEntity = tx.putRole("Entity Role");
        Role roleResource = tx.putRole("Attribute Role");
        RelationshipType relationshipType = tx.putRelationshipType("Relationship Type").relates(roleEntity).relates(roleResource);
        AttributeTypeImpl<String> resourceType = (AttributeTypeImpl<String>) tx.putAttributeType("Attribute Type", AttributeType.DataType.STRING).plays(roleResource);
        EntityType entityType = tx.putEntityType("Entity Type").plays(roleEntity).has(resourceType);
        Entity e1 = entityType.create();
        Entity e2 = entityType.create();
        Entity e3 = entityType.create();

        //Create fake resources
        Set<ConceptId> resourceIds = new HashSet<>();
        AttributeImpl<?> r1 = createFakeResource(resourceType, "1");
        AttributeImpl<?> r11 = createFakeResource(resourceType, "1");
        AttributeImpl<?> r111 = createFakeResource(resourceType, "1");

        resourceIds.add(r1.id());
        resourceIds.add(r11.id());
        resourceIds.add(r111.id());

        //Give resources some relationships
        addReifiedRelation(roleEntity, roleResource, relationshipType, e1, r1);

        //When merging this relation should not be absorbed
        addReifiedRelation(roleEntity, roleResource, relationshipType, e1, r11);

        //Absorb
        addReifiedRelation(roleEntity, roleResource, relationshipType, e2, r11);

        //Don't Absorb
        addEdgeRelation(e2, r111);

        // Absorb
        addEdgeRelation(e3, r111);

        //Check everything is broken
        assertEquals(3, resourceType.instances().count());
        assertEquals(1, r1.relationships().count());
        assertEquals(2, r11.relationships().count());
        assertEquals(1, r1.relationships().count());
        assertEquals(4, tx.getTinkerTraversal().V().hasLabel(Schema.BaseType.RELATIONSHIP.name()).toList().size());
        assertEquals(2, tx.getTinkerTraversal().E().hasLabel(Schema.EdgeLabel.ATTRIBUTE.getLabel()).toList().size());

        r1.relationships().forEach(rel -> assertTrue(rel.rolePlayers().anyMatch(thing -> thing.equals(e1))));

        //Now fix everything
        tx.fixDuplicateResources(r1.getIndex(), resourceIds);

        //Check everything is in order
        assertEquals(1, resourceType.instances().count());

        //Get back the surviving resource
        Attribute<String> foundR1 = null;
        for (Attribute<String> attribute : resourceType.instances().collect(toSet())) {
            if(attribute.value().equals("1")){
                foundR1 = attribute;
                break;
            }
        }

        assertNotNull(foundR1);
        assertThat(foundR1.owners().collect(toSet()), containsInAnyOrder(e1, e2, e3));
        assertEquals(6, tx.admin().getMetaRelationType().instances().count());
    }

    private void addEdgeRelation(Entity entity, Attribute<?> attribute) {
        entity.has(attribute);
    }

    private void addReifiedRelation(Role roleEntity, Role roleResource, RelationshipType relationshipType, Entity entity, Attribute<?> attribute) {
        relationshipType.create().assign(roleResource, attribute).assign(roleEntity, entity);
    }


    private AttributeImpl<String> createFakeResource(AttributeTypeImpl<String> type, String value){
        String index = Schema.generateAttributeIndex(type.label(), value);
        Vertex resourceVertex = tx.getTinkerPopGraph().addVertex(Schema.BaseType.ATTRIBUTE.name());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), type.currentShard().vertex().element());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(), index);
        resourceVertex.property(Schema.VertexProperty.VALUE_STRING.name(), value);
        resourceVertex.property(Schema.VertexProperty.ID.name(), Schema.PREFIX_VERTEX + resourceVertex.id().toString());

        return AttributeImpl.get(new VertexElement(tx, resourceVertex));
    }

    @Test
    public void whenMergingDuplicateResourceEdges_EnsureNoDuplicatesRemain(){
        AttributeTypeImpl<String> resourceType = (AttributeTypeImpl<String>) tx.putAttributeType("My Sad Attribute", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("My Happy EntityType").has(resourceType);
        RelationshipType relationshipType = tx.putRelationshipType("My Miserable RelationshipType").has(resourceType);
        Entity entity = entityType.create();
        Relationship relationship = relationshipType.create();

        AttributeImpl<?> r1dup1 = createFakeResource(resourceType, "1");
        AttributeImpl<?> r1dup2 = createFakeResource(resourceType, "1");
        AttributeImpl<?> r1dup3 = createFakeResource(resourceType, "1");

        AttributeImpl<?> r2dup1 = createFakeResource(resourceType, "2");
        AttributeImpl<?> r2dup2 = createFakeResource(resourceType, "2");
        AttributeImpl<?> r2dup3 = createFakeResource(resourceType, "2");

        entity.has(r1dup1);
        entity.has(r1dup2);
        entity.has(r1dup3);

        relationship.has(r1dup1);
        relationship.has(r1dup2);
        relationship.has(r1dup3);

        entity.has(r2dup1);

        //Check everything is broken
        //Entities Too Many Resources
        assertEquals(4, entity.attributes().count());
        assertEquals(3, relationship.attributes().count());

        //There are too many resources
        assertEquals(6, tx.admin().getMetaAttributeType().instances().count());

        //Now fix everything for resource 1
        tx.fixDuplicateResources(r1dup1.getIndex(), new HashSet<>(Arrays.asList(r1dup1.id(), r1dup2.id(), r1dup3.id())));

        //Check resource one has been sorted out
        assertEquals(2, entity.attributes().count());
        assertEquals(2, entity.attributes().count());
        assertEquals(4, tx.admin().getMetaAttributeType().instances().count()); // 4 because we still have 2 dups on r2

        //Now fix everything for resource 2
        tx.fixDuplicateResources(r2dup1.getIndex(), new HashSet<>(Arrays.asList(r2dup1.id(), r2dup2.id(), r2dup3.id())));

        //Check resource one has been sorted out
        assertEquals(2, entity.attributes().count());
        assertEquals(2, entity.attributes().count());
        assertEquals(2, tx.admin().getMetaAttributeType().instances().count()); // 4 because we still have 2 dups on r2
    }
}
