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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.util.Schema;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RelationshipTypeTest extends TxTestBase {
    @Test
    public void whenGettingTheRolesOfRelationTypes_AllTheRolesAreReturned() throws Exception {
        RelationshipType relationshipType = tx.putRelationshipType("relationTypes");
        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        Role role3 = tx.putRole("role3");
        relationshipType.relates(role1).relates(role2).relates(role3);
        assertThat(relationshipType.roles().collect(toSet()), containsInAnyOrder(role1, role2, role3));
    }

    @Test
    public void whenMutatingRolesOfRelationType_EnsureRelationTypeRolesAreAlwaysUpdated(){
        RelationshipType relationshipType = tx.putRelationshipType("c1");
        Role role1 = tx.putRole("c2");
        Role role2 = tx.putRole("c3");
        assertThat(relationshipType.roles().collect(toSet()), empty());

        relationshipType.relates(role1).relates(role2);
        assertThat(relationshipType.roles().collect(toSet()), containsInAnyOrder(role1, role2));

        relationshipType.unrelate(role1);
        assertThat(relationshipType.roles().collect(toSet()), containsInAnyOrder(role2));
    }

    @Test
    public void whenCallingInstancesOnImplicitRelationType_RelationEdgesAreReturned(){
        AttributeType<String> attributeType = tx.putAttributeType("My Special Attribute Type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("Ad thing");

        EntityType entityType = tx.putEntityType("My Special Entity Type").has(attributeType);
        Entity entity = entityType.create();

        RelationshipType implicitRelationshipType = tx.getRelationshipType(Schema.ImplicitType.HAS.getLabel(attributeType.label()).getValue());

        assertNotNull(implicitRelationshipType);
        assertThat(implicitRelationshipType.instances().collect(toSet()), empty());

        entity.has(attribute);

        assertEquals(1, implicitRelationshipType.instances().count());
    }

    @Test
    public void whenSettingAnImplicitRelationTypeWithInstancesAbstract_Throw(){
        AttributeType<String> attributeType = tx.putAttributeType("My Special Attribute Type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("Ad thing");

        EntityType entityType = tx.putEntityType("My Special Entity Type").has(attributeType);
        entityType.create().has(attribute);

        RelationshipType implicitRelationshipType = tx.getRelationshipType(Schema.ImplicitType.HAS.getLabel(attributeType.label()).getValue());

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.addingInstancesToAbstractType(implicitRelationshipType).getMessage());

        implicitRelationshipType.isAbstract(true);
    }
}