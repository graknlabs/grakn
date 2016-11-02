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

package io.mindmaps.graph.internal;

import io.mindmaps.Mindmaps;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.ConceptException;
import io.mindmaps.exception.InvalidConceptTypeException;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class TypeTest {

    private AbstractMindmapsGraph mindmapsGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph(){
        mindmapsGraph = (AbstractMindmapsGraph) Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        mindmapsGraph.initialiseMetaConcepts();
        EntityType top = mindmapsGraph.putEntityType("top");
        EntityType middle1 = mindmapsGraph.putEntityType("mid1");
        EntityType middle2 = mindmapsGraph.putEntityType("mid2");
        EntityType middle3 = mindmapsGraph.putEntityType("mid3'");
        EntityType bottom = mindmapsGraph.putEntityType("bottom");

        bottom.superType(middle1);
        middle1.superType(top);
        middle2.superType(top);
        middle3.superType(top);
    }
    @After
    public void destroyGraph()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testItemIdentifier(){
        Type test = mindmapsGraph.putEntityType("test");
        assertEquals("test", test.getId());
    }

    @Test
    public void testGetRoleTypeAsConceptType(){
        RoleType test1 = mindmapsGraph.putRoleType("test");
        Type test2 = mindmapsGraph.getEntityType("test");
        assertNull(test2);
    }

    @Test
    public void testGetPlayedRole() throws Exception{
        EntityType creature = mindmapsGraph.putEntityType("creature");
        RoleType monster = mindmapsGraph.putRoleType("monster");
        RoleType animal = mindmapsGraph.putRoleType("animal");
        RoleType monsterSub = mindmapsGraph.putRoleType("monsterSub");

        assertEquals(0, creature.playsRoles().size());

        creature.playsRole(monster);
        creature.playsRole(animal);
        monsterSub.superType(monster);

        assertEquals(3, creature.playsRoles().size());
        assertTrue(creature.playsRoles().contains(monster));
        assertTrue(creature.playsRoles().contains(animal));
        assertTrue(creature.playsRoles().contains(monsterSub));
        assertTrue(creature.playsRoles().contains(monsterSub));
    }

    @Test
    public void testGetSubHierarchySuperSet() throws Exception{
        TypeImpl c1 = (TypeImpl) mindmapsGraph.putEntityType("c1");
        TypeImpl c2 = (TypeImpl) mindmapsGraph.putEntityType("c2");
        TypeImpl c3 = (TypeImpl) mindmapsGraph.putEntityType("c3'");
        TypeImpl c4 = (TypeImpl) mindmapsGraph.putEntityType("c4");

        assertTrue(c1.getSubHierarchySuperSet().contains(c1));
        assertFalse(c1.getSubHierarchySuperSet().contains(c2));
        assertFalse(c1.getSubHierarchySuperSet().contains(c3));
        assertFalse(c1.getSubHierarchySuperSet().contains(c4));

        c1.superType(c2);
        assertTrue(c1.getSubHierarchySuperSet().contains(c1));
        assertTrue(c1.getSubHierarchySuperSet().contains(c2));
        assertFalse(c1.getSubHierarchySuperSet().contains(c3));
        assertFalse(c1.getSubHierarchySuperSet().contains(c4));

        c2.superType(c3);
        assertTrue(c1.getSubHierarchySuperSet().contains(c1));
        assertTrue(c1.getSubHierarchySuperSet().contains(c2));
        assertTrue(c1.getSubHierarchySuperSet().contains(c3));
        assertFalse(c1.getSubHierarchySuperSet().contains(c4));

        mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), c3.getId()).
                outE(Schema.EdgeLabel.ISA.getLabel()).next().remove();
        c3.superType(c4);
        boolean correctExceptionThrown = false;
        try{
            c4.superType(c2);
            c1.getSubHierarchySuperSet();
        } catch(RuntimeException e){
            correctExceptionThrown = e.getMessage().contains("loop");
        }
        assertTrue(correctExceptionThrown);

    }

    @Test
    public void testCannotSubClassMetaTypes(){
        RuleType metaType = mindmapsGraph.getMetaRuleInference();
        RuleType superType = mindmapsGraph.putRuleType("An Entity Type");

        expectedException.expect(InvalidConceptTypeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CANNOT_SUBCLASS_META.getMessage(metaType.getId(), superType.getId()))
        ));

        superType.superType(metaType);
    }

    @Test
    public void testGetSubChildrenSet(){
        EntityType parent = mindmapsGraph.putEntityType("parent");
        EntityType child1 = mindmapsGraph.putEntityType("c1");
        EntityType child2 = mindmapsGraph.putEntityType("c2");
        EntityType child3 = mindmapsGraph.putEntityType("c3");

        assertEquals(1, parent.subTypes().size());

        child1.superType(parent);
        child2.superType(parent);
        child3.superType(parent);

        assertEquals(4, parent.subTypes().size());
        assertTrue(parent.subTypes().contains(child3));
        assertTrue(parent.subTypes().contains(child2));
        assertTrue(parent.subTypes().contains(child1));
    }

    @Test
    public void testGetSubHierarchySubSet(){
        EntityType parent = mindmapsGraph.putEntityType("p");
        EntityType superParent = mindmapsGraph.putEntityType("sp");
        EntityType child1 = mindmapsGraph.putEntityType("c1");
        EntityType child2 = mindmapsGraph.putEntityType("c2");
        EntityType child3 = mindmapsGraph.putEntityType("c3");
        EntityType child3a = mindmapsGraph.putEntityType("3a");
        EntityType child3b = mindmapsGraph.putEntityType("3b");
        EntityType child3b1 = mindmapsGraph.putEntityType("3b1");
        EntityType child3b2 = mindmapsGraph.putEntityType("3b2");
        EntityType child3b3 = mindmapsGraph.putEntityType("3b3");

        assertEquals(1, ((TypeImpl) parent).subTypes().size());

        parent.superType(superParent);
        child1.superType(parent);
        child2.superType(parent);
        child3.superType(parent);
        child3a.superType(child3);
        child3b.superType(child3a);
        child3b1.superType(child3b);
        child3b2.superType(child3b);
        child3b3.superType(child3b);

        assertEquals(9, ((TypeImpl) parent).subTypes().size());
        assertTrue(((TypeImpl) parent).subTypes().contains(parent));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3));
        assertTrue(((TypeImpl) parent).subTypes().contains(child2));
        assertTrue(((TypeImpl) parent).subTypes().contains(child1));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3a));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b1));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b2));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b3));
        assertFalse(((TypeImpl) parent).subTypes().contains(superParent));

    }

    @Test
    public void testDuplicateConceptType(){
        Type movie = mindmapsGraph.putEntityType("movie");
        Type moive2 = mindmapsGraph.putEntityType("movie");
        assertEquals(movie, moive2);
    }

    @Test
    public void testSuperConceptType(){
        EntityType parent = mindmapsGraph.putEntityType("p");
        EntityType superParent = mindmapsGraph.putEntityType("sp");
        EntityType superParent2 = mindmapsGraph.putEntityType("sp2");

        parent.superType(superParent);
        assertNotEquals(superParent2, parent.superType());
        assertEquals(superParent, parent.superType());

        parent.superType(superParent2);
        assertNotEquals(superParent, parent.superType());
        assertEquals(superParent2, parent.superType());
    }

    @Test
    public void allowsRoleType(){
        EntityTypeImpl conceptType = (EntityTypeImpl) mindmapsGraph.putEntityType("ct");
        RoleType roleType1 = mindmapsGraph.putRoleType("rt1'");
        RoleType roleType2 = mindmapsGraph.putRoleType("rt2");

        conceptType.playsRole(roleType1).playsRole(roleType2);
        Set<RoleType> foundRoles = new HashSet<>();
        mindmapsGraph.getTinkerPopGraph().traversal().V(conceptType.getBaseIdentifier()).
                out(Schema.EdgeLabel.PLAYS_ROLE.getLabel()).forEachRemaining(r -> foundRoles.add(mindmapsGraph.getRoleType(r.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()))));

        assertEquals(2, foundRoles.size());
        assertTrue(foundRoles.contains(roleType1));
        assertTrue(foundRoles.contains(roleType2));
    }

    @Test
    public void checkSuperConceptTypeOverride(){
        EntityTypeImpl conceptType = (EntityTypeImpl) mindmapsGraph.putEntityType("A Thing");
        EntityTypeImpl conceptType2 = (EntityTypeImpl) mindmapsGraph.putEntityType("A Super Thing");
        assertNotNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.ISA));
        assertNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.SUB));
        conceptType.superType(conceptType2);
        assertNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.ISA));
        assertNotNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.SUB));
    }

    @Test
    public void testRulesOfHypothesis(){
        Type type = mindmapsGraph.putEntityType("A Concept Type");
        RuleType ruleType = mindmapsGraph.putRuleType("A Rule Type");
        assertEquals(0, type.getRulesOfHypothesis().size());
        Rule rule1 = mindmapsGraph.addRule("lhs", "rhs", ruleType).addHypothesis(type);
        Rule rule2 = mindmapsGraph.addRule("lhs", "rhs", ruleType).addHypothesis(type);
        assertEquals(2, type.getRulesOfHypothesis().size());
        assertTrue(type.getRulesOfHypothesis().contains(rule1));
        assertTrue(type.getRulesOfHypothesis().contains(rule2));
    }

    @Test
    public void getRulesOfConclusion(){
        Type type = mindmapsGraph.putEntityType("A Concept Type");
        RuleType ruleType = mindmapsGraph.putRuleType("A Rule Type");
        assertEquals(0, type.getRulesOfConclusion().size());
        Rule rule1 = mindmapsGraph.addRule("lhs", "rhs", ruleType).addConclusion(type);
        Rule rule2 = mindmapsGraph.addRule("lhs", "rhs", ruleType).addConclusion(type);
        assertEquals(2, type.getRulesOfConclusion().size());
        assertTrue(type.getRulesOfConclusion().contains(rule1));
        assertTrue(type.getRulesOfConclusion().contains(rule2));
    }

    @Test
    public void testDeletePlaysRole(){
        EntityType type = mindmapsGraph.putEntityType("A Concept Type");
        RoleType role1 = mindmapsGraph.putRoleType("A Role 1");
        RoleType role2 = mindmapsGraph.putRoleType("A Role 2");
        assertEquals(0, type.playsRoles().size());
        type.playsRole(role1).playsRole(role2);
        assertEquals(2, type.playsRoles().size());
        assertTrue(type.playsRoles().contains(role1));
        assertTrue(type.playsRoles().contains(role2));
        type.deletePlaysRole(role1);
        assertEquals(1, type.playsRoles().size());
        assertFalse(type.playsRoles().contains(role1));
        assertTrue(type.playsRoles().contains(role2));
    }

    @Test
    public void testDeleteConceptType(){
        EntityType toDelete = mindmapsGraph.putEntityType("1");
        assertNotNull(mindmapsGraph.getConcept("1"));
        toDelete.delete();
        assertNull(mindmapsGraph.getConcept("1"));

        toDelete = mindmapsGraph.putEntityType("2");
        Instance instance = mindmapsGraph.addEntity(toDelete);

        boolean conceptExceptionThrown = false;
        try{
            toDelete.delete();
        } catch (ConceptException e){
            conceptExceptionThrown = true;
        }
        assertTrue(conceptExceptionThrown);
    }

    @Test
    public void testGetInstances(){
        EntityType entityType = mindmapsGraph.putEntityType("Entity");
        RoleType actor = mindmapsGraph.putRoleType("Actor");
        mindmapsGraph.addEntity(entityType);
        EntityType production = mindmapsGraph.putEntityType("Production");
        EntityType movie = mindmapsGraph.putEntityType("Movie").superType(production);
        Instance musicVideo = mindmapsGraph.addEntity(production);
        Instance godfather = mindmapsGraph.addEntity(movie);

        Collection<? extends Concept> types = mindmapsGraph.getMetaType().instances();
        Collection<? extends Concept> data = production.instances();

        assertEquals(11, types.size());
        assertEquals(2, data.size());

        assertTrue(types.contains(actor));
        assertTrue(types.contains(movie));
        assertTrue(types.contains(production));

        assertTrue(data.contains(godfather));
        assertTrue(data.contains(musicVideo));
    }

    @Test(expected=ConceptException.class)
    public void testCircularSub(){
        EntityType entityType = mindmapsGraph.putEntityType("Entity");
        entityType.superType(entityType);
    }

    @Test(expected=ConceptException.class)
    public void testCircularSubLong(){
        EntityType entityType1 = mindmapsGraph.putEntityType("Entity1");
        EntityType entityType2 = mindmapsGraph.putEntityType("Entity2");
        EntityType entityType3 = mindmapsGraph.putEntityType("Entity3");
        entityType1.superType(entityType2);
        entityType2.superType(entityType3);
        entityType3.superType(entityType1);
    }


    @Test
    public void testMetaTypeIsAbstractImmutable(){
        Type meta = mindmapsGraph.getMetaRuleType();

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(meta.getId()))
        ));

        meta.setAbstract(true);
    }

    @Test
    public void testMetaTypePlaysRoleImmutable(){
        Type meta = mindmapsGraph.getMetaRuleType();
        RoleType roleType = mindmapsGraph.putRoleType("A Role");

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(meta.getId()))
        ));

        meta.playsRole(roleType);
    }

    @Test
    public void testHasResource(){
        String resourceTypeId = "Resource Type";
        EntityType entityType = mindmapsGraph.putEntityType("Entity1");
        ResourceType resourceType = mindmapsGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        RelationType relationType = entityType.hasResource(resourceType);
        assertEquals(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId), relationType.getId());

        Set<String> roleIds = relationType.hasRoles().stream().map(Concept::getId).collect(Collectors.toSet());
        assertEquals(2, roleIds.size());

        assertTrue(roleIds.contains(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId)));
        assertTrue(roleIds.contains(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId)));

        assertEquals(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId), entityType.playsRoles().iterator().next().getId());
        assertEquals(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId), resourceType.playsRoles().iterator().next().getId());
    }
}