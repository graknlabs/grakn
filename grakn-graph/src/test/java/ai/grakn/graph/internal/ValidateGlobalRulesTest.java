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

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateGlobalRulesTest extends GraphTestBase{

    @Test(expected = InvocationTargetException.class)
    public void testConstructor() throws Exception { //Checks that you cannot initialise it.
        Constructor<ValidateGlobalRules> c = ValidateGlobalRules.class.getDeclaredConstructor();
        c.setAccessible(true);
        ValidateGlobalRules u = c.newInstance();
    }


    @Test
    public void testValidatePlaysRoleStructure() throws Exception {
        Type fakeType = graknGraph.putEntityType("Fake Concept");
        EntityTypeImpl wolf = (EntityTypeImpl) graknGraph.putEntityType("wolf");
        EntityTypeImpl creature = (EntityTypeImpl) graknGraph.putEntityType("creature");
        EntityTypeImpl hunter = (EntityTypeImpl) graknGraph.putEntityType("hunter");
        RoleType animal = graknGraph.putRoleType("animal");
        RelationType hunts = graknGraph.putRelationType("hunts");
        RoleTypeImpl witcher = (RoleTypeImpl) graknGraph.putRoleType("witcher");
        RoleTypeImpl monster = (RoleTypeImpl) graknGraph.putRoleType("monster");
        Instance geralt = hunter.addEntity();
        InstanceImpl werewolf = (InstanceImpl) wolf.addEntity();

        RelationImpl assertion = (RelationImpl) hunts.addRelation().
                putRolePlayer(witcher, geralt).putRolePlayer(monster, werewolf);
        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertTrue(ValidateGlobalRules.validatePlaysRoleStructure(casting).isPresent());
        }

        hunter.playsRole(witcher);

        boolean [] flags = {false, false};
        int count = 0;
        for (CastingImpl casting : assertion.getMappingCasting()) {
            flags[count] = ValidateGlobalRules.validatePlaysRoleStructure(casting).isPresent();
            count++;
        }
        assertFalse(flags[0] && flags[1]);
        assertTrue(flags[0] || flags[1]);

        wolf.superType(creature);
        creature.playsRole(monster);

        for (CastingImpl casting : assertion.getMappingCasting()) {
            assertFalse(ValidateGlobalRules.validatePlaysRoleStructure(casting).isPresent());
        }
    }

    @Test
    public void testValidatePlaysRoleStructureUnique() {
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RelationType relationType = graknGraph.putRelationType("rt").hasRole(role1).hasRole(role2);

        EntityType entityType = graknGraph.putEntityType("et");

        ((EntityTypeImpl) entityType).playsRole(role1, true);
        ((EntityTypeImpl) entityType).playsRole(role2, false);

        Entity other1 = entityType.addEntity();
        Entity other2 = entityType.addEntity();

        EntityImpl entity = (EntityImpl) entityType.addEntity();

        RelationImpl relation1 = (RelationImpl) relationType.addRelation()
                .putRolePlayer(role2, other1).putRolePlayer(role1, entity);

        // Valid with only a single relation
        relation1.getMappingCasting().forEach(casting -> {
            assertFalse(ValidateGlobalRules.validatePlaysRoleStructure(casting).isPresent());
        });

        RelationImpl relation2 = (RelationImpl) relationType.addRelation()
                .putRolePlayer(role2, other2).putRolePlayer(role1, entity);

        // Invalid with multiple relations
        relation1.getMappingCasting().forEach(casting -> {
            if (casting.getRole().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysRoleStructure(casting).isPresent());
            }
        });
        relation2.getMappingCasting().forEach(casting -> {
            if (casting.getRole().equals(role1)) {
                assertTrue(ValidateGlobalRules.validatePlaysRoleStructure(casting).isPresent());
            }
        });
    }

    @Test
    public void testValidateRelationTypeHasRoles() throws Exception {
        RoleType hunter = graknGraph.putRoleType("hunter");
        RoleType monster = graknGraph.putRoleType("monster");
        RoleType creature = graknGraph.putRoleType("creature");
        RelationType kills = graknGraph.putRelationType("kills");

        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.hasRole(hunter);
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.hasRole(hunter);
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.hasRole(monster);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
        kills.hasRole(creature);
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(kills).isPresent());
    }

    @Test
    public void testValidateAssertionStructure() throws Exception {
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        RoleType napper = graknGraph.putRoleType("napper");
        RoleType hunter = graknGraph.putRoleType("hunter");
        RoleType monster = graknGraph.putRoleType("monster");
        RoleType creature = graknGraph.putRoleType("creature");
        Instance cthulhu = fakeType.addEntity();
        Instance werewolf = fakeType.addEntity();
        Instance cartman = fakeType.addEntity();
        RelationType kills = graknGraph.putRelationType("kills");
        RelationType naps = graknGraph.putRelationType("naps").hasRole(napper);

        RelationImpl assertion = (RelationImpl) kills.addRelation().
                putRolePlayer(hunter, cartman).putRolePlayer(monster, werewolf).putRolePlayer(creature, cthulhu);

        kills.hasRole(monster);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion).isPresent());

        kills.hasRole(hunter);
        kills.hasRole(creature);
        assertFalse(ValidateGlobalRules.validateRelationshipStructure(assertion).isPresent());

        RelationImpl assertion2 = (RelationImpl) naps.addRelation().putRolePlayer(hunter, cthulhu);
        assertTrue(ValidateGlobalRules.validateRelationshipStructure(assertion2).isPresent());
    }


    @Test
    public void testAbstractConceptValidation(){
        RoleType roleType = graknGraph.putRoleType("hasRole");
        RelationType relationType = graknGraph.putRelationType("relationTypes");

        assertTrue(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(roleType).isPresent());
        assertTrue(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());

        roleType.setAbstract(true);
        relationType.setAbstract(true);

        assertFalse(ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(roleType).isPresent());
        assertFalse(ValidateGlobalRules.validateHasMinimumRoles(relationType).isPresent());
    }

    @Test
    public void testAbstractInstancesDoNotValidateSubTypes(){
        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType entityType = graknGraph.putEntityType("entityType").playsRole(r1).playsRole(r2);
        RelationType relationType = graknGraph.putRelationType("relationTypes").setAbstract(true);
        RelationType hasCast = graknGraph.putRelationType("has cast").superType(relationType).hasRole(r1).hasRole(r2);

        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();

        hasCast.addRelation().putRolePlayer(r1, e1).putRolePlayer(r2, e2);

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges((TypeImpl) relationType).isPresent());
    }

    @Test
    public void validateIsAbstractHasNoIncomingIsaEdges() {
        EntityTypeImpl x1 = (EntityTypeImpl) graknGraph.putEntityType("x1");
        EntityTypeImpl x2 = (EntityTypeImpl) graknGraph.putEntityType("x2'");
        EntityTypeImpl x3 = (EntityTypeImpl) graknGraph.putEntityType("x3");
        EntityTypeImpl x4 = (EntityTypeImpl) graknGraph.putEntityType("x4'");

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1).isPresent());
        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x2).isPresent());
        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x3).isPresent());
        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x4).isPresent());

        x2.superType(x1);

        assertFalse(ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(x1).isPresent());
    }
}