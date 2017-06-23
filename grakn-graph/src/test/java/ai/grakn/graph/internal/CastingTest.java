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
import ai.grakn.concept.Thing;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CastingTest extends GraphTestBase {

    private RelationType relationType;
    private EntityType entityType;
    private RoleType role3;
    private RoleType role2;
    private RoleType role1;

    @Before
    public void createOntology(){
        role1 = graknGraph.putRoleType("role1");
        role2 = graknGraph.putRoleType("role2");
        role3 = graknGraph.putRoleType("role3");
        entityType = graknGraph.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);
        relationType = graknGraph.putRelationType("Relation Type").relates(role1).relates(role2).relates(role3);
    }

    @Test
    public void whenCreatingRelation_EnsureRolePlayerContainsInstanceRoleTypeRelationTypeAndRelation(){
        Entity e1 = entityType.addEntity();

        RelationImpl relation = (RelationImpl) relationType.addRelation().
                addRolePlayer(role1, e1);

        Set<Casting> castings = relation.castingsRelation().collect(Collectors.toSet());

        castings.forEach(rolePlayer -> {
            assertEquals(e1, rolePlayer.getInstance());
            assertEquals(role1, rolePlayer.getRoleType());
            assertEquals(relationType, rolePlayer.getRelationType());
            assertEquals(relation, rolePlayer.getRelation());
        });
    }

    @Test
    public void whenUpdatingRelation_EnsureRolePlayersAreUpdated(){
        Entity e1 = entityType.addEntity();
        Entity e3 = entityType.addEntity();

        RelationImpl relation = (RelationImpl) relationType.addRelation().
                addRolePlayer(role1, e1);

        Set<Thing> things = relation.castingsRelation().map(Casting::getInstance).collect(Collectors.toSet());
        Set<RoleType> roles = relation.castingsRelation().map(Casting::getRoleType).collect(Collectors.toSet());
        assertThat(things, containsInAnyOrder(e1));
        assertThat(roles, containsInAnyOrder(role1));

        //Now Update
        relation.addRolePlayer(role2, e1).addRolePlayer(role3, e3);

        things = relation.castingsRelation().map(Casting::getInstance).collect(Collectors.toSet());
        roles = relation.castingsRelation().map(Casting::getRoleType).collect(Collectors.toSet());
        assertThat(things, containsInAnyOrder(e1, e3));
        assertThat(roles, containsInAnyOrder(role1, role2, role3));
    }
}
