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

package io.grakn.graph.internal;

import io.grakn.Grakn;
import io.grakn.concept.EntityType;
import io.grakn.concept.Instance;
import io.grakn.concept.Relation;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;
import io.grakn.exception.GraknValidationException;
import io.grakn.util.ErrorMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class OntologyMutationTest {
    private AbstractGraknGraph graknGraph;
    private RoleType husband;
    private RoleType wife;
    private RelationType marriage;
    private EntityType person;
    private EntityType woman;
    private EntityType man;
    private EntityType car;
    private Instance alice;
    private Instance bob;
    private Relation relation;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph() throws GraknValidationException {
        graknGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();

        //spouse = graknGraph.putRoleType("Spouse");
        husband = graknGraph.putRoleType("Husband");//.superType(spouse);
        wife = graknGraph.putRoleType("Wife");
        RoleType driver = graknGraph.putRoleType("Driver");
        RoleType driven = graknGraph.putRoleType("Driven");

        //union = graknGraph.putRelationType("Union").hasRole(spouse).hasRole(wife);
        marriage = graknGraph.putRelationType("marriage").hasRole(husband).hasRole(wife);
        RelationType carBeingDrivenBy = graknGraph.putRelationType("car being driven by").hasRole(driven).hasRole(driver);

        person = graknGraph.putEntityType("Person").playsRole(husband).playsRole(wife);
        man = graknGraph.putEntityType("Man").superType(person);
        woman = graknGraph.putEntityType("Woman").superType(person);
        car = graknGraph.putEntityType("Car");

        alice = graknGraph.addEntity(woman);
        bob = graknGraph.addEntity(man);
        relation = graknGraph.addRelation(marriage).putRolePlayer(wife, alice).putRolePlayer(husband, bob);
        graknGraph.commit();
    }
    @After
    public void destroyGraph()  throws Exception{
        graknGraph.close();
    }

    @Test
    public void testDeletePlaysRole() throws GraknValidationException {
        person.deletePlaysRole(wife);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(woman.getId(), alice.getId(), wife.getId()))
        ));

        graknGraph.commit();
    }

    @Test
    public void testDeleteHasRole() throws GraknValidationException {
        marriage.deleteHasRole(husband);

        String roles = "";
        String rolePlayers = "";
        for(Map.Entry<RoleType, Instance> entry: relation.rolePlayers().entrySet()){
            if(entry.getKey() != null)
                roles = roles + entry.getKey().getId() + ",";
            if(entry.getValue() != null)
                rolePlayers = rolePlayers + entry.getValue().getId() + ",";
        }

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_RELATION.getMessage(relation.getId(), marriage.getId(),
                        roles.split(",").length, roles,
                        rolePlayers.split(",").length, roles))
        ));

        graknGraph.commit();
    }

    @Test
    public void testChangeSuperTypeOfEntityType() throws GraknValidationException {
        man.superType(car);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(man.getId(), bob.getId(), husband.getId()))
        ));

        graknGraph.commit();
    }

    @Test
    public void testChangeIsAbstract() throws GraknValidationException {
        man.setAbstract(true);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(man.getId()))
        ));

        graknGraph.commit();
    }

}
