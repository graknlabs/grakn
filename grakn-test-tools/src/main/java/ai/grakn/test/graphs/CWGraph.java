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

package ai.grakn.test.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class CWGraph extends TestGraph {

    private static ResourceType<String> key;

    private static EntityType person;
    private static EntityType weapon;
    private static EntityType rocket;
    private static EntityType country;
    
    private static ResourceType<String> alignment, propulsion, nationality;

    private static RelationType isEnemyOf;
    private static RelationType isPaidBy;
    private static RelationType owns;

    private static RoleType enemySource, enemyTarget;
    private static RoleType owner, ownedItem;
    private static RoleType payee, payer;
    private static RoleType seller, buyer, transactionItem;

    private static Thing colonelWest, Nono, America, Tomahawk;

    public static Consumer<GraknGraph> get() {
        return new CWGraph().build();
    }

    @Override
    protected void buildOntology(GraknGraph graph) {
        key = graph.putResourceType("name", ResourceType.DataType.STRING);

        //Resources
        nationality = graph.putResourceType("nationality", ResourceType.DataType.STRING);
        propulsion = graph.putResourceType("propulsion", ResourceType.DataType.STRING);
        alignment = graph.putResourceType("alignment", ResourceType.DataType.STRING);

        //Roles
        owner = graph.putRoleType("item-owner");
        ownedItem = graph.putRoleType("owned-item");
        seller = graph.putRoleType("seller");
        buyer = graph.putRoleType("buyer");
        payee = graph.putRoleType("payee");
        payer = graph.putRoleType("payer");
        enemySource = graph.putRoleType("enemy-source");
        enemyTarget = graph.putRoleType("enemy-target");
        transactionItem = graph.putRoleType("transaction-item");

        //Entitites
        person = graph.putEntityType("person")
                .plays(seller)
                .plays(payee)
                .resource(key)
                .resource(nationality);

        graph.putEntityType("criminal");

        weapon = graph.putEntityType("weapon")
                .plays(transactionItem)
                .plays(ownedItem)
                .resource(key);

        rocket = graph.putEntityType("rocket")
                .plays(transactionItem)
                .plays(ownedItem)
                .resource(key)
                .resource(propulsion);

        graph.putEntityType("missile")
                .superType(weapon)
                .plays(transactionItem)
                .resource(key);

        country = graph.putEntityType("country")
                .plays(buyer)
                .plays(owner)
                .plays(enemyTarget)
                .plays(payer)
                .plays(enemySource)
                .resource(key)
                .resource(alignment);

        //Relations
        owns = graph.putRelationType("owns")
                .relates(owner)
                .relates(ownedItem);

        isEnemyOf = graph.putRelationType("is-enemy-of")
                .relates(enemySource)
                .relates(enemyTarget);

        graph.putRelationType("transaction")
                .relates(seller)
                .relates(buyer)
                .relates(transactionItem);

        isPaidBy = graph.putRelationType("is-paid-by")
                .relates(payee)
                .relates(payer);
    }

    @Override
    protected void buildInstances(GraknGraph graph) {
        colonelWest =  putEntity(graph, "colonelWest", person, key.getLabel());
        Nono =  putEntity(graph, "Nono", country, key.getLabel());
        America =  putEntity(graph, "America", country, key.getLabel());
        Tomahawk =  putEntity(graph, "Tomahawk", rocket, key.getLabel());

        putResource(colonelWest, nationality, "American");
        putResource(Tomahawk, propulsion, "gsp");
    }

    @Override
    protected void buildRelations(GraknGraph graph) {
        //Enemy(Nono, America)
        isEnemyOf.addRelation()
                .addRolePlayer(enemySource, Nono)
                .addRolePlayer(enemyTarget, America);

        //Owns(Nono, Missile)
        owns.addRelation()
                .addRolePlayer(owner, Nono)
                .addRolePlayer(ownedItem, Tomahawk);

        //isPaidBy(West, Nono)
        isPaidBy.addRelation()
                .addRolePlayer(payee, colonelWest)
                .addRolePlayer(payer, Nono);
    }

    @Override
    protected void buildRules(GraknGraph graph) {
        RuleType inferenceRule = graph.admin().getMetaRuleInference();

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        Pattern R1_LHS = graph.graql().parsePattern("{$x isa person;$x has nationality 'American';" +
                        "$y isa weapon;" +
                        "$z isa country;$z has alignment 'hostile';" +
                        "(seller: $x, transaction-item: $y, buyer: $z) isa transaction;}");

        Pattern R1_RHS = graph.graql().parsePattern("{$x isa criminal;}");
        inferenceRule.putRule(R1_LHS, R1_RHS);

        //R2: "Missiles are a kind of a weapon"
        Pattern R2_LHS = graph.graql().parsePattern("{$x isa missile;}");
        Pattern R2_RHS = graph.graql().parsePattern("{$x isa weapon;}");
        inferenceRule.putRule(R2_LHS, R2_RHS);

        //R3: "If a country is an enemy of America then it is hostile"
        Pattern R3_LHS = graph.graql().parsePattern(
                "{$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y has name 'America';}");
        Pattern R3_RHS = graph.graql().parsePattern("{$x has alignment 'hostile';}");
        inferenceRule.putRule(R3_LHS, R3_RHS);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        Pattern R4_LHS = graph.graql().parsePattern("{$x isa rocket;$x has propulsion 'gsp';}");
        Pattern R4_RHS = graph.graql().parsePattern("{$x isa missile;}");
        inferenceRule.putRule(R4_LHS, R4_RHS);

        Pattern R5_LHS = graph.graql().parsePattern(
                "{$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;" +
                "($y, $z) isa owns;}");

        Pattern R5_RHS = graph.graql().parsePattern("{(seller: $x, buyer: $y, transaction-item: $z) isa transaction;}");
        inferenceRule.putRule(R5_LHS, R5_RHS);
    }
}
