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

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class GeoGraph extends TestGraph {

    private static AttributeType<String> key;

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationshipType isLocatedIn;

    private static Role geoEntity, entityLocation;

    private static Thing Europe;
    private static Thing Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Thing Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Thing Poland, England, Germany, France, Italy;
    private static Thing UW;
    private static Thing PW;
    private static Thing Imperial;
    private static Thing UCL;

    public static Consumer<GraknTx> get(){
        return new GeoGraph().build();
    }

    @Override
    public void buildSchema(GraknTx graph) {
        key = graph.putAttributeType("name", AttributeType.DataType.STRING);

        geoEntity = graph.putRole("geo-entity");
        entityLocation = graph.putRole("entity-location");
        isLocatedIn = graph.putRelationshipType("is-located-in")
                .relates(geoEntity).relates(entityLocation);

        geographicalObject = graph.putEntityType("geoObject")
                .plays(geoEntity)
                .plays(entityLocation);
        geographicalObject.attribute(key);

        continent = graph.putEntityType("continent")
                .sup(geographicalObject)
                .plays(entityLocation);
        country = graph.putEntityType("country")
                .sup(geographicalObject)
                .plays(geoEntity)
                .plays(entityLocation);
        region = graph.putEntityType("region")
                .sup(geographicalObject)
                .plays(geoEntity)
                .plays(entityLocation);
        city = graph.putEntityType("city")
                .sup(geographicalObject)
                .plays(geoEntity)
                .plays(entityLocation);
        university = graph.putEntityType("university")
                        .plays(geoEntity);
        university.attribute(key);
    }

    @Override
    public void buildInstances(GraknTx graph) {
        Europe = putEntity(graph, "Europe", continent, key.getLabel());

        Poland = putEntity(graph, "Poland", country, key.getLabel());
        Masovia = putEntity(graph, "Masovia", region, key.getLabel());
        Silesia = putEntity(graph, "Silesia", region, key.getLabel());
        Warsaw = putEntity(graph, "Warsaw", city, key.getLabel());
        Wroclaw = putEntity(graph, "Wroclaw", city, key.getLabel());
        UW = putEntity(graph, "University-of-Warsaw", university, key.getLabel());
        PW = putEntity(graph, "Warsaw-Polytechnics", university, key.getLabel());

        England = putEntity(graph, "England", country, key.getLabel());
        GreaterLondon = putEntity(graph, "GreaterLondon", region, key.getLabel());
        London = putEntity(graph, "London", city, key.getLabel());
        Imperial = putEntity(graph, "Imperial College London", university, key.getLabel());
        UCL = putEntity(graph, "University College London", university, key.getLabel());

        Germany = putEntity(graph, "Germany", country, key.getLabel());
        Bavaria = putEntity(graph, "Bavaria", region, key.getLabel());
        Munich = putEntity(graph, "Munich", city, key.getLabel());
        putEntity(graph, "University of Munich", university, key.getLabel());

        France = putEntity(graph, "France", country, key.getLabel());
        IleDeFrance = putEntity(graph, "IleDeFrance", region, key.getLabel());
        Paris = putEntity(graph, "Paris", city, key.getLabel());

        Italy = putEntity(graph, "Italy", country, key.getLabel());
        Lombardy = putEntity(graph, "Lombardy", region, key.getLabel());
        Milan = putEntity(graph, "Milan", city, key.getLabel());
    }

    @Override
    public void buildRelations(GraknTx graph) {
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Poland)
                .addRolePlayer(entityLocation, Europe);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Masovia)
                .addRolePlayer(entityLocation, Poland);

        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Silesia)
                .addRolePlayer(entityLocation, Poland);

        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Warsaw)
                .addRolePlayer(entityLocation, Masovia);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Wroclaw)
                .addRolePlayer(entityLocation, Silesia);

        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, PW)
                .addRolePlayer(entityLocation, Warsaw);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, UW)
                .addRolePlayer(entityLocation, Warsaw);


        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Imperial)
                .addRolePlayer(entityLocation, London);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, UCL)
                .addRolePlayer(entityLocation, London);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, London)
                .addRolePlayer(entityLocation, GreaterLondon);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, GreaterLondon)
                .addRolePlayer(entityLocation, England);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, England)
               .addRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Munich)
                .addRolePlayer(entityLocation, Bavaria);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Bavaria)
                .addRolePlayer(entityLocation, Germany);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Germany)
                .addRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Milan)
                .addRolePlayer(entityLocation, Lombardy);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Lombardy)
                .addRolePlayer(entityLocation, Italy);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Italy)
                .addRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, Paris)
                .addRolePlayer(entityLocation, IleDeFrance);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, IleDeFrance)
                .addRolePlayer(entityLocation, France);
        isLocatedIn.addRelationship()
                .addRolePlayer(geoEntity, France)
                .addRolePlayer(entityLocation, Europe);

    }

    @Override
    public void buildRules(GraknTx graph) {
        RuleType inferenceRule = graph.admin().getMetaRuleInference();
        Pattern transitivity_LHS = graph.graql().parsePattern(
                "{(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;}");
        Pattern transitivity_RHS = graph.graql().parsePattern("{(geo-entity: $x, entity-location: $z) isa is-located-in;}");
        inferenceRule.putRule(transitivity_LHS, transitivity_RHS);
    }
}
