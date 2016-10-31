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

package io.grakn.example;

import io.grakn.GraknGraph;
import io.grakn.concept.Entity;
import io.grakn.concept.EntityType;
import io.grakn.concept.Instance;
import io.grakn.concept.RelationType;
import io.grakn.concept.Resource;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.concept.RuleType;
import io.grakn.exception.GraknValidationException;
import io.grakn.util.ErrorMessage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * A class which loads sample data into a grakn graph
 */
public class MovieGraphFactory {
    private static GraknGraph graknGraph;
    private static EntityType movie, person, genre, character, cluster, language;
    private static ResourceType<String> title, gender, realName, name;
    private static ResourceType<Long> tmdbVoteCount, releaseDate, runtime;
    private static ResourceType<Double> tmdbVoteAverage;
    private static RelationType hasCast, directedBy, hasGenre, hasCluster;
    private static RoleType productionBeingDirected, director, productionWithCast, actor, characterBeingPlayed;
    private static RoleType genreOfProduction, productionWithGenre, clusterOfProduction, productionWithCluster;

    private static Instance godfather, theMuppets, heat, apocalypseNow, hocusPocus, spy, chineseCoffee;
    private static Instance marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw;
    private static Instance mirandaHeart, betteMidler, sarahJessicaParker;
    private static Instance crime, drama, war, action, comedy, family, musical, fantasy;
    private static Instance donVitoCorleone, michaelCorleone, colonelWalterEKurtz, benjaminLWillard, ltVincentHanna;
    private static Instance neilMcCauley, bradleyFine, nancyBArtingstall, winifred, sarah, harry;
    private static Instance cluster0, cluster1;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    private MovieGraphFactory(){
        throw new UnsupportedOperationException();
    }

    public static void loadGraph(GraknGraph graknGraph) {
        MovieGraphFactory.graknGraph = graknGraph;
        buildGraph();
        try {
            MovieGraphFactory.graknGraph.commit();
        } catch (GraknValidationException e) {
            throw new RuntimeException(ErrorMessage.CANNOT_LOAD_EXAMPLE.getMessage(), e);
        }
    }

    private static void buildGraph() {
        buildOntology();
        try {
            buildInstances();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {

        productionBeingDirected = graknGraph.putRoleType("production-being-directed");
        director = graknGraph.putRoleType("director");
        directedBy = graknGraph.putRelationType("directed-by")
                .hasRole(productionBeingDirected).hasRole(director);

        productionWithCast = graknGraph.putRoleType("production-with-cast");
        actor = graknGraph.putRoleType("actor");
        characterBeingPlayed = graknGraph.putRoleType("character-being-played");
        hasCast = graknGraph.putRelationType("has-cast")
                .hasRole(productionWithCast).hasRole(actor).hasRole(characterBeingPlayed);

        genreOfProduction = graknGraph.putRoleType("genre-of-production");
        productionWithGenre = graknGraph.putRoleType("production-with-genre");
        hasGenre = graknGraph.putRelationType("has-genre")
                .hasRole(genreOfProduction).hasRole(productionWithGenre);

        clusterOfProduction = graknGraph.putRoleType("cluster-of-production");
        productionWithCluster = graknGraph.putRoleType("production-with-cluster");
        hasCluster = graknGraph.putRelationType("has-cluster")
                .hasRole(clusterOfProduction).hasRole(productionWithCluster);

        title = graknGraph.putResourceType("title", ResourceType.DataType.STRING);
        tmdbVoteCount = graknGraph.putResourceType("tmdb-vote-count", ResourceType.DataType.LONG);
        tmdbVoteAverage = graknGraph.putResourceType("tmdb-vote-average", ResourceType.DataType.DOUBLE);
        releaseDate = graknGraph.putResourceType("release-date", ResourceType.DataType.LONG);
        runtime = graknGraph.putResourceType("runtime", ResourceType.DataType.LONG);
        gender = graknGraph.putResourceType("gender", ResourceType.DataType.STRING).setRegex("(fe)?male");
        realName = graknGraph.putResourceType("real-name", ResourceType.DataType.STRING);
        name = graknGraph.putResourceType("name", ResourceType.DataType.STRING);

        EntityType production = graknGraph.putEntityType("production")
                .playsRole(productionWithCluster).playsRole(productionBeingDirected).playsRole(productionWithCast)
                .playsRole(productionWithGenre);

        hasResource(production, title);
        hasResource(production, tmdbVoteCount);
        hasResource(production, tmdbVoteAverage);
        hasResource(production, releaseDate);
        hasResource(production, runtime);

        movie = graknGraph.putEntityType("movie").superType(production);

        graknGraph.putEntityType("tv-show").superType(production);

        person = graknGraph.putEntityType("person")
                .playsRole(director).playsRole(actor).playsRole(characterBeingPlayed);

        hasResource(person, gender);
        hasResource(person, name);
        hasResource(person, realName);

        genre = graknGraph.putEntityType("genre").playsRole(genreOfProduction);

        hasResource(genre, name);

        character = graknGraph.putEntityType("character")
                .playsRole(characterBeingPlayed);

        hasResource(character, name);

        graknGraph.putEntityType("award");
        language = graknGraph.putEntityType("language");

        hasResource(language, name);

        cluster = graknGraph.putEntityType("cluster").playsRole(clusterOfProduction);
    }

    private static void buildInstances() throws ParseException {
        godfather = putEntity(movie, "Godfather");
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, DATE_FORMAT.parse("Sun Jan 01 00:00:00 GMT 1984").getTime());

        theMuppets = putEntity(movie, "The Muppets");
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, DATE_FORMAT.parse("Sat Feb 02 00:00:00 GMT 1985").getTime());

        apocalypseNow = putEntity(movie, "Apocalypse Now");
        putResource(apocalypseNow, title, "Apocalypse Now");
        putResource(apocalypseNow, tmdbVoteCount, 400L);
        putResource(apocalypseNow, tmdbVoteAverage, 8.4);

        heat = putEntity(movie, "Heat");
        putResource(heat, title, "Heat");

        hocusPocus = putEntity(movie, "Hocus Pocus");
        putResource(hocusPocus, title, "Hocus Pocus");
        putResource(hocusPocus, tmdbVoteCount, 435L);

        spy = putEntity(movie, "Spy");
        putResource(spy, title, "Spy");
        putResource(spy, releaseDate, DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime());

        chineseCoffee = putEntity(movie, "Chinese Coffee");
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, DATE_FORMAT.parse("Sat Sep 02 00:00:00 GMT 2000").getTime());

        marlonBrando = putEntity(person, "Marlon Brando");
        putResource(marlonBrando, name, "Marlon Brando");
        alPacino = putEntity(person, "Al Pacino");
        putResource(alPacino, name, "Al Pacino");
        missPiggy = putEntity(person, "Miss Piggy");
        putResource(missPiggy, name, "Miss Piggy");
        kermitTheFrog = putEntity(person, "Kermit The Frog");
        putResource(kermitTheFrog, name, "Kermit The Frog");
        martinSheen = putEntity(person, "Martin Sheen");
        putResource(martinSheen, name, "Martin Sheen");
        robertDeNiro = putEntity(person, "Robert de Niro");
        putResource(robertDeNiro, name, "Robert de Niro");
        judeLaw = putEntity(person, "Jude Law");
        putResource(judeLaw, name, "Jude Law");
        mirandaHeart = putEntity(person, "Miranda Heart");
        putResource(mirandaHeart, name, "Miranda Heart");
        betteMidler = putEntity(person, "Bette Midler");
        putResource(betteMidler, name, "Bette Midler");
        sarahJessicaParker = putEntity(person, "Sarah Jessica Parker");
        putResource(sarahJessicaParker, name, "Sarah Jessica Parker");

        crime = putEntity(genre, "crime");
        putResource(crime, name, "crime");
        drama = putEntity(genre, "drama");
        putResource(drama, name, "drama");
        war = putEntity(genre, "war");
        putResource(war, name, "war");
        action = putEntity(genre, "action");
        putResource(action, name, "action");
        comedy = putEntity(genre, "comedy");
        putResource(comedy, name, "comedy");
        family = putEntity(genre, "family");
        putResource(family, name, "family");
        musical = putEntity(genre, "musical");
        putResource(musical, name, "musical");
        fantasy = putEntity(genre, "fantasy");
        putResource(fantasy, name, "fantasy");

        donVitoCorleone = putEntity(character, "Don Vito Corleone");
        putResource(donVitoCorleone, name, "Don Vito Corleone");
        michaelCorleone = putEntity(character, "Michael Corleone");
        putResource(michaelCorleone, name, "Michael Corleone");
        colonelWalterEKurtz = putEntity(character, "Colonel Walter E. Kurtz");
        putResource(colonelWalterEKurtz, name, "Colonel Walter E. Kurtz");
        benjaminLWillard = putEntity(character, "Benjamin L. Willard");
        putResource(benjaminLWillard, name, "Benjamin L. Willard");
        ltVincentHanna = putEntity(character, "Lt Vincent Hanna");
        putResource(ltVincentHanna, name, "Lt Vincent Hanna");
        neilMcCauley = putEntity(character, "Neil McCauley");
        putResource(neilMcCauley, name, "Neil McCauley");
        bradleyFine = putEntity(character, "Bradley Fine");
        putResource(bradleyFine, name, "Bradley Fine");
        nancyBArtingstall = putEntity(character, "Nancy B Artingstall");
        putResource(nancyBArtingstall, name, "Nancy B Artingstall");
        winifred = putEntity(character, "Winifred");
        putResource(winifred, name, "Winifred");
        sarah = putEntity(character, "Sarah");
        putResource(sarah, name, "Sarah");
        harry = putEntity(character, "Harry");
        putResource(harry, name, "Harry");

        cluster0 = putEntity(cluster, "0");
        cluster1 = putEntity(cluster, "1");
    }

    private static void buildRelations() {
        graknGraph.addRelation(directedBy)
                .putRolePlayer(productionBeingDirected, chineseCoffee)
                .putRolePlayer(director, alPacino);

        hasCast(godfather, marlonBrando, donVitoCorleone);
        hasCast(godfather, alPacino, michaelCorleone);
        hasCast(theMuppets, missPiggy, missPiggy);
        hasCast(theMuppets, kermitTheFrog, kermitTheFrog);
        hasCast(apocalypseNow, marlonBrando, colonelWalterEKurtz);
        hasCast(apocalypseNow, martinSheen, benjaminLWillard);
        hasCast(heat, alPacino, ltVincentHanna);
        hasCast(heat, robertDeNiro, neilMcCauley);
        hasCast(spy, judeLaw, bradleyFine);
        hasCast(spy, mirandaHeart, nancyBArtingstall);
        hasCast(hocusPocus, betteMidler, winifred);
        hasCast(hocusPocus, sarahJessicaParker, sarah);
        hasCast(chineseCoffee, alPacino, harry);

        hasGenre(godfather, crime);
        hasGenre(godfather, drama);
        hasGenre(apocalypseNow, drama);
        hasGenre(apocalypseNow, war);
        hasGenre(heat, crime);
        hasGenre(heat, drama);
        hasGenre(heat, action);
        hasGenre(theMuppets, comedy);
        hasGenre(theMuppets, family);
        hasGenre(theMuppets, musical);
        hasGenre(hocusPocus, comedy);
        hasGenre(hocusPocus, family);
        hasGenre(hocusPocus, fantasy);
        hasGenre(spy, comedy);
        hasGenre(spy, family);
        hasGenre(spy, musical);
        hasGenre(chineseCoffee, drama);

        hasCluster(godfather, cluster0);
        hasCluster(apocalypseNow, cluster0);
        hasCluster(heat, cluster0);
        hasCluster(theMuppets, cluster1);
        hasCluster(hocusPocus, cluster1);
    }

    private static void buildRules() {
        // These rules are totally made up for testing purposes and don't work!
        RuleType aRuleType = graknGraph.putRuleType("a-rule-type");

        graknGraph.putRule("expectation-rule", "$x id 'expect-lhs';", "$x id 'expect-rhs';", aRuleType)
                .setExpectation(true)
                .addConclusion(movie).addHypothesis(person);

        graknGraph.putRule("materialize-rule", "$x id 'materialize-lhs';", "$x id 'materialize-rhs';", aRuleType)
                .setMaterialise(true)
                .addConclusion(person).addConclusion(genre).addHypothesis(hasCast);
    }

    private static Entity putEntity(EntityType type, String name) {
        return graknGraph.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type);
    }

    private static void hasResource(EntityType type, ResourceType<?> resourceType) {
        RoleType owner = graknGraph.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = graknGraph.putRoleType("has-" + resourceType.getId() + "-value");
        graknGraph.putRelationType("has-" + resourceType.getId()).hasRole(owner).hasRole(value);

        type.playsRole(owner);
        resourceType.playsRole(value);
    }

    private static <D> void putResource(Instance instance, ResourceType<D> resourceType, D resource) {
        Resource resourceInstance = graknGraph.putResource(resource, resourceType);

        RoleType owner = graknGraph.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = graknGraph.putRoleType("has-" + resourceType.getId() + "-value");
        RelationType relationType = graknGraph.putRelationType("has-" + resourceType.getId())
                .hasRole(owner).hasRole(value);

        graknGraph.addRelation(relationType)
                .putRolePlayer(owner, instance)
                .putRolePlayer(value, resourceInstance);
    }

    private static void hasCast(Instance movie, Instance person, Instance character) {
        graknGraph.addRelation(hasCast)
                .putRolePlayer(productionWithCast, movie)
                .putRolePlayer(actor, person)
                .putRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Instance movie, Instance genre) {
        graknGraph.addRelation(hasGenre)
                .putRolePlayer(productionWithGenre, movie)
                .putRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Instance movie, Instance cluster) {
        graknGraph.addRelation(hasCluster)
                .putRolePlayer(productionWithCluster, movie)
                .putRolePlayer(clusterOfProduction, cluster);
    }
}
