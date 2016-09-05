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

package io.mindmaps.example;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.Resource;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.RuleType;
import io.mindmaps.core.model.Type;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * A class which loads sample data into a mindmaps graph
 */
public class MovieGraphFactory {
    private static MindmapsTransaction mindmapsTransaction;
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

    public static void loadGraph(MindmapsGraph mindmapsGraph) {
        mindmapsTransaction = mindmapsGraph.getTransaction();
        buildGraph();
        try {
            mindmapsTransaction.commit();
        } catch (MindmapsValidationException e) {
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

        productionBeingDirected = mindmapsTransaction.putRoleType("production-being-directed");
        director = mindmapsTransaction.putRoleType("director");
        directedBy = mindmapsTransaction.putRelationType("directed-by")
                .hasRole(productionBeingDirected).hasRole(director);

        productionWithCast = mindmapsTransaction.putRoleType("production-with-cast");
        actor = mindmapsTransaction.putRoleType("actor");
        characterBeingPlayed = mindmapsTransaction.putRoleType("character-being-played");
        hasCast = mindmapsTransaction.putRelationType("has-cast")
                .hasRole(productionWithCast).hasRole(actor).hasRole(characterBeingPlayed);

        genreOfProduction = mindmapsTransaction.putRoleType("genre-of-production");
        productionWithGenre = mindmapsTransaction.putRoleType("production-with-genre");
        hasGenre = mindmapsTransaction.putRelationType("has-genre")
                .hasRole(genreOfProduction).hasRole(productionWithGenre);

        clusterOfProduction = mindmapsTransaction.putRoleType("cluster-of-production");
        productionWithCluster = mindmapsTransaction.putRoleType("production-with-cluster");
        hasCluster = mindmapsTransaction.putRelationType("has-cluster")
                .hasRole(clusterOfProduction).hasRole(productionWithCluster);

        title = mindmapsTransaction.putResourceType("title", Data.STRING);
        tmdbVoteCount = mindmapsTransaction.putResourceType("tmdb-vote-count", Data.LONG);
        tmdbVoteAverage = mindmapsTransaction.putResourceType("tmdb-vote-average", Data.DOUBLE);
        releaseDate = mindmapsTransaction.putResourceType("release-date", Data.LONG);
        runtime = mindmapsTransaction.putResourceType("runtime", Data.LONG);
        gender = mindmapsTransaction.putResourceType("gender", Data.STRING);
        realName = mindmapsTransaction.putResourceType("real-name", Data.STRING);
        name = mindmapsTransaction.putResourceType("name", Data.STRING);

        EntityType production = mindmapsTransaction.putEntityType("production")
                .playsRole(productionWithCluster).playsRole(productionBeingDirected).playsRole(productionWithCast)
                .playsRole(productionWithGenre);

        hasResource(production, title);
        hasResource(production, tmdbVoteCount);
        hasResource(production, tmdbVoteAverage);
        hasResource(production, releaseDate);
        hasResource(production, runtime);

        movie = mindmapsTransaction.putEntityType("movie").superType(production);

        mindmapsTransaction.putEntityType("tv-show").superType(production);

        person = mindmapsTransaction.putEntityType("person")
                .playsRole(director).playsRole(actor).playsRole(characterBeingPlayed);

        hasResource(person, gender);
        hasResource(person, realName);

        genre = mindmapsTransaction.putEntityType("genre").playsRole(genreOfProduction);

        hasResource(genre, name);

        character = mindmapsTransaction.putEntityType("character")
                .playsRole(characterBeingPlayed);

        hasResource(character, name);

        mindmapsTransaction.putEntityType("award");
        language = mindmapsTransaction.putEntityType("language");

        hasResource(language, name);

        cluster = mindmapsTransaction.putEntityType("cluster").playsRole(clusterOfProduction);
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
        alPacino = putEntity(person, "Al Pacino");
        missPiggy = putEntity(person, "Miss Piggy");
        kermitTheFrog = putEntity(person, "Kermit The Frog");
        martinSheen = putEntity(person, "Martin Sheen");
        robertDeNiro = putEntity(person, "Robert de Niro");
        judeLaw = putEntity(person, "Jude Law");
        mirandaHeart = putEntity(person, "Miranda Heart");
        betteMidler = putEntity(person, "Bette Midler");
        sarahJessicaParker = putEntity(person, "Sarah Jessica Parker");

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
        mindmapsTransaction.addRelation(directedBy)
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
        RuleType aRuleType = mindmapsTransaction.putRuleType("a-rule-type");

        mindmapsTransaction.putRule("expectation-rule", "expect-lhs", "expect-rhs", aRuleType)
                .setExpectation(true)
                .addConclusion(movie).addHypothesis(person);

        mindmapsTransaction.putRule("materialize-rule", "materialize-lhs", "materialize-rhs", aRuleType)
                .setMaterialise(true)
                .addConclusion(person).addConclusion(genre).addHypothesis(hasCast);
    }

    private static Entity putEntity(EntityType type, String name) {
        return mindmapsTransaction.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type);
    }

    private static void hasResource(Type type, ResourceType<?> resourceType) {
        RoleType owner = mindmapsTransaction.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = mindmapsTransaction.putRoleType("has-" + resourceType.getId() + "-value");
        mindmapsTransaction.putRelationType("has-" + resourceType.getId()).hasRole(owner).hasRole(value);

        type.playsRole(owner);
        resourceType.playsRole(value);
    }

    private static <D> void putResource(Instance instance, ResourceType<D> resourceType, D resource) {
        Resource resourceInstance = mindmapsTransaction.putResource(resource, resourceType);

        RoleType owner = mindmapsTransaction.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = mindmapsTransaction.putRoleType("has-" + resourceType.getId() + "-value");
        RelationType relationType = mindmapsTransaction.putRelationType("has-" + resourceType.getId())
                .hasRole(owner).hasRole(value);

        mindmapsTransaction.addRelation(relationType)
                .putRolePlayer(owner, instance)
                .putRolePlayer(value, resourceInstance);
    }

    private static void hasCast(Instance movie, Instance person, Instance character) {
        mindmapsTransaction.addRelation(hasCast)
                .putRolePlayer(productionWithCast, movie)
                .putRolePlayer(actor, person)
                .putRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Instance movie, Instance genre) {
        mindmapsTransaction.addRelation(hasGenre)
                .putRolePlayer(productionWithGenre, movie)
                .putRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Instance movie, Instance cluster) {
        mindmapsTransaction.addRelation(hasCluster)
                .putRolePlayer(productionWithCluster, movie)
                .putRolePlayer(clusterOfProduction, cluster);
    }
}
