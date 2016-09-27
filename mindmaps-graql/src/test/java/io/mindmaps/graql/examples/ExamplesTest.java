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

package io.mindmaps.graql.examples;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ExamplesTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph(UUID.randomUUID().toString().replaceAll("-", "a"));
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testPhilosophers() {
        load(
                "insert person isa entity-type;",
                "insert id 'Socrates' isa person;",
                "insert id 'Plato' isa person;",
                "insert id 'Aristotle' isa person;",
                "insert id 'Alexander' isa person;"
        );

        assertEquals(4, qb.parseMatch("match $p isa person;").stream().count());

        load(
                "insert school isa entity-type;",
                "insert id 'Peripateticism' isa school;",
                "insert id 'Platonism' isa school;",
                "insert id 'Idealism' isa school;",
                "insert id 'Cynicism' isa school;"
        );

        assertEquals(1, qb.parseMatch("match $x id 'Cynicism';").stream().count());

        load(
                "insert practice isa relation-type;",
                "insert philosopher isa role-type;",
                "insert philosophy isa role-type;",
                "insert practice has-role philosopher, has-role philosophy;",
                "insert person plays-role philosopher;",
                "insert school plays-role philosophy;",
                "insert (philosopher: Socrates, philosophy: Platonism) isa practice;",
                "insert (philosopher: Plato, philosophy: Idealism) isa practice;",
                "insert (philosopher: Plato, philosophy: Platonism) isa practice;",
                "insert (philosopher: Aristotle, philosophy: Peripateticism) isa practice;"
        );

        assertEquals(
                2,
                qb.parseMatch("match (philosopher: $x, Platonism) isa practice;").stream().count()
        );

        load(
                "insert education isa relation-type;",
                "insert teacher isa role-type;",
                "insert student isa role-type;",
                "insert education has-role teacher, has-role student;",
                "insert person plays-role teacher, plays-role student;",
                "insert (teacher: Socrates, student: Plato) isa education;",
                "insert (teacher: Plato, student: Aristotle) isa education;",
                "insert (teacher: Aristotle, student: Alexander) isa education;"
        );

        load(
                "insert title isa resource-type, datatype string;",
                "insert epithet isa resource-type, datatype string;",
                "insert person has-resource title;",
                "insert person has-resource epithet;"
        );

        load(
                "insert Alexander has epithet 'The Great';",
                "insert Alexander has title 'Hegemon';",
                "insert Alexander has title 'King of Macedon';",
                "insert Alexander has title 'Shah of Persia';",
                "insert Alexander has title 'Pharaoh of Egypt';",
                "insert Alexander has title 'Lord of Asia';"
        );

        MatchQuery pharaoh = qb.parseMatch("match $x has title contains 'Pharaoh';");
        assertEquals("Alexander", pharaoh.iterator().next().get("x").getId());

        load(
                "insert knowledge isa relation-type;",
                "insert thinker isa role-type;",
                "insert thought isa role-type;",
                "insert knowledge has-role thinker, has-role thought;",
                "insert fact isa entity-type, plays-role thought;",
                "insert description isa resource-type, datatype string;",
                "insert fact has-resource description;",
                "insert person plays-role thinker;",
                "insert id 'sun-fact' isa fact, has description 'The Sun is bigger than the Earth';",
                "insert (thinker: Aristotle, thought: sun-fact) isa knowledge;",
                "insert id 'cave-fact' isa fact, has description 'Caves are mostly pretty dark';",
                "insert (thinker: Plato, thought: cave-fact) isa knowledge;",
                "insert id 'nothing' isa fact;",
                "insert (thinker: Socrates, thought: nothing) isa knowledge;"
        );

        load(
                "insert knowledge plays-role thought;",
                "match $socratesKnowsNothing (Socrates, nothing); " +
                "insert (thinker: Socrates, thought: $socratesKnowsNothing) isa knowledge;"
        );

        assertEquals(
                2,
                qb.parseMatch("match (Socrates, $x) isa knowledge;").stream().count()
        );
    }

    private void load(String... queries) {
        Stream.of(queries).map(qb::parseInsert).forEach(InsertQuery::execute);
    }
}
