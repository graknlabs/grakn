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

package io.mindmaps.graql.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.*;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import javafx.util.Pair;
import org.junit.Test;

import java.util.*;

import static io.mindmaps.graql.internal.reasoner.Utility.*;

public class AtomicTest {

    @Test
    public void testValuePredicate(){
        MindmapsTransaction graph = SNBGraph.getTransaction();
        QueryParser qp = QueryParser.create(graph);
        String queryString = "match " +
                "$x1 isa person;\n" +
                "$x2 isa tag;\n" +
                "($x1, $x2) isa recommendation";

        MatchQueryDefault MQ = qp.parseMatchQuery(queryString).getMatchQuery();
        printMatchQueryResults(MQ);
    }

    @Test
    public void testRelationConstructor(){
        MindmapsTransaction graph = GenericGraph.getTransaction("geo-test.gql");
        QueryParser qp = QueryParser.create(graph);

        String queryString = "match (geo-entity $x, entity-location $y) isa is-located-in;";

        MatchQueryDefault MQ = qp.parseMatchQuery(queryString).getMatchQuery();
        Query query = new Query(MQ, graph);

        Atomic atom = query.selectAtoms().iterator().next();
        Set<String> vars = atom.getVarNames();

        String relTypeId = atom.getTypeId();
        RelationType relType = graph.getRelationType(relTypeId);
        Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

        Set<Map<String, String>> roleMaps = new HashSet<>();
        computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

        Collection<Relation> rels = new LinkedList<>();
        roleMaps.forEach( map -> rels.add(new Relation(relTypeId, map)));
    }

    @Test
    public void testRelationConstructor2(){
        MindmapsTransaction graph = GenericGraph.getTransaction("geo-test.gql");
        QueryParser qp = QueryParser.create(graph);

        String queryString = "match ($x, $y, $z) isa ternary-relation-test";

        MatchQueryDefault MQ = qp.parseMatchQuery(queryString).getMatchQuery();
        Query query = new Query(MQ, graph);

        Atomic atom = query.selectAtoms().iterator().next();
        Map<RoleType, Pair<String, Type>> rmap = ((Relation) atom).getRoleVarTypeMap();

        Set<String> vars = atom.getVarNames();

        String relTypeId = atom.getTypeId();
        RelationType relType = graph.getRelationType(relTypeId);
        Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

        Set<Map<String, String>> roleMaps = new HashSet<>();
        computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

        Collection<Relation> rels = new LinkedList<>();
        roleMaps.forEach( map -> rels.add(new Relation(relTypeId, map)));
    }





}
