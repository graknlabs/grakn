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
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.reasoner.graphs.CWGraph;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import javafx.util.Pair;
import org.junit.Test;

import java.util.*;

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;

public class AtomicTest {

    @Test
    public void testRoleInference(){
        MindmapsGraph graph = CWGraph.getGraph();

        String queryString = "match isa owns, ($z, $y); $z isa country; $y isa weapon; select $y, $z";
        Query query = new Query(queryString, graph);
        Atomic atom = query.getAtomsWithType(graph.getType("owns")).iterator().next();

        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        queryString = "match isa owns, ($z, $y); $z isa country; select $y, $z";
        query = new Query(queryString, graph);
        atom = query.getAtomsWithType(graph.getType("owns")).iterator().next();

        Map<RoleType, Pair<String, Type>> roleMap2 = atom.getRoleVarTypeMap();

        assert(roleMap.size() == 2 && roleMap2.size() == 2);
    }

    @Test
    public void testRoleInference2(){
        MindmapsGraph graph = CWGraph.getGraph();

        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z";
        Query query = new Query(queryString, graph);
        Atomic atom = query.getAtomsWithType(graph.getType("transaction")).iterator().next();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        queryString = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa weapon; select $x, $y, $z";
        query = new Query(queryString, graph);
        atom = query.getAtomsWithType(graph.getType("transaction")).iterator().next();
        Map<RoleType, Pair<String, Type>> roleMap2 = atom.getRoleVarTypeMap();

        assert(roleMap.size() == 3 && roleMap2.size() == 3);
    }

    @Test
    public void testRelationConstructor(){
        MindmapsGraph graph = GenericGraph.getGraph("geo-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);

        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";

        MatchQuery MQ = qb.parseMatch(queryString);
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
        MindmapsGraph graph = GenericGraph.getGraph("geo-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);

        String queryString = "match ($x, $y, $z) isa ternary-relation-test;";

        MatchQuery MQ = qb.parseMatch(queryString);
        Query query = new Query(MQ, graph);

        Atomic atom = query.selectAtoms().iterator().next();
        Map<RoleType, Pair<String, Type>> rmap = atom.getRoleVarTypeMap();

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
