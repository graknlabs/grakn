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

package ai.grakn.test.graql.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graphs.CWGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import java.util.stream.Collectors;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


public class AtomicTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext cwGraph = GraphContext.preLoad(CWGraph.get());

    @ClassRule
    public static final GraphContext genealogyOntology = GraphContext.preLoad("genealogy/ontology.gql");

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
        Reasoner.linkConceptTypes(snbGraph.graph());
        Reasoner.linkConceptTypes(cwGraph.graph());
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testRecursive(){
        GraknGraph graph = snbGraph.graph();
        String recRelString = "{($x, $y) isa resides;}";
        String nrecRelString = "{($x, $y) isa recommendation;}";
        ReasonerAtomicQuery recQuery = new ReasonerAtomicQuery(conjunction(recRelString, graph), graph);
        ReasonerAtomicQuery nrecQuery = new ReasonerAtomicQuery(conjunction(nrecRelString, graph), graph);
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testFactory(){
        GraknGraph graph = snbGraph.graph();
        String atomString = "{$x isa person;}";
        String relString = "{($x, $y) isa recommendation;}";
        String resString = "{$x has gender 'male';}";

        Atom atom = new ReasonerAtomicQuery(conjunction(atomString, graph), graph).getAtom();
        Atom relation = new ReasonerAtomicQuery(conjunction(relString, graph), graph).getAtom();
        Atom res = new ReasonerAtomicQuery(conjunction(resString, graph), graph).getAtom();

        assertTrue(atom.isType());
        assertTrue(relation.isRelation());
        assertTrue(res.isResource());
    }

    @Test
    public void testRoleInference(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y) isa owns; $z isa country; $y isa rocket;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap = atom.getRoleVarTypeMap();

        String patternString2 = "{isa owns, ($z, $y); $z isa country;}";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        Atom atom2 = query2.getAtom();

        Map<RoleType, Pair<VarName, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(2, roleMap.size());
        assertEquals(2, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference2(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y, $x), isa transaction;$z isa country;$x isa person;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap = atom.getRoleVarTypeMap();

        String patternString2 = "{($z, $y, seller: $x), isa transaction;$z isa country;$y isa rocket;}";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        Atom atom2 = query2.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(3, roleMap.size());
        assertEquals(3, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference3(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{(buyer: $y, seller: $y, transaction-item: $x), isa transaction;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        Map<RoleType, Pair<VarName, Type>> roleMap = query.getAtom().getRoleVarTypeMap();

        String patternString2 = "{(buyer: $y, seller: $y, $x), isa transaction;}";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        Map<RoleType, Pair<VarName, Type>> roleMap2 = query2.getAtom().getRoleVarTypeMap();

        String patternString3 = "{(buyer: $y, $y, transaction-item: $x), isa transaction;}";
        ReasonerAtomicQuery query3 = new ReasonerAtomicQuery(conjunction(patternString3, graph), graph);
        Map<RoleType, Pair<VarName, Type>> roleMap3 = query3.getAtom().getRoleVarTypeMap();

        assertEquals(3, roleMap.size());
        assertEquals(3, roleMap2.size());
        assertEquals(3, roleMap3.size());
        assertEquals(roleMap, roleMap2);
        assertEquals(roleMap2, roleMap3);
    }

    @Test
    public void testRoleInference4(){
        GraknGraph graph = genealogyOntology.graph();
        String relationString = "{($p, son: $gc) isa parentship;}";
        String fullRelationString = "{(parent: $p, son: $gc) isa parentship;}";
        String relationString2 = "{(father: $gp, $p) isa parentship;}";
        String fullRelationString2 = "{(father: $gp, child: $p) isa parentship;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        Relation correctFullRelation = (Relation) new ReasonerAtomicQuery(conjunction(fullRelationString, graph), graph).getAtom();
        Relation relation2 = (Relation) new ReasonerAtomicQuery(conjunction(relationString2, graph), graph).getAtom();
        Relation correctFullRelation2 = (Relation) new ReasonerAtomicQuery(conjunction(fullRelationString2, graph), graph).getAtom();

        assertTrue(relation.getRoleVarTypeMap().equals(correctFullRelation.getRoleVarTypeMap()));
        assertTrue(relation2.getRoleVarTypeMap().equals(correctFullRelation2.getRoleVarTypeMap()));
    }

    @Test
    public void testTypeInference(){
        String typeId = snbGraph.graph().getType(TypeName.of("recommendation")).getId().getValue();
        String patternString = "{($x, $y); $x isa person; $y isa product;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().getValue().equals(typeId));
    }

    @Test
    public void testTypeInference2(){
        String typeId = cwGraph.graph().getType(TypeName.of("transaction")).getId().getValue();
        String patternString = "{($z, $y, $x);$z isa country;$x isa rocket;$y isa person;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, cwGraph.graph()), cwGraph.graph());
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().getValue().equals(typeId));
    }

    @Test
    public void testUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String relation = "{(parent: $y, child: $x);}";
        String specialisedRelation = "{(father: $p, daughter: $c);}";
        String specialisedRelation2 = "{(daughter: $p, father: $c);}";

        Atomic atom = new ReasonerAtomicQuery(conjunction(relation, graph), graph).getAtom();
        Atomic specialisedAtom = new ReasonerAtomicQuery(conjunction(specialisedRelation, graph), graph).getAtom();
        Atomic specialisedAtom2 = new ReasonerAtomicQuery(conjunction(specialisedRelation2, graph), graph).getAtom();

        Map<VarName, VarName> unifiers = specialisedAtom.getUnifiers(atom);
        Map<VarName, VarName> unifiers2 = specialisedAtom2.getUnifiers(atom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(VarName.of("p"), VarName.of("y"));
        correctUnifiers.put(VarName.of("c"), VarName.of("x"));
        Map<VarName, VarName> correctUnifiers2 = new HashMap<>();
        correctUnifiers2.put(VarName.of("p"), VarName.of("x"));
        correctUnifiers2.put(VarName.of("c"), VarName.of("y"));
        assertTrue(unifiers.toString(), unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
        assertTrue(unifiers2.toString(), unifiers2.entrySet().containsAll(correctUnifiers2.entrySet()));
    }

    @Test
    public void testUnification2() {
        GraknGraph graph = genealogyOntology.graph();
        String childString = "{(wife: $5b7a70db-2256-4d03-8fa4-2621a354899e, husband: $0f93f968-873a-43fa-b42f-f674c224ac04) isa marriage;}";
        String parentString = "{(wife: $x) isa marriage;}";
        Atom childAtom = new ReasonerAtomicQuery(conjunction(childString, graph), graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(VarName.of("5b7a70db-2256-4d03-8fa4-2621a354899e"), VarName.of("x"));
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));

        Map<VarName, VarName> reverseUnifiers = parentAtom.getUnifiers(childAtom);
        Map<VarName, VarName> correctReverseUnifiers = new HashMap<>();
        correctReverseUnifiers.put(VarName.of("x"), VarName.of("5b7a70db-2256-4d03-8fa4-2621a354899e"));
        assertTrue(reverseUnifiers.entrySet().containsAll(correctReverseUnifiers.entrySet()));
    }

    @Test
    public void testRewriteAndUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String parentString = "{$r (wife: $x) isa marriage;}";
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();

        String childPatternString = "(wife: $x, husband: $y) isa marriage";
        InferenceRule testRule = new InferenceRule(graph.admin().getMetaRuleInference().addRule(
                graph.graql().parsePattern(childPatternString),
                graph.graql().parsePattern(childPatternString)),
                graph);
        testRule.unify(parentAtom);
        Atom headAtom = testRule.getHead().getAtom();
        Map<RoleType, VarName> roleMap = headAtom.getRoleVarTypeMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getKey()));
        assertTrue(roleMap.get(graph.getRoleType("wife")).equals(VarName.of("x")));
    }

    @Test
    public void testRewrite(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "{(father: $x1, daughter: $x2) isa parentship;}";
        String parentRelation = "{$r (father: $x, daughter: $y) isa parentship;}";
        ReasonerAtomicQuery childQuery = new ReasonerAtomicQuery(conjunction(childRelation, graph), graph);
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentRelation, graph), graph).getAtom();

        Pair<Atom, Map<VarName, VarName>> rewrite = childAtom.rewrite(parentAtom, childQuery);
        Atom rewrittenAtom = rewrite.getKey();
        Map<VarName, VarName> unifiers = rewrite.getValue();
        Set<VarName> unifiedVariables = Sets.newHashSet(VarName.of("x1"), VarName.of("x2"));
        assertTrue(rewrittenAtom.isUserDefinedName());
        assertTrue(unifiedVariables.containsAll(unifiers.keySet()));
    }

    @Test
    public void testIndirectRoleUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "{($r1: $x1, $r2: $x2) isa parentship;$r1 type-name 'father';$r2 type-name 'daughter';}";
        String parentRelation = "{($R1: $x, $R2: $y) isa parentship;$R1 type-name 'father';$R2 type-name 'daughter';}";
        Atom childAtom = new ReasonerAtomicQuery(conjunction(childRelation, graph), graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentRelation, graph), graph).getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(VarName.of("x1"), VarName.of("x"));
        correctUnifiers.put(VarName.of("x2"), VarName.of("y"));
        correctUnifiers.put(VarName.of("r1"), VarName.of("R1"));
        correctUnifiers.put(VarName.of("r2"), VarName.of("R2"));
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
    }

    @Test
    public void testIndirectRoleUnification2(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "{($r1: $x1, $r2: $x2);$r1 type-name 'father';$r2 type-name 'daughter';}";
        String parentRelation = "{($R1: $x, $R2: $y);$R1 type-name 'father';$R2 type-name 'daughter';}";

        Atom childAtom = new ReasonerAtomicQuery(conjunction(childRelation, graph), graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentRelation, graph), graph).getAtom();
        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(VarName.of("x1"), VarName.of("x"));
        correctUnifiers.put(VarName.of("x2"), VarName.of("y"));
        correctUnifiers.put(VarName.of("r1"), VarName.of("R1"));
        correctUnifiers.put(VarName.of("r2"), VarName.of("R2"));
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
    }

    @Test
    public void testMatchAllUnification(){
        GraknGraph graph = snbGraph.graph();
        String childString = "{($z, $b) isa recommendation;}";
        String parentString = "{($a, $x);}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(childString, graph), graph).getAtom();
        Relation parentRelation = (Relation) new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();
        Map<VarName, VarName> unifiers = relation.getUnifiers(parentRelation);
        relation.unify(unifiers);
        assertEquals(unifiers.size(), 2);
        Set<VarName> vars = relation.getVarNames();
        Set<VarName> correctVars = new HashSet<>();
        correctVars.add(VarName.of("a"));
        correctVars.add(VarName.of("x"));
        assertTrue(!vars.contains(VarName.of("")));
        assertTrue(vars.containsAll(correctVars));
    }

    @Test
    public void testMatchAllUnification2(){
        GraknGraph graph = snbGraph.graph();
        String parentString = "{$r($a, $x);}";
        Relation parent = (Relation) new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();
        PatternAdmin body = graph.graql().parsePattern("($z, $b) isa recommendation").admin();
        PatternAdmin head = graph.graql().parsePattern("($z, $b) isa recommendation").admin();
        InferenceRule rule = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);

        rule.unify(parent);
        Set<VarName> vars = rule.getHead().getAtom().getVarNames();
        Set<VarName> correctVars = new HashSet<>();
        correctVars.add(VarName.of("r"));
        correctVars.add(VarName.of("a"));
        correctVars.add(VarName.of("x"));
        assertTrue(!vars.contains(VarName.of("")));
        assertTrue(vars.toString(), vars.containsAll(correctVars));
    }

    @Test
    public void testValuePredicateComparison(){
        GraknGraph graph = snbGraph.graph();
        String valueString = "{$x value '0';}";
        String valueString2 = "{$x value != 0;}";
        Atomic atom = new ReasonerQueryImpl(conjunction(valueString, graph), graph).getAtoms().iterator().next();
        Atomic atom2 =new ReasonerQueryImpl(conjunction(valueString2, graph), graph).getAtoms().iterator().next();
        assertTrue(!atom.isEquivalent(atom2));
    }

    @Test
    public void testMultiPredResourceEquivalence(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x has age $a;$a value >23; $a value <27;}";
        String patternString2 = "{$p has age $a;$a value >23;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        assertTrue(!query.getAtom().isEquivalent(query2.getAtom()));
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}

