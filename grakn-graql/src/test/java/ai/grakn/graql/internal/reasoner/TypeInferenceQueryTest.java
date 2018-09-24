/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraqlTestUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeInferenceQueryTest {

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("typeInferenceTest.gql");

    @Test
    public void testTypeInference_singleGuard() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //parent of all roles so all relations possible
        String patternString = "{$x isa noRoleEntity; ($x, $y);}";
        String subbedPatternString = "{$x id '" + conceptId(graph, "noRoleEntity") + "';($x, $y);}";

        //SRE -> rel2
        //sub(SRE)=TRE -> rel3
        String patternString2 = "{$x isa singleRoleEntity; ($x, $y);}";
        String subbedPatternString2 = "{$x id '" + conceptId(graph, "singleRoleEntity") + "';($x, $y);}";

        //TRE -> rel3
        String patternString3 = "{$x isa twoRoleEntity; ($x, $y);}";
        String subbedPatternString3 = "{$x id '" + conceptId(graph, "twoRoleEntity") + "';($x, $y);}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );

        typeInference(allRelations(graph), patternString, subbedPatternString, graph);
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        typeInference(possibleTypes, patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_doubleGuard() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString = "{$x isa singleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{($x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString2 = "{$x isa twoRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{($x, $y);" +
                "$x id '" + conceptId(graph, "twoRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        //{rel1} ^ {rel1, rel2, rel3} = {rel1}
        String patternString3 = "{$x isa yetAnotherSingleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString3 = "{($x, $y);" +
                "$x id '" + conceptId(graph, "yetAnotherSingleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );

        List<RelationshipType> possibleTypes2 = Collections.singletonList(graph.getSchemaConcept(Label.of("twoRoleBinary")));

        typeInference(possibleTypes, patternString, subbedPatternString, graph);
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        typeInference(possibleTypes2, patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole() {
        EmbeddedGraknTx<?> graph = testContext.tx();
        String patternString = "{(role1: $x, $y);}";
        String patternString2 = "{(role2: $x, $y);}";
        String patternString3 = "{(role3: $x, $y);}";

        List<RelationshipType> possibleTypes = Collections.singletonList(graph.getSchemaConcept(Label.of("twoRoleBinary")));
        List<RelationshipType> possibleTypes2 = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );

        typeInference(possibleTypes, patternString, graph);
        typeInference(allRelations(graph), patternString2, graph);
        typeInference(possibleTypes2, patternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        EmbeddedGraknTx<?> graph = testContext.tx();
        String patternString = "{(subRole2: $x, $y);}";
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString = "{(role2: $x, $y); $y isa singleRoleEntity;}";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$y id '" + conceptId(graph, "singleRoleEntity") + "';}";
        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString2 = "{(role2: $x, $y); $y isa twoRoleEntity;}";
        String subbedPatternString2 = "{(role2: $x, $y);" +
                "$y id '" + conceptId(graph, "twoRoleEntity") + "';}";
        //{rel1} ^ {rel1, rel2, rel3}
        String patternString3 = "{(role1: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString3 = "{(role1: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") + "';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );

        typeInference(possibleTypes, patternString, subbedPatternString, graph);
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("twoRoleBinary"))), patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothConceptsAreSubConcepts() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //{rel3} ^ {rel2, rel3}
        String patternString = "{(subRole2: $x, $y); $y isa twoRoleEntity;}";
        String subbedPatternString = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(graph, "twoRoleEntity") + "';}";
        //{rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{(subRole2: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") + "';}";

        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, subbedPatternString, graph);
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("threeRoleBinary"))), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_typeContradiction() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //{rel1} ^ {rel2}
        String patternString = "{(role1: $x, $y); $y isa singleRoleEntity;}";
        String subbedPatternString = "{(role1: $x, $y);" +
                "$y id '" + conceptId(graph, "singleRoleEntity") + "';}";
        String patternString2 = "{(role1: $x, $y); $x isa singleRoleEntity;}";
        String subbedPatternString2 = "{(role1: $x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';}";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, graph);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        EmbeddedGraknTx<?> graph = testContext.tx();
        //{rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa singleRoleEntity;(role2: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );
        typeInference(possibleTypes, patternString, subbedPatternString, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //{rel1, rel2, rel3} ^ {rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa threeRoleEntity;(subRole2: $x, role3: $y); $y isa threeRoleEntity;}";
        String subbedPatternString = "{(subRole2: $x, role3: $y);" +
                "$x id '" + conceptId(graph, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "threeRoleEntity") + "';}";

        //{rel1, rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{$x isa threeRoleEntity;(role2: $x, role3: $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{(role2: $x, role3: $y);" +
                "$x id '" + conceptId(graph, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, subbedPatternString, graph);

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        EmbeddedGraknTx<?> graph = testContext.tx();

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{$x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherSingleRoleEntity;}";
        String subbedPatternString2 = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherSingleRoleEntity") +"';}";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, graph);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_metaGuards() {
        EmbeddedGraknTx<?> graph = testContext.tx();
        String patternString = "{($x, $y);$x isa entity; $y isa entity;}";
        typeInference(allRelations(graph), patternString, graph);
    }

    @Test
    public void testTypeInference_genericRelation() {
        EmbeddedGraknTx<?> graph = testContext.tx();
        String patternString = "{($x, $y);}";
        typeInference(allRelations(graph), patternString, graph);
    }

    private <T extends Atomic> T getAtom(ReasonerQuery q, Class<T> type, Set<Var> vars){
        return q.getAtoms(type)
                .filter(at -> at.getVarNames().containsAll(vars))
                .findFirst().get();
    }

    @Test
    public void testTypeInference_conjunctiveQuery() {
        EmbeddedGraknTx<?> graph = testContext.tx();
        String patternString = "{" +
                "($x, $y); $x isa anotherSingleRoleEntity;" +
                "($y, $z); $y isa anotherTwoRoleEntity;" +
                "($z, $w); $w isa threeRoleEntity;" +
                "}";

        ReasonerQueryImpl conjQuery = ReasonerQueries.create(conjunction(patternString, graph), graph);

        //determination of possible rel types for ($y, $z) relation depends on its neighbours which should be preserved
        //when resolving (and separating atoms) the query
        RelationshipAtom XYatom = getAtom(conjQuery, RelationshipAtom.class, Sets.newHashSet(var("x"), var("y")));
        RelationshipAtom YZatom = getAtom(conjQuery, RelationshipAtom.class, Sets.newHashSet(var("y"), var("z")));
        RelationshipAtom ZWatom = getAtom(conjQuery, RelationshipAtom.class, Sets.newHashSet(var("z"), var("w")));
        RelationshipAtom midAtom = (RelationshipAtom) ReasonerQueries.atomic(YZatom).getAtom();

        assertEquals(midAtom.getPossibleTypes(), YZatom.getPossibleTypes());

        //differently prioritised options arise from using neighbour information
        List<RelationshipType> firstTypeOption = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("twoRoleBinary")),
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );
        List<RelationshipType> secondTypeOption = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("anotherTwoRoleBinary")),
                graph.getSchemaConcept(Label.of("twoRoleBinary")),
                graph.getSchemaConcept(Label.of("threeRoleBinary"))
        );
        typeInference(secondTypeOption, XYatom.getCombinedPattern().toString(), graph);
        typeInference(firstTypeOption, YZatom.getCombinedPattern().toString(), graph);
        typeInference(firstTypeOption, ZWatom.getCombinedPattern().toString(), graph);
    }

    private void typeInference(List<RelationshipType> possibleTypes, String pattern, EmbeddedGraknTx<?> graph){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, graph), graph);
        Atom atom = query.getAtom();
        List<SchemaConcept> relationshipTypes = atom.getPossibleTypes();

        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationshipTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            assertTrue(CollectionUtils.isEqualCollection(possibleTypes, relationshipTypes));
            assertEquals(atom.getSchemaConcept(), null);
        }

        typeInferenceQueries(possibleTypes, pattern, graph);
    }

    private void typeInference(List<RelationshipType> possibleTypes, String pattern, String subbedPattern, EmbeddedGraknTx<?> graph){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, graph), graph);
        ReasonerAtomicQuery subbedQuery = ReasonerQueries.atomic(conjunction(subbedPattern, graph), graph);
        Atom atom = query.getAtom();
        Atom subbedAtom = subbedQuery.getAtom();

        List<SchemaConcept> relationshipTypes = atom.getPossibleTypes();
        List<SchemaConcept> subbedRelationshipTypes = subbedAtom.getPossibleTypes();
        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationshipTypes);
            assertEquals(relationshipTypes, subbedRelationshipTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
            assertEquals(subbedAtom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            assertTrue(CollectionUtils.isEqualCollection(possibleTypes, relationshipTypes));
            assertTrue(CollectionUtils.isEqualCollection(relationshipTypes, subbedRelationshipTypes));
            assertEquals(atom.getSchemaConcept(), null);
            assertEquals(subbedAtom.getSchemaConcept(), null);
        }

        typeInferenceQueries(possibleTypes, pattern, graph);
        typeInferenceQueries(possibleTypes, subbedPattern, graph);
    }

    private void typeInferenceQueries(List<RelationshipType> possibleTypes, String pattern, EmbeddedGraknTx<?> graph) {
        QueryBuilder qb = graph.graql();
        List<ConceptMap> typedAnswers = typedAnswers(possibleTypes, pattern, graph);
        List<ConceptMap> unTypedAnswers = qb.match(qb.parser().parsePattern(pattern)).get().execute();
        assertEquals(typedAnswers.size(), unTypedAnswers.size());
        GraqlTestUtil.assertCollectionsEqual(typedAnswers, unTypedAnswers);
    }

    private List<ConceptMap> typedAnswers(List<RelationshipType> possibleTypes, String pattern, EmbeddedGraknTx<?> graph){
        List<ConceptMap> answers = new ArrayList<>();
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, graph), graph);
        for(Type type : possibleTypes){
            GetQuery typedQuery = graph.graql().match(ReasonerQueries.atomic(query.getAtom().addType(type)).getPattern()).get();
            typedQuery.stream().filter(ans -> !answers.contains(ans)).forEach(answers::add);
        }
        return answers;
    }

    private List<RelationshipType> allRelations(EmbeddedGraknTx<?> tx){
        RelationshipType metaType = tx.getRelationshipType(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue());
        return metaType.subs().filter(t -> !t.equals(metaType)).collect(Collectors.toList());
    }

    private ConceptId conceptId(EmbeddedGraknTx<?> graph, String type){
        return graph.getEntityType(type).instances().map(Concept::id).findFirst().orElse(null);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, EmbeddedGraknTx<?> graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
