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
 *
 */

package ai.grakn.generator;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.pholser.junit.quickcheck.MinimalCounterexampleHook;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.StringUtil.valueToString;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Generator to create random {@link GraknTx}s.
 *
 * @author Felix Chapman
 */
@SuppressWarnings("unchecked") // We're performing random operations. Generics will not constrain us!
public class GraknTxs extends AbstractGenerator<GraknTx> implements MinimalCounterexampleHook {

    private static GraknTx lastGeneratedGraph;

    private static StringBuilder graphSummary;

    private GraknTx tx;
    private Boolean open = null;

    public GraknTxs() {
        super(GraknTx.class);
    }

    public static GraknTx lastGeneratedGraph() {
        return lastGeneratedGraph;
    }

    /**
     * Mutate the tx being generated by calling a random method on it.
     */
    private void mutateOnce() {
        boolean succesfulMutation = false;

        while (!succesfulMutation) {
            succesfulMutation = true;
            try {
                random.choose(mutators).run();
            } catch (UnsupportedOperationException | GraknTxOperationException | KBGeneratorException e) {
                // We only catch acceptable exceptions for the tx API to throw
                succesfulMutation = false;
            }
        }
    }

    @Override
    public GraknTx generate() {
        // TODO: Generate more keyspaces
        // We don't do this now because creating lots of keyspaces seems to slow the system tx
        String keyspace = gen().make(MetasyntacticStrings.class).generate(random, status);
        GraknSession factory = Grakn.session(Grakn.IN_MEMORY, keyspace);

        int size = status.size();

        startSummary();

        graphSummary.append("size: ").append(size).append("\n");

        closeGraph(lastGeneratedGraph);

        // Clear tx before retrieving
        tx = factory.open(GraknTxType.WRITE);
        tx.admin().delete();
        tx = factory.open(GraknTxType.WRITE);

        for (int i = 0; i < size; i++) {
            mutateOnce();
        }

        // Close graphs randomly, unless parameter is set
        boolean shouldOpen = open != null ? open : random.nextBoolean();

        if (!shouldOpen) tx.close();

        setLastGeneratedGraph(tx);
        return tx;
    }

    private static void setLastGeneratedGraph(GraknTx graph) {
        lastGeneratedGraph = graph;
    }

    private void closeGraph(GraknTx graph){
        if(graph != null && !graph.isClosed()){
            graph.close();
        }
    }

    public void configure(Open open) {
        setOpen(open.value());
    }

    public GraknTxs setOpen(boolean open) {
        this.open = open;
        return this;
    }

    // A list of methods that will mutate the tx in some random way when called
    private final ImmutableList<Runnable> mutators = ImmutableList.of(
            () -> {
                Label label = typeLabel();
                EntityType superType = entityType();
                EntityType entityType = tx.putEntityType(label).sup(superType);
                summaryAssign(entityType, "tx", "putEntityType", label);
                summary(entityType, "superType", superType);
            },
            () -> {
                Label label = typeLabel();
                AttributeType.DataType dataType = gen(AttributeType.DataType.class);
                AttributeType superType = resourceType();
                AttributeType attributeType = tx.putAttributeType(label, dataType).sup(superType);
                summaryAssign(attributeType, "tx", "putResourceType", label, dataType);
                summary(attributeType, "superType", superType);
            },
            () -> {
                Label label = typeLabel();
                Role superType = role();
                Role role = tx.putRole(label).sup(superType);
                summaryAssign(role, "tx", "putRole", label);
                summary(role, "superType", superType);
            },
            () -> {
                Label label = typeLabel();
                RelationshipType superType = relationType();
                RelationshipType relationshipType = tx.putRelationshipType(label).sup(superType);
                summaryAssign(relationshipType, "tx", "putRelationshipType", label);
                summary(relationshipType, "superType", superType);
            },
            () -> {
                Type type = type();
                Role role = role();
                type.plays(role);
                summary(type, "plays", role);
            },
            () -> {
                Type type = type();
                AttributeType attributeType = resourceType();
                type.attribute(attributeType);
                summary(type, "resource", attributeType);
            },
            () -> {
                Type type = type();
                AttributeType attributeType = resourceType();
                type.key(attributeType);
                summary(type, "key", attributeType);
            },
            () -> {
                Type type = type();
                boolean isAbstract = random.nextBoolean();
                type.setAbstract(isAbstract);
                summary(type, "setAbstract", isAbstract);
            },
            () -> {
                EntityType entityType1 = entityType();
                EntityType entityType2 = entityType();
                entityType1.sup(entityType2);
                summary(entityType1, "superType", entityType2);
            },
            () -> {
                EntityType entityType = entityType();
                Entity entity = entityType.addEntity();
                summaryAssign(entity, entityType, "addEntity");
            },
            () -> {
                Role role1 = role();
                Role role2 = role();
                role1.sup(role2);
                summary(role1, "superType", role2);
            },
            () -> {
                RelationshipType relationshipType1 = relationType();
                RelationshipType relationshipType2 = relationType();
                relationshipType1.sup(relationshipType2);
                summary(relationshipType1, "superType", relationshipType2);
            },
            () -> {
                RelationshipType relationshipType = relationType();
                Relationship relationship = relationshipType.addRelationship();
                summaryAssign(relationship, relationshipType, "addRelationship");
            },
            () -> {
                RelationshipType relationshipType = relationType();
                Role role = role();
                relationshipType.relates(role);
                summary(relationshipType, "relates", role);
            },
            () -> {
                AttributeType attributeType1 = resourceType();
                AttributeType attributeType2 = resourceType();
                attributeType1.sup(attributeType2);
                summary(attributeType1, "superType", attributeType2);
            },
            () -> {
                AttributeType attributeType = resourceType();
                Object value = gen().make(ResourceValues.class).generate(random, status);
                Attribute attribute = attributeType.putAttribute(value);
                summaryAssign(attribute, attributeType, "putResource", valueToString(value));
            },
            // () -> resourceType().setRegex(gen(String.class)), // TODO: Enable this when doesn't throw a NPE
            () -> {
                RuleType ruleType1 = ruleType();
                RuleType ruleType2 = ruleType();
                ruleType1.sup(ruleType2);
                summary(ruleType1, "superType", ruleType2);
            },
            //TODO: re-enable when grakn-kb can create graql constructs
            /*() -> {
                RuleType ruleType = ruleType();
                Rule rule = ruleType.putRule(graql.parsePattern("$x"), graql.parsePattern("$x"));// TODO: generate more complicated rules
                summaryAssign(rule, ruleType, "putRule", "var(\"x\")", "var(\"y\")");
            },*/
            () -> {
                Thing thing = instance();
                Attribute attribute = resource();
                thing.attribute(attribute);
                summary(thing, "resource", attribute);
            },
            () -> {
                Type type = type();
                Thing thing = instance();
                type.scope(thing);
                summary(type, "scope", thing);
            },
            () -> {
                Relationship relationship = relation();
                Role role = role();
                Thing thing = instance();
                relationship.addRolePlayer(role, thing);
                summary(relationship, "addRolePlayer", role, thing);
            }
    );

    private static void startSummary() {
        graphSummary = new StringBuilder();
    }

    private void summary(Object target, String methodName, Object... args) {
        graphSummary.append(summaryFormat(target)).append(".").append(methodName).append("(");
        graphSummary.append(Stream.of(args).map(this::summaryFormat).collect(joining(", ")));
        graphSummary.append(");\n");
    }

    private void summaryAssign(Object assign, Object target, String methodName, Object... args) {
        summary(summaryFormat(assign) + " = " + summaryFormat(target), methodName, args);
    }

    private String summaryFormat(Object object) {
        if (object instanceof SchemaConcept) {
            return ((SchemaConcept) object).getLabel().getValue().replaceAll("-", "_");
        } else if (object instanceof Thing) {
            Thing thing = (Thing) object;
            return summaryFormat(thing.type()) + thing.getId().getValue();
        } else if (object instanceof Label) {
            return valueToString(((Label) object).getValue());
        } else {
            return object.toString();
        }
    }

    private Label typeLabel() {
        return gen().make(Labels.class, gen().make(MetasyntacticStrings.class)).generate(random, status);
    }

    private Type type() {
        // TODO: Revise this when meta concept is a type
        Collection<? extends Type> candidates = tx.admin().getMetaConcept().subs().
                map(Concept::asType).collect(toSet());
        return random.choose(candidates);
    }

    private EntityType entityType() {
        return random.choose(tx.admin().getMetaEntityType().subs().collect(toSet()));
    }

    private Role role() {
        return random.choose(tx.admin().getMetaRole().subs().collect(toSet()));
    }

    private AttributeType resourceType() {
        return random.choose((Collection<AttributeType>) tx.admin().getMetaResourceType().subs().collect(toSet()));
    }

    private RelationshipType relationType() {
        return random.choose(tx.admin().getMetaRelationType().subs().collect(toSet()));
    }

    private RuleType ruleType() {
        return random.choose(tx.admin().getMetaRuleType().subs().collect(toSet()));
    }

    private Thing instance() {
        return chooseOrThrow(allInstancesFrom(tx));
    }

    private Relationship relation() {
        return chooseOrThrow(tx.admin().getMetaRelationType().instances());
    }

    private Attribute resource() {
        return chooseOrThrow((Stream<Attribute>) tx.admin().getMetaResourceType().instances());
    }

    //TODO: re-enable when grakn-kb can create graql constructs
//    private Rule rule() {
//        return chooseOrThrow(tx.admin().getMetaRuleType().instances());
//    }

    private <T> T chooseOrThrow(Stream<? extends T> stream) {
        Set<? extends  T> collection = stream.collect(toSet());
        if (collection.isEmpty()) {
            throw new KBGeneratorException();
        } else {
            return random.choose(collection);
        }
    }

    public static List<Concept> allConceptsFrom(GraknTx graph) {
        List<Concept> concepts = Lists.newArrayList(GraknTxs.allSchemaElementsFrom(graph));
        concepts.addAll(allInstancesFrom(graph).collect(toSet()));
        return concepts;
    }

    public static Collection<? extends SchemaConcept> allSchemaElementsFrom(GraknTx graph) {
        Set<SchemaConcept> allSchemaConcepts = new HashSet<>();
        allSchemaConcepts.addAll(graph.admin().getMetaConcept().subs().collect(toSet()));
        allSchemaConcepts.addAll(graph.admin().getMetaRole().subs().collect(toSet()));
        return allSchemaConcepts;
    }

    public static Stream<? extends Thing> allInstancesFrom(GraknTx graph) {
        // TODO: Revise this when meta concept is a type
        return graph.admin().getMetaConcept().subs().
                flatMap(element -> element.asType().instances());
    }

    @Override
    public void handle(Object[] counterexample, Runnable action) {
        System.err.println("Graph generated:\n" + graphSummary);
    }

    /**
     * Specify whether the generated tx should be open or closed
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Open {
        boolean value() default true;
    }

    private static class KBGeneratorException extends RuntimeException {

    }
}
