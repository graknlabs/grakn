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
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraphRuntimeException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.pholser.junit.quickcheck.MinimalCounterexampleHook;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.util.StringConverter.valueToString;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.joining;

/**
 * Generator to create random {@link GraknGraph}s.
 */
@SuppressWarnings("unchecked") // We're performing random operations. Generics will not constrain us!
public class GraknGraphs extends AbstractGenerator<GraknGraph> implements MinimalCounterexampleHook {

    private static GraknGraph lastGeneratedGraph;

    private static StringBuilder graphSummary;

    private GraknGraph graph;
    private Boolean open = null;

    public GraknGraphs() {
        super(GraknGraph.class);
    }

    public static GraknGraph lastGeneratedGraph() {
        return lastGeneratedGraph;
    }

    /**
     * Mutate the graph being generated by calling a random method on it.
     */
    private void mutateOnce() {
        boolean succesfulMutation = false;

        while (!succesfulMutation) {
            succesfulMutation = true;
            try {
                random.choose(mutators).run();
            } catch (UnsupportedOperationException | GraphRuntimeException | GraphGeneratorException e) {
                // We only catch acceptable exceptions for the graph API to throw
                succesfulMutation = false;
            }
        }
    }

    @Override
    public GraknGraph generate() {
        // TODO: Generate more keyspaces
        // We don't do this now because creating lots of keyspaces seems to slow the system graph
        String keyspace = gen().make(MetasyntacticStrings.class).generate(random, status);
        GraknSession factory = Grakn.session(Grakn.IN_MEMORY, keyspace);

        int size = status.size();

        graphSummary = new StringBuilder();
        graphSummary.append("size: ").append(size).append("\n");

        closeGraph(lastGeneratedGraph);

        // Clear graph before retrieving
        graph = factory.open(GraknTxType.WRITE);
        graph.clear();
        graph = factory.open(GraknTxType.WRITE);

        for (int i = 0; i < size; i++) {
            mutateOnce();
        }

        // Close graphs randomly, unless parameter is set
        boolean shouldOpen = open != null ? open : random.nextBoolean();

        if (!shouldOpen) graph.close();

        lastGeneratedGraph = graph;
        return graph;
    }

    private void closeGraph(GraknGraph graph){
        if(graph != null && !graph.isClosed()){
            graph.close();
        }
    }

    public void configure(Open open) {
        setOpen(open.value());
    }

    public GraknGraphs setOpen(boolean open) {
        this.open = open;
        return this;
    }

    // A list of methods that will mutate the graph in some random way when called
    private final ImmutableList<Runnable> mutators = ImmutableList.of(
            () -> {
                TypeName typeName = typeName();
                EntityType superType = entityType();
                EntityType entityType = graph.putEntityType(typeName).superType(superType);
                summaryAssign(entityType, "graph", "putEntityType", typeName);
                summary(entityType, "superType", superType);
            },
            () -> {
                TypeName typeName = typeName();
                ResourceType.DataType dataType = gen(ResourceType.DataType.class);
                ResourceType superType = resourceType();
                ResourceType resourceType = graph.putResourceType(typeName, dataType).superType(superType);
                summaryAssign(resourceType, "graph", "putResourceType", typeName, dataType);
                summary(resourceType, "superType", superType);
            },
            () -> {
                TypeName typeName = typeName();
                RoleType superType = roleType();
                RoleType roleType = graph.putRoleType(typeName).superType(superType);
                summaryAssign(roleType, "graph", "putRoleType", typeName);
                summary(roleType, "superType", superType);
            },
            () -> {
                TypeName typeName = typeName();
                RelationType superType = relationType();
                RelationType relationType = graph.putRelationType(typeName).superType(superType);
                summaryAssign(relationType, "graph", "putRelationType", typeName);
                summary(relationType, "superType", superType);
            },
            () -> {
                boolean flag = gen(Boolean.class);
                graph.showImplicitConcepts(flag);
                summary("graph", "showImplicitConcepts", flag);
            },
            () -> {
                Type type = type();
                RoleType roleType = roleType();
                type.plays(roleType);
                summary(type, "plays", roleType);
            },
            () -> {
                Type type = type();
                ResourceType resourceType = resourceType();
                type.resource(resourceType);
                summary(type, "resource", resourceType);
            },
            () -> {
                Type type = type();
                ResourceType resourceType = resourceType();
                type.key(resourceType);
                summary(type, "key", resourceType);
            },
            () -> {
                Type type = type();
                type.setAbstract(true);
                summary(type, "setAbstract", true);
            },
            () -> {
                EntityType entityType1 = entityType();
                EntityType entityType2 = entityType();
                entityType1.superType(entityType2);
                summary(entityType1, "superType", entityType2);
            },
            () -> {
                EntityType entityType = entityType();
                Entity entity = entityType.addEntity();
                summaryAssign(entity, entityType, "addEntity");
            },
            () -> {
                RoleType roleType1 = roleType();
                RoleType roleType2 = roleType();
                roleType1.superType(roleType2);
                summary(roleType1, "superType", roleType2);
            },
            () -> {
                RelationType relationType1 = relationType();
                RelationType relationType2 = relationType();
                relationType1.superType(relationType2);
                summary(relationType1, "superType", relationType2);
            },
            () -> {
                RelationType relationType = relationType();
                Relation relation = relationType.addRelation();
                summaryAssign(relation, relationType, "addRelation");
            },
            () -> {
                RelationType relationType = relationType();
                RoleType roleType = roleType();
                relationType.relates(roleType);
                summary(relationType, "relates", roleType);
            },
            () -> {
                ResourceType resourceType1 = resourceType();
                ResourceType resourceType2 = resourceType();
                resourceType1.superType(resourceType2);
                summary(resourceType1, "superType", resourceType2);
            },
            () -> {
                ResourceType resourceType = resourceType();
                Object value = gen().make(ResourceValues.class).generate(random, status);
                Resource resource = resourceType.putResource(value);
                summaryAssign(resource, resourceType, "putResource", valueToString(value));
            },
            // () -> resourceType().setRegex(gen(String.class)), // TODO: Enable this when doesn't throw a NPE
            () -> {
                RuleType ruleType1 = ruleType();
                RuleType ruleType2 = ruleType();
                ruleType1.superType(ruleType2);
                summary(ruleType1, "superType", ruleType2);
            },
            () -> {
                RuleType ruleType = ruleType();
                Rule rule = ruleType.putRule(var("x"), var("x"));// TODO: generate more complicated rules
                summaryAssign(rule, ruleType, "putRule", "var(\"x\")", "var(\"y\")");
            },
            () -> {
                Instance instance = instance();
                Resource resource = resource();
                instance.resource(resource);
                summary(instance, "resource", resource);
            },
            () -> {
                Type type = type();
                Instance instance = instance();
                type.scope(instance);
                summary(type, "scope", instance);
            },
            () -> {
                Relation relation = relation();
                RoleType roleType = roleType();
                Instance instance = instance();
                relation.addRolePlayer(roleType, instance);
                summary(relation, "addRolePlayer", roleType, instance);
            }
    );

    private void summary(Object target, String methodName, Object... args) {
        graphSummary.append(summaryFormat(target)).append(".").append(methodName).append("(");
        graphSummary.append(Stream.of(args).map(this::summaryFormat).collect(joining(", ")));
        graphSummary.append(");\n");
    }

    private void summaryAssign(Object assign, Object target, String methodName, Object... args) {
        summary(summaryFormat(assign) + " = " + summaryFormat(target), methodName, args);
    }

    private String summaryFormat(Object object) {
        if (object instanceof Type) {
            return ((Type) object).getName().getValue().replaceAll("-", "_");
        } else if (object instanceof Instance) {
            Instance instance = (Instance) object;
            return summaryFormat(instance.type()) + instance.getId().getValue();
        } else if (object instanceof TypeName) {
            return valueToString(((TypeName) object).getValue());
        } else {
            return object.toString();
        }
    }

    private TypeName typeName() {
        return gen().make(TypeNames.class, gen().make(MetasyntacticStrings.class)).generate(random, status);
    }

    private Type type() {
        return random.choose(graph.admin().getMetaConcept().subTypes());
    }

    private EntityType entityType() {
        return random.choose(graph.admin().getMetaEntityType().subTypes());
    }

    private RoleType roleType() {
        return random.choose(graph.admin().getMetaRoleType().subTypes());
    }

    private ResourceType resourceType() {
        return random.choose((Collection<ResourceType>) graph.admin().getMetaResourceType().subTypes());
    }

    private RelationType relationType() {
        return random.choose(graph.admin().getMetaRelationType().subTypes());
    }

    private RuleType ruleType() {
        return random.choose(graph.admin().getMetaRuleType().subTypes());
    }

    private Instance instance() {
        return chooseOrThrow(graph.admin().getMetaConcept().instances());
    }

    private Relation relation() {
        return chooseOrThrow(graph.admin().getMetaRelationType().instances());
    }

    private Resource resource() {
        return chooseOrThrow((Collection<Resource>) graph.admin().getMetaResourceType().instances());
    }

    private Rule rule() {
        return chooseOrThrow(graph.admin().getMetaRuleType().instances());
    }

    private <T> T chooseOrThrow(Collection<? extends T> collection) {
        if (collection.isEmpty()) {
            throw new GraphGeneratorException();
        } else {
            return random.choose(collection);
        }
    }

    public static List<Concept> allConceptsFrom(GraknGraph graph) {
        List<Concept> concepts = Lists.newArrayList(GraknGraphs.allTypesFrom(graph));
        concepts.addAll(allInstancesFrom(graph));
        return concepts;
    }

    public static Collection<? extends Type> allTypesFrom(GraknGraph graph) {
        return withImplicitConceptsVisible(graph, g -> g.admin().getMetaConcept().subTypes());
    }

    public static Collection<? extends Instance> allInstancesFrom(GraknGraph graph) {
        return withImplicitConceptsVisible(graph, g -> g.admin().getMetaConcept().instances());
    }

    public static <T> T withImplicitConceptsVisible(GraknGraph graph, Function<GraknGraph, T> function) {
        boolean implicitFlag = graph.implicitConceptsVisible();
        graph.showImplicitConcepts(true);
        T result = function.apply(graph);
        graph.showImplicitConcepts(implicitFlag);
        return result;
    }

    @Override
    public void handle(Object[] counterexample, Runnable action) {
        System.err.println("Graph generated:\n" + graphSummary);
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Open {
        boolean value() default true;
    }

    private class GraphGeneratorException extends RuntimeException {

    }
}
