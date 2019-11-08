/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 *
 */

package grakn.core.concept.util;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.impl.SchemaConceptImpl;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.structure.PropertyNotUniqueException;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;
import grakn.core.core.Schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class ConceptUtils {

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return top (most general) non-meta {@link SchemaConcept}s from within the provided set
     */
    public static <T extends SchemaConcept> Set<T> top(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(t -> Sets.intersection(nonMetaSups(t), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return bottom (most specific) non-meta {@link SchemaConcept}s from within the provided set
     */
    public static <T extends SchemaConcept> Set<T> bottom(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(t -> Sets.intersection(t.subs().filter(t2 -> !t.equals(t2)).collect(toSet()), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return top {@link SchemaConcept}s from within the provided set or meta concept if it exists
     */
    public static <T extends SchemaConcept> Set<T> topOrMeta(Set<T> schemaConcepts) {
        Set<T> concepts = top(schemaConcepts);
        T meta = concepts.stream()
                .filter(c -> Schema.MetaSchema.isMetaLabel(c.label()))
                .findFirst().orElse(null);
        return meta != null ? Collections.singleton(meta) : concepts;
    }

    /**
     * @param schemaConcept input type
     * @return set of all non-meta super types of the role
     */
    public static Set<? extends SchemaConcept> nonMetaSups(SchemaConcept schemaConcept) {
        Set<SchemaConcept> superTypes = new HashSet<>();
        SchemaConcept superType = schemaConcept.sup();
        while (superType != null && !Schema.MetaSchema.isMetaLabel(superType.label())) {
            superTypes.add(superType);
            superType = superType.sup();
        }
        return superTypes;
    }

    /**
     * @param parent type
     * @param child  type
     * @param direct flag indicating whether only direct types should be considered
     * @return true if child is a subtype of parent
     */
    private static boolean typesCompatible(SchemaConcept parent, SchemaConcept child, boolean direct) {
        if (parent == null) return true;
        if (child == null) return false;
        if (direct) return parent.equals(child);
        if (Schema.MetaSchema.isMetaLabel(parent.label())) return true;
        SchemaConcept superType = child;
        while (superType != null && !Schema.MetaSchema.isMetaLabel(superType.label())) {
            if (superType.equals(parent)) return true;
            superType = superType.sup();
        }
        return false;
    }

    /**
     * @param parentTypes set of types defining parent, parent defines type constraints to be fulfilled
     * @param childTypes  set of types defining child
     * @param direct      flag indicating whether only direct types should be considered
     * @return true if type sets are disjoint - it's possible to find a disjoint pair among parent and child set
     */
    public static boolean areDisjointTypeSets(Set<? extends SchemaConcept> parentTypes, Set<? extends SchemaConcept> childTypes, boolean direct) {
        return childTypes.isEmpty() && !parentTypes.isEmpty()
                || parentTypes.stream().anyMatch(parent -> childTypes.stream()
                .anyMatch(child -> ConceptUtils.areDisjointTypes(parent, child, direct)));
    }

    /**
     * determines disjointness of parent-child types, parent defines the bound on the child
     *
     * @param parent {@link SchemaConcept}
     * @param child  {@link SchemaConcept}
     * @param direct flag indicating whether only direct types should be considered
     * @return true if types do not belong to the same type hierarchy, also:
     * - true if parent is null and
     * - false if parent non-null and child null - parents defines a constraint to satisfy
     */
    private static boolean areDisjointTypes(SchemaConcept parent, SchemaConcept child, boolean direct) {
        return parent != null && child == null || !typesCompatible(parent, child, direct) && !typesCompatible(child, parent, direct);
    }

    /**
     * Computes dependent concepts of a thing - concepts that need to be persisted if we persist the provided thing.
     *
     * @param topThings things dependants of which we want to retrieve
     * @return stream of things that are dependants of the provided thing - includes non-direct dependants.
     */
    public static Stream<Thing> getDependentConcepts(Collection<Thing> topThings) {
        Set<Thing> things = new HashSet<>(topThings);
        Set<Thing> visitedThings = new HashSet<>();
        Stack<Thing> thingStack = new Stack<>();
        thingStack.addAll(topThings);
        while (!thingStack.isEmpty()) {
            Thing thing = thingStack.pop();
            if (!visitedThings.contains(thing)) {
                thing.getDependentConcepts()
                        .peek(things::add)
                        .filter(t -> !visitedThings.contains(t))
                        .forEach(thingStack::add);
                visitedThings.add(thing);
            }
        }
        return things.stream();
    }

    /**
     * Performs a natural join (⋈) between answers with the resultant answer containing the explanation of the left operand.
     * If the answers have an empty set of common variables, the join corresponds to a trivial entry set union.
     * NB: Assumes answers are compatible (concepts corresponding to join vars if any are the same or are compatible types)
     *
     * @param baseAnswer left operand of answer join
     * @param toJoin right operand of answer join
     * @return joined answers
     */
    public static ConceptMap joinAnswers(ConceptMap baseAnswer, ConceptMap toJoin) {
        if (toJoin.isEmpty()) return baseAnswer;
        if (baseAnswer.isEmpty()) return toJoin;

        Set<Variable> joinVars = Sets.intersection(baseAnswer.vars(), toJoin.vars());
        Map<Variable, Concept> entryMap = Stream
                .concat(baseAnswer.map().entrySet().stream(), toJoin.map().entrySet().stream())
                .filter(e -> !joinVars.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (Variable var : joinVars) {
            Concept concept = baseAnswer.get(var);
            Concept otherConcept = toJoin.get(var);
            if (concept.equals(otherConcept)) entryMap.put(var, concept);
            else {
                boolean typeCompatible = concept.isSchemaConcept() && otherConcept.isSchemaConcept()
                        && !ConceptUtils.areDisjointTypes(concept.asSchemaConcept(), otherConcept.asSchemaConcept(), false);
                if (typeCompatible) {
                    SchemaConcept topType = Iterables.getOnlyElement(ConceptUtils.topOrMeta(
                            Sets.newHashSet(
                                    concept.asSchemaConcept(),
                                    otherConcept.asSchemaConcept())
                            )
                    );
                    entryMap.put(var, topType);
                }
                return new ConceptMap();
            }
        }
        Pattern mergedPattern;
        if (baseAnswer.getPattern() != null && toJoin.getPattern() != null) {
            mergedPattern = Graql.and(baseAnswer.getPattern(), toJoin.getPattern());
        } else if (baseAnswer.getPattern() != null) {
            mergedPattern = baseAnswer.getPattern();
        } else {
            mergedPattern = toJoin.getPattern();
        }
        return new ConceptMap(entryMap, baseAnswer.explanation(), mergedPattern);
    }

    public static void validateBaseType(SchemaConceptImpl schemaConcept, Schema.BaseType expectedBaseType) {
        // throws if label is already taken for a different type
        if (!expectedBaseType.equals(schemaConcept.baseType())) {
            throw PropertyNotUniqueException.cannotCreateProperty(schemaConcept, Schema.VertexProperty.SCHEMA_LABEL, schemaConcept.label());
        }
    }
}