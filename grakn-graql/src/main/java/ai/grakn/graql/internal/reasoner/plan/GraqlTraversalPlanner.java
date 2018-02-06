/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner.plan;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.OntologicalAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Resolution planner using {@link GreedyTraversalPlan} to establish optimal resolution order..
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class GraqlTraversalPlanner {

    /**
     *
     * Refined plan procedure:
     * - establish a list of starting atom candidates based on their substitutions
     * - create a plan using {@link GreedyTraversalPlan}
     * - if the graql plan picks an atom that is not a candidate
     *   - pick an optimal candidate
     *   - call the procedure on atoms with removed candidate
     * - otherwise return
     *
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using a refined {@link GraqlTraversal} procedure.
     */
    public static ImmutableList<Atom> refinedPlan(ReasonerQueryImpl query) {
        List<Atom> startCandidates = query.getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toList());
        Set<IdPredicate> subs = query.getAtoms(IdPredicate.class).collect(Collectors.toSet());
        return ImmutableList.copyOf(refinedPlan(query, startCandidates, subs));
    }

    private static Atom optimalCandidate(List<Atom> candidates){
        return candidates.stream()
                .sorted(Comparator.comparing(at -> !at.isGround()))
                .sorted(Comparator.comparing(at -> -at.getPredicates().count()))
                .findFirst().orElse(null);
    }

    /**
     * @param query top level query for which the plan is constructed
     * @param atoms list of current atoms of interest
     * @param subs extra substitutions
     * @return an optimally ordered list of provided atoms
     */
    private static List<Atom> refinedPlan(ReasonerQueryImpl query, List<Atom> atoms, Set<IdPredicate> subs){
        List<Atom> candidates = subs.isEmpty()?
                atoms :
                atoms.stream()
                        .filter(at -> at.getPredicates(IdPredicate.class).findFirst().isPresent())
                        .collect(Collectors.toList());

        ImmutableList<Atom> initialPlan = planFromTraversal(atoms, atomsToPattern(atoms, subs), query.tx());
        if (candidates.contains(initialPlan.get(0)) || candidates.isEmpty()) {
            return initialPlan;
        } else {
            Atom first = optimalCandidate(candidates);

            List<Atom> atomsToPlan = new ArrayList<>(atoms);
            atomsToPlan.remove(first);

            Set<IdPredicate> extraSubs = first.getVarNames().stream()
                    .filter(v -> subs.stream().noneMatch(s -> s.getVarName().equals(v)))
                    .map(v -> new IdPredicate(v, ConceptId.of("placeholderId"), query))
                    .collect(Collectors.toSet());
            return Stream.concat(
                    Stream.of(first),
                    refinedPlan(query, atomsToPlan, Sets.union(subs, extraSubs)).stream()
            ).collect(Collectors.toList());
        }
    }

    /**
     * @param atoms of interest
     * @param subs extra substitutions in the form of id predicates
     * @return conjunctive pattern composed of atoms + their constraints + subs
     */
    private static Conjunction<PatternAdmin> atomsToPattern(List<Atom> atoms, Set<IdPredicate> subs){
        return Patterns.conjunction(
                Stream.concat(
                        atoms.stream().flatMap(at -> Stream.concat(Stream.of(at), at.getNonSelectableConstraints())),
                        subs.stream()
                )
                        .map(Atomic::getCombinedPattern)
                        .flatMap(p -> p.admin().varPatterns().stream())
                        .collect(Collectors.toSet())
        );
    }

    /**
     *
     * @param atoms list of current atoms of interest
     * @param queryPattern corresponding pattern
     * @return an optimally ordered list of provided atoms
     */
    static ImmutableList<Atom> planFromTraversal(List<Atom> atoms, PatternAdmin queryPattern, GraknTx tx){
        Multimap<VarProperty, Atom> propertyMap = HashMultimap.create();
        atoms.stream()
                .filter(at -> !(at instanceof OntologicalAtom))
                .forEach(at -> at.getVarProperties().forEach(p -> propertyMap.put(p, at)));
        Set<VarProperty> properties = propertyMap.keySet();

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(queryPattern, tx);
        ImmutableList<Fragment> fragments = graqlTraversal.fragments().iterator().next();

        ImmutableList.Builder<Atom> builder = ImmutableList.builder();
        builder.addAll(atoms.stream().filter(at -> at instanceof OntologicalAtom).iterator());
        builder.addAll(fragments.stream()
                .map(Fragment::varProperty)
                .filter(Objects::nonNull)
                .filter(properties::contains)
                .distinct()
                .flatMap(p -> propertyMap.get(p).stream())
                .distinct()
                .iterator());
        return builder.build();
    }
}
