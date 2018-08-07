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

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * @author Felix Chapman
 */
public class RolePlayerFragmentSetTest {

    @ClassRule
    public static final SampleKBContext sampleKB = MovieKB.context();

    private final Var a = Graql.var("a"), b = Graql.var("b"), c = Graql.var("c"), d = Graql.var("d");

    @Test
    public void whenApplyingRoleOptimisation_ExpandRoleToAllSubs() {
        Label author = Label.of("author");
        Label director = Label.of("director");
        EquivalentFragmentSet authorLabelFragmentSet = EquivalentFragmentSets.label(null, d, ImmutableSet.of(author));

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(
                EquivalentFragmentSets.rolePlayer(null, a, b, c, d),
                authorLabelFragmentSet
        );

        RolePlayerFragmentSet.ROLE_OPTIMISATION.apply(fragmentSets, sampleKB.tx());

        HashSet<EquivalentFragmentSet> expected = Sets.newHashSet(
                RolePlayerFragmentSet.of(null, a, b, c, null, ImmutableSet.of(author, director), null),
                authorLabelFragmentSet
        );

        assertEquals(expected, fragmentSets);
    }

    @Test
    public void whenRoleIsNotInGraph_DoNotApplyRoleOptimisation() {
        Label magician = Label.of("magician");

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(
                EquivalentFragmentSets.rolePlayer(null, a, b, c, d),
                EquivalentFragmentSets.label(null, d, ImmutableSet.of(magician))
        );

        Collection<EquivalentFragmentSet> expected = Sets.newHashSet(fragmentSets);

        RolePlayerFragmentSet.ROLE_OPTIMISATION.apply(fragmentSets, sampleKB.tx());

        assertEquals(expected, fragmentSets);
    }

    @Test
    public void whenLabelDoesNotReferToARole_DoNotApplyRoleOptimisation() {
        Label movie = Label.of("movie");

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(
                EquivalentFragmentSets.rolePlayer(null, a, b, c, d),
                EquivalentFragmentSets.label(null, d, ImmutableSet.of(movie))
        );

        Collection<EquivalentFragmentSet> expected = Sets.newHashSet(fragmentSets);

        RolePlayerFragmentSet.ROLE_OPTIMISATION.apply(fragmentSets, sampleKB.tx());

        assertEquals(expected, fragmentSets);
    }

    @Test
    public void whenApplyingRoleOptimisationToMetaRole_DoNotExpandRoleToAllSubs() {
        Label role = Label.of("role");
        EquivalentFragmentSet authorLabelFragmentSet = EquivalentFragmentSets.label(null, d, ImmutableSet.of(role));

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(
                EquivalentFragmentSets.rolePlayer(null, a, b, c, d),
                authorLabelFragmentSet
        );

        RolePlayerFragmentSet.ROLE_OPTIMISATION.apply(fragmentSets, sampleKB.tx());

        HashSet<EquivalentFragmentSet> expected = Sets.newHashSet(
                RolePlayerFragmentSet.of(null, a, b, c, null, null, null),
                authorLabelFragmentSet
        );

        assertEquals(expected, fragmentSets);
    }

    @Test
    public void whenRelationTypeIsNotInGraph_DoNotApplyRelationTypeOptimisation() {
        Label magician = Label.of("magician");

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(
                EquivalentFragmentSets.rolePlayer(null, a, b, c, null),
                EquivalentFragmentSets.isa(null, a, d, true),
                EquivalentFragmentSets.label(null, d, ImmutableSet.of(magician))
        );

        Collection<EquivalentFragmentSet> expected = Sets.newHashSet(fragmentSets);

        RolePlayerFragmentSet.RELATION_TYPE_OPTIMISATION.apply(fragmentSets, sampleKB.tx());

        assertEquals(expected, fragmentSets);
    }

    @Test
    public void whenLabelDoesNotReferToARelationType_DoNotApplyRelationTypeOptimisation() {
        Label movie = Label.of("movie");

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(
                EquivalentFragmentSets.rolePlayer(null, a, b, c, null),
                EquivalentFragmentSets.isa(null, a, d, true),
                EquivalentFragmentSets.label(null, d, ImmutableSet.of(movie))
        );

        Collection<EquivalentFragmentSet> expected = Sets.newHashSet(fragmentSets);

        RolePlayerFragmentSet.RELATION_TYPE_OPTIMISATION.apply(fragmentSets, sampleKB.tx());

        assertEquals(expected, fragmentSets);
    }
}