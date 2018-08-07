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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import com.google.common.base.Equivalence;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;


/**
 *
 * <p>
 * Static class defining different equivalence comparisons for {@link Atomic}s :
 *
 * - Equality: two {@link Atomic}s are equal if they are equal including all their corresponding variables.
 *
 * - Alpha-equivalence: two {@link Atomic}s are alpha-equivalent if they are equal up to the choice of free variables.
 *
 * - Structural equivalence:
 * Two atomics are structurally equivalent if they are equal up to the choice of free variables and partial substitutions (id predicates {@link ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate}).
 * Hence:
 * - any two IdPredicates are structurally equivalent
 * - structural equivalence check on queries is done by looking at {@link Atom}s. As a result:
 *   * connected predicates are assessed together with atoms they are connected to
 *   * dangling predicates are ignored
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AtomicEquivalence extends Equivalence<Atomic> {

    public static <B extends Atomic, S extends B> boolean equivalence(Collection<S> a1, Collection<S> a2, Equivalence<B> equiv){
        return ReasonerUtils.isEquivalentCollection(a1, a2, equiv);
    }

    public static <B extends Atomic, S extends B> int equivalenceHash(Stream<S> atoms, Equivalence<B> equiv){
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        atoms.forEach(atom -> hashes.add(equiv.hash(atom)));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    public static <B extends Atomic, S extends B> int equivalenceHash(Collection<S> atoms, Equivalence<B> equiv){
        return equivalenceHash(atoms.stream(), equiv);
    }

    public final static Equivalence<Atomic> Equality = new AtomicEquivalence(){

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.equals(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.hashCode();
        }
    };

    public final static Equivalence<Atomic> AlphaEquivalence = new AtomicEquivalence(){

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.isAlphaEquivalent(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.alphaEquivalenceHashCode();
        }
    };

    public final static Equivalence<Atomic> StructuralEquivalence = new AtomicEquivalence(){

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.isStructurallyEquivalent(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.structuralEquivalenceHashCode();
        }
    };
}
