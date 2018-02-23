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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;

import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.binary.HasAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.OntologicalAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.PlaysAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelatesAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.SubAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 * <p>
 * Factory class for creating {@link Atomic} objects.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class AtomicFactory {

    /**
     * @param atomType to create
     * @param var atom variable name
     * @param predicateVar predicate variable name
     * @param predicate atom's predicate
     * @param parent reasoner query the atom should belong to
     * @param <T> atom type parameter
     * @return fresh ontological atom
     */
   public static <T extends OntologicalAtom> OntologicalAtom createOntologicalAtom(Class<T> atomType, Var var, Var predicateVar, @Nullable IdPredicate predicate, ReasonerQuery parent) {
        if (atomType.equals(PlaysAtom.class)) { return PlaysAtom.create(var, predicateVar, predicate, parent); }
        else if (atomType.equals(HasAtom.class)){ return HasAtom.create(var, predicateVar, predicate, parent); }
        else if (atomType.equals(RelatesAtom.class)){ return RelatesAtom.create(var, predicateVar, predicate, parent); }
        else if (atomType.equals(SubAtom.class)){ return SubAtom.create(var, predicateVar, predicate, parent); }
        else{ throw GraqlQueryException.illegalAtomCreation(atomType.getName()); }
    }
    /**
     * @param atom to be copied
     * @param parent query the copied atom should belong to
     * @return atom copy
     */
    public static Atomic create(Atomic atom, ReasonerQuery parent) {
        return atom.copy(parent);
    }

    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public static Stream<Atomic> createAtoms(Conjunction<VarPatternAdmin> pattern, ReasonerQuery parent) {
        Set<Atomic> atoms = pattern.varPatterns().stream()
                .flatMap(var -> var.getProperties()
                        .map(vp -> vp.mapToAtom(var, pattern.varPatterns(), parent))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        return atoms.stream()
                .filter(at -> atoms.stream()
                        .filter(Atom.class::isInstance)
                        .map(Atom.class::cast)
                        .flatMap(Atom::getInnerPredicates)
                        .noneMatch(at::equals)
                );
    }

}
