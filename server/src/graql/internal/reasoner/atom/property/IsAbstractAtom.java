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

package grakn.core.graql.internal.reasoner.atom.property;

import grakn.core.graql.query.pattern.property.IsAbstract;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.query.pattern.VarPattern;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.internal.reasoner.atom.AtomicBase;
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * Atomic corresponding to {@link IsAbstract}.
 * </p>
 *
 *
 */
@AutoValue
public abstract class IsAbstractAtom extends AtomicBase {

    @Override public abstract VarPattern getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static IsAbstractAtom create(Var varName, ReasonerQuery parent) {
        return new AutoValue_IsAbstractAtom(varName, varName.isAbstract().admin(), parent);
    }

    private static IsAbstractAtom create(IsAbstractAtom a, ReasonerQuery parent) {
        return new AutoValue_IsAbstractAtom(a.getVarName(), a.getPattern(), parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent); }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        return !(obj == null || this.getClass() != obj.getClass());
    }

    @Override
    public int alphaEquivalenceHashCode() { return 1;}

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    @Override
    public boolean subsumes(Atomic atom) { return this.isAlphaEquivalent(atom); }

}
