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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import java.util.Set;

public class ValueFlagProperty extends AbstractVarProperty implements UniqueVarProperty, SingleFragmentProperty {

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("value");
    }

    @Override
    public Fragment getFragment(VarName start) {
        return Fragments.value(start);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass());

    }

    @Override
    public int hashCode() {
        return 17;
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        return new ValuePredicate(var.getVarName(), parent);
    }
}
