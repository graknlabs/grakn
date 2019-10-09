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
 */

package grakn.core.graql.gremlin.sets;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.graql.gremlin.fragment.Fragments;
import grakn.core.kb.graql.planning.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Set;

/**
 * @see EquivalentFragmentSets#id(VarProperty, Variable, ConceptId)
 *
 */
@AutoValue
abstract class IdFragmentSet extends EquivalentFragmentSetImpl {

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.id(varProperty(), var(), id()));
    }

    abstract Variable var();
    abstract ConceptId id();
}
