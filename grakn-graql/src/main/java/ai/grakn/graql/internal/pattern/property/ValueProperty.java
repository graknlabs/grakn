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

import ai.grakn.concept.Attribute;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.util.CommonUtil;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the {@code value} property on a {@link Attribute}.
 *
 * This property can be queried or inserted.
 *
 * This property matches only resources whose value matches the given {@link ValuePredicate}.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class ValueProperty extends AbstractVarProperty implements NamedProperty {

    public static final String NAME = "val";

    public static ValueProperty of(ValuePredicateAdmin predicate) {
        return new AutoValue_ValueProperty(predicate);
    }

    public abstract ValuePredicateAdmin predicate();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return predicate().toString();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.value(this, start, predicate()));
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        executor.builder(var).value(predicate().equalsValue().get()); // TODO
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        return ImmutableSet.of();
    }

    @Override
    public Set<Var> producedVars(Var var) {
        return ImmutableSet.of(var);
    }

    @Override
    public void checkInsertable(VarPatternAdmin var) {
        if (!predicate().equalsValue().isPresent()) {
            throw GraqlQueryException.insertPredicate();
        }
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return CommonUtil.optionalToStream(predicate().getInnerVar());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return new ValuePredicate(var.var(), this.predicate(), parent);
    }
}
