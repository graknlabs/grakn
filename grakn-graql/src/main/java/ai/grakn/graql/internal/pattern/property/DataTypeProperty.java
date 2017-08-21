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

import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.parser.QueryParser;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.property.DataTypeAtom;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code datatype} property on a {@link AttributeType}.
 *
 * This property can be queried or inserted.
 *
 * The insertion behaviour is not implemented here, but instead in
 * {@link ai.grakn.graql.internal.query.InsertQueryExecutor}.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class DataTypeProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    public static final String NAME = "datatype";

    public static DataTypeProperty of(AttributeType.DataType<?> datatype) {
        return new AutoValue_DataTypeProperty(datatype);
    }

    public abstract AttributeType.DataType<?> dataType();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return QueryParser.DATA_TYPES.inverse().get(dataType());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.dataType(this, start, dataType()));
    }

    @Override
    public void define(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        executor.builder(var).dataType(dataType());
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
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return new DataTypeAtom(var.var(), this, parent);
    }
}
