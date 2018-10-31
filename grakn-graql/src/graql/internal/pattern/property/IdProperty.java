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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code id} property on a {@link ai.grakn.concept.Concept}.
 *
 * This property can be queried. While this property cannot be inserted, if used in an insert query any existing concept
 * with the given ID will be retrieved.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class IdProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    public static final String NAME = "id";

    public static IdProperty of(ConceptId id) {
        return new AutoValue_IdProperty(id);
    }

    public abstract ConceptId id();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return StringConverter.idToString(id());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.id(this, start, id()));
    }

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).id(id());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        // This property works in both insert and define queries, because it is only for look-ups
        return insert(var);
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        // This property works in undefine queries, because it is only for look-ups
        return insert(var);
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return IdPredicate.create(var.var(), id(), parent);
    }
}
