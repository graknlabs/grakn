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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.relates;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code relates} property on a {@link RelationType}.
 *
 * This property can be queried, inserted or deleted.
 *
 * This property relates a {@link RelationType} and a {@link RoleType}. It indicates that a {@link Relation} whose
 * type is this {@link RelationType} may have a role-player playing the given {@link RoleType}.
 *
 * @author Felix Chapman
 */
public class RelatesProperty extends AbstractVarProperty implements NamedProperty {

    private final VarPatternAdmin role;

    public RelatesProperty(VarPatternAdmin role) {
        this.role = role;
    }

    public VarPatternAdmin getRole() {
        return role;
    }

    @Override
    public String getName() {
        return "relates";
    }

    @Override
    public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(relates(start, role.getVarName()));
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(role);
    }

    @Override
    public Stream<VarPatternAdmin> getInnerVars() {
        return Stream.of(role);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws GraqlQueryException {
        RoleType roleType = insertQueryExecutor.getConcept(role).asRoleType();
        concept.asRelationType().relates(roleType);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        TypeLabel roleLabel = role.getTypeLabel().orElseThrow(() -> GraqlQueryException.failDelete(this));
        concept.asRelationType().deleteRelates(graph.getType(roleLabel));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelatesProperty that = (RelatesProperty) o;

        return role.equals(that.role);

    }

    @Override
    public int hashCode() {
        return role.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.getVarName().asUserDefined();
        VarPatternAdmin roleVar = this.getRole();
        Var roleVariable = roleVar.getVarName().asUserDefined();
        IdPredicate rolePredicate = getIdPredicate(roleVariable, roleVar, vars, parent);

        VarPatternAdmin hrVar = varName.relates(roleVariable).admin();
        return new TypeAtom(hrVar, rolePredicate, parent);
    }
}
