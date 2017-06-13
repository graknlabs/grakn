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

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code sub} property on a {@link ai.grakn.concept.Type}.
 *
 * This property can be queried or inserted.
 *
 * This property relates a {@link ai.grakn.concept.Type} and another {@link ai.grakn.concept.Type}. It indicates
 * that every instance of the left type is also an instance of the right type.
 *
 * @author Felix Chapman
 */
public class SubProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    private final VarPatternAdmin superType;

    public SubProperty(VarPatternAdmin superType) {
        this.superType = superType;
    }

    public VarPatternAdmin getSuperType() {
        return superType;
    }

    @Override
    public String getName() {
        return "sub";
    }

    @Override
    public String getProperty() {
        return superType.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.sub(start, superType.getVarName()));
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(superType);
    }

    @Override
    public Stream<VarPatternAdmin> getInnerVars() {
        return Stream.of(superType);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws GraqlQueryException {
        Concept superConcept = insertQueryExecutor.getConcept(superType);

        if (concept.isEntityType()) {
            concept.asEntityType().superType(superConcept.asEntityType());
        } else if (concept.isRelationType()) {
            concept.asRelationType().superType(superConcept.asRelationType());
        } else if (concept.isRoleType()) {
            concept.asRoleType().superType(superConcept.asRoleType());
        } else if (concept.isResourceType()) {
            concept.asResourceType().superType(superConcept.asResourceType());
        } else if (concept.isRuleType()) {
            concept.asRuleType().superType(superConcept.asRuleType());
        } else {
            throw CommonUtil.unreachableStatement("Can't recognize type " + concept);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubProperty that = (SubProperty) o;

        return superType.equals(that.superType);

    }

    @Override
    public int hashCode() {
        return superType.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.getVarName().asUserDefined();
        VarPatternAdmin typeVar = this.getSuperType();
        Var typeVariable = typeVar.getVarName().asUserDefined();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        VarPatternAdmin resVar = varName.sub(typeVariable).admin();
        return new TypeAtom(resVar, predicate, parent);
    }
}
