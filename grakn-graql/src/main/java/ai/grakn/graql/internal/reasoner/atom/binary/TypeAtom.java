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
package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.pattern.property.HasAttributeTypeProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 *
 * <p>
 * Atom implementation defining type atoms of the general form:
 *
 * {isa|sub|plays|relates|has}($varName, $predicateVariable)
 *
 * Type atoms correspond to the following respective graql properties:
 * {@link IsaProperty},
 * {@link ai.grakn.graql.internal.pattern.property.SubProperty},
 * {@link ai.grakn.graql.internal.pattern.property.PlaysProperty}
 * {@link ai.grakn.graql.internal.pattern.property.RelatesProperty}
 * {@link HasAttributeTypeProperty}
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class TypeAtom extends Binary{

    protected TypeAtom(VarPattern pattern, Var predicateVar, @Nullable IdPredicate p, ReasonerQuery parent) {
        super(pattern, predicateVar, p, parent);}
    protected TypeAtom(TypeAtom a, ReasonerQuery parent) { super(a, parent);}

    @Override
    public boolean isType(){ return true;}

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        return this.getSchemaConcept() == null ||
                //ensure not ontological atom query
                getPattern().admin().hasProperty(IsaProperty.class)
                && this.getSchemaConcept().subs().anyMatch(sub -> sub.equals(ruleAtom.getSchemaConcept()));
    }

    @Override
    public boolean isSelectable() {
        return getTypePredicate() == null
                //disjoint atom
                || !this.getNeighbours(Atom.class).findFirst().isPresent()
                || getPotentialRules().findFirst().isPresent();
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefined() && getSchemaConcept() != null && getSchemaConcept().isRelationshipType();
    }

    @Nullable
    @Override
    public SchemaConcept getSchemaConcept() {
        if (getTypePredicate() == null) return null;
        SchemaConcept schemaConcept = tx().getConcept(getTypePredicate().getPredicate());
        if (schemaConcept == null) throw GraqlQueryException.idNotFound(getTypePredicate().getPredicate());
        return schemaConcept;
    }

    /**
     * @param u unifier to be applied
     * @return set of type atoms resulting from applying the unifier
     */
    public abstract Set<TypeAtom> unify(Unifier u);
}

