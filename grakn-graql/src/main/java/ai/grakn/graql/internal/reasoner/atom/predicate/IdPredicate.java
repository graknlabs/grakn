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

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.reasoner.utils.IgnoreHashEquals;
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an id predicate. Corresponds to {@link IdProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class IdPredicate extends Predicate<ConceptId>{

    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();
    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract ConceptId getPredicate();

    public static IdPredicate create(VarPattern pattern, ReasonerQuery parent) {
        return new AutoValue_IdPredicate(pattern.admin().var(), pattern, parent, extractPredicate(pattern));
    }
    public static IdPredicate create(Var varName, Label label, ReasonerQuery parent) {
        return create(createIdVar(varName.asUserDefined(), label, parent.tx()), parent);
    }
    public static IdPredicate create(Var varName, ConceptId id, ReasonerQuery parent) {
        return create(createIdVar(varName.asUserDefined(), id), parent);
    }
    public static IdPredicate create(Var varName, Concept con, ReasonerQuery parent) {
        return create(createIdVar(varName.asUserDefined(), con.id()), parent);
    }
    private static IdPredicate create(IdPredicate a, ReasonerQuery parent) {
        return create(a.getPattern(), parent);
    }

    private static ConceptId extractPredicate(VarPattern var){
        return var.admin().getProperty(IdProperty.class).map(IdProperty::id).orElse(null);
    }

    private static VarPattern createIdVar(Var varName, ConceptId typeId){
        return varName.id(typeId);
    }

    private static VarPattern createIdVar(Var varName, Label label, GraknTx graph){
        SchemaConcept schemaConcept = graph.getSchemaConcept(label);
        if (schemaConcept == null) throw GraqlQueryException.labelNotFound(label);
        return varName.id(schemaConcept.id());
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        return true;
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return 1;
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public void checkValid() {
        ConceptId conceptId = getPredicate();
        if (tx().getConcept(conceptId) == null){
            throw GraqlQueryException.idNotFound(conceptId);
        }
    }

    @Override
    public String toString(){
        return "[" + getVarName() + "/" + getPredicateValue() + "]";
    }

    @Override
    public String getPredicateValue() { return getPredicate().getValue();}
}
