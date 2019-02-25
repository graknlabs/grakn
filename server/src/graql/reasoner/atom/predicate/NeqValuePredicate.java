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

package grakn.core.graql.reasoner.atom.predicate;

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;

public class NeqValuePredicate extends NeqPredicate {

    private final Object value;

    private NeqValuePredicate(Variable varName, Variable predicateVar, @Nullable Object value, Statement pattern, ReasonerQuery parentQuery) {
        super(varName, predicateVar, pattern, parentQuery);
        this.value = value;
    }

    public static NeqValuePredicate create(Variable varName, @Nullable Variable var, @Nullable Object value, ReasonerQuery parent){
        Variable predicateVar = var != null? var : Graql.var().var().asUserDefined();
        ValueProperty.Operation.Comparison<?> op = ValueProperty.Operation.Comparison.of(Graql.Token.Comparator.NEQV, value != null ? value : Graql.var(predicateVar));
        Statement pattern = new Statement(varName).operation(op);
        return new NeqValuePredicate(varName, predicateVar, value, pattern, parent);
    }

    public static NeqValuePredicate create(Variable varName, ValueProperty.Operation op, ReasonerQuery parent) {
        Statement innerStatement = op.innerStatement();
        Variable var = innerStatement != null? innerStatement.var() : Graql.var().var().asUserDefined();
        Object value = var == null? op.value() : null;
        return create(varName, var, value, parent);
    }

    @Nullable public Object getValue(){ return value;}

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this.getVarName(), this.getPredicate(), this.getValue(), parent);}

    @Override
    public String toString(){
        return "[" + getVarName() + " !== " + getPredicate() + "]"
                + (getValue() != null? "[" + getPredicate() + "/" + getValue() + "]" : "");
    }

    @Override
    public boolean isSatisfied(ConceptMap sub) {
        Object predicateVal = getPredicates(getPredicate(), ValuePredicate.class)
                .map(p -> p.getPredicate().value())
                .findFirst().orElse(null);

        Object val = getValue() != null? getValue() : predicateVal;
        if (val == null &&
                (!sub.containsVar(getVarName()) || !sub.containsVar(getPredicate()))) {
            return true;
        }

        Concept concept = sub.containsVar(getVarName())? sub.get(getVarName()) : null;
        Concept referenceConcept = sub.containsVar(getPredicate())? sub.get(getPredicate()) : null;
        return  concept != null
                && concept.isAttribute()
                && (
                        (val != null && !concept.asAttribute().value().equals(val))
                                || (
                                        referenceConcept != null
                                                && referenceConcept.isAttribute()
                                                && !concept.asAttribute().value().equals(referenceConcept.asAttribute().value())
                        )
        );
    }

}
