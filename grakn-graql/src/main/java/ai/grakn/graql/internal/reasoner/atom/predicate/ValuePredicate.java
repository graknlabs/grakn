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

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an value predicate. Corresponds to {@link ValueProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ValuePredicate extends Predicate<ValuePredicateAdmin> {

    public ValuePredicate(VarPatternAdmin pattern, ReasonerQuery par) { super(pattern, par);}
    public ValuePredicate(Var varName, ValuePredicateAdmin pred, ReasonerQuery par){
        this(createValueVar(varName, pred), par);}
    private ValuePredicate(ValuePredicate pred) { super(pred);}

    @Override
    public Atomic copy() {
        return new ValuePredicate(this);
    }

    public Set<ValuePredicate> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> new ValuePredicate(v, getPredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    public static VarPatternAdmin createValueVar(Var name, ValuePredicateAdmin pred) {
        return Graql.var(name).val(pred).admin();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Predicate a2 = (Predicate) obj;
        return this.getVarName().equals(a2.getVarName())
                && this.getPredicate().getClass().equals(a2.getPredicate().getClass())
                && this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        hashCode = hashCode * 37 + this.getPredicate().getClass().hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate a2 = (ValuePredicate) obj;
        return this.getPredicate().getClass().equals(a2.getPredicate().getClass()) &&
                this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = super.equivalenceHashCode();
        hashCode = hashCode * 37 + this.getPredicate().getClass().getName().hashCode();
        return hashCode;
    }

    @Override
    public boolean isValuePredicate(){ return true;}

    @Override
    public String getPredicateValue() {
        return getPredicate().getPredicate().map(P::getValue).map(Object::toString).orElse("");
    }

    @Override
    protected ValuePredicateAdmin extractPredicate(VarPatternAdmin pattern) {
        Iterator<ValueProperty> properties = pattern.getProperties(ValueProperty.class).iterator();
        ValueProperty property = properties.next();
        if (properties.hasNext()) {
            throw new IllegalStateException("Attempting creation of ValuePredicate atom with more than single predicate");
        }
        return property.getPredicate();
    }

    @Override
    public Set<Var> getVarNames(){
        Set<Var> vars = super.getVarNames();
        VarPatternAdmin innerVar = getPredicate().getInnerVar().orElse(null);
        if(innerVar != null && innerVar.isUserDefinedName()) vars.add(innerVar.getVarName());
        return vars;
    }
}
