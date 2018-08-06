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

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.utils.IgnoreHashEquals;
import com.google.auto.value.AutoValue;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an value predicate. Corresponds to {@link ValueProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class ValuePredicate extends Predicate<ai.grakn.graql.ValuePredicate> {

    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();

    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract ai.grakn.graql.ValuePredicate getPredicate();

    public static ValuePredicate create(VarPattern pattern, ReasonerQuery parent) {
        return new AutoValue_ValuePredicate(pattern.admin().var(), pattern, parent, extractPredicate(pattern));
    }
    public static ValuePredicate create(Var varName, ai.grakn.graql.ValuePredicate pred, ReasonerQuery parent) {
        return create(createValueVar(varName, pred), parent);
    }
    private static ValuePredicate create(ValuePredicate pred, ReasonerQuery parent) {
        return create(pred.getPattern(), parent);
    }

    public static VarPattern createValueVar(Var name, ai.grakn.graql.ValuePredicate pred) { return name.val(pred);}

    private static ai.grakn.graql.ValuePredicate extractPredicate(VarPattern pattern) {
        Iterator<ValueProperty> properties = pattern.admin().getProperties(ValueProperty.class).iterator();
        ValueProperty property = properties.next();
        if (properties.hasNext()) {
            throw GraqlQueryException.valuePredicateAtomWithMultiplePredicates();
        }
        return property.predicate();
    }

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this, parent);
    }

    @Override
    public String toString(){ return "[" + getVarName() + " val " + getPredicate() + "]"; }

    public Set<ValuePredicate> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> create(v, getPredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public boolean isAlphaEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate p2 = (ValuePredicate) obj;
        return this.getPredicate().getClass().equals(p2.getPredicate().getClass()) &&
                this.getPredicateValue().equals(p2.getPredicateValue());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = super.alphaEquivalenceHashCode();
        hashCode = hashCode * 37 + this.getPredicate().getClass().getName().hashCode();
        return hashCode;
    }

    @Override
    public boolean isCompatibleWith(Object obj) {
        if (this.isAlphaEquivalent(obj)) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate p2 = (ValuePredicate) obj;
        return getPredicate().isCompatibleWith(p2.getPredicate());
    }

    @Override
    public String getPredicateValue() {
        return getPredicate().getPredicate().map(P::getValue).map(Object::toString).orElse("");
    }

    @Override
    public Set<Var> getVarNames(){
        Set<Var> vars = super.getVarNames();
        VarPatternAdmin innerVar = getPredicate().getInnerVar().orElse(null);
        if(innerVar != null && innerVar.var().isUserDefinedName()) vars.add(innerVar.var());
        return vars;
    }
}
