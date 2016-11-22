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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import java.util.HashMap;
import java.util.Map;

public abstract class Predicate<T> extends AtomBase{

    protected T predicate = null;

    public Predicate(VarAdmin pattern) { this(pattern, null);}
    public Predicate(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.predicate = extractPredicate(pattern);
    }

    public Predicate(Predicate pred) {
        super(pred);
        this.predicate = extractPredicate(pred.getPattern().asVar());
    }

    /**
     * @return true if the atom corresponds to a unifier (id atom)
     * */
    public boolean isIdPredicate(){ return false;}

    /**
     * @return true if the atom corresponds to a value atom
     * */
    public boolean isValuePredicate(){ return false;}

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Predicate a2 = (Predicate) obj;
        return this.getVarName().equals(a2.getVarName())
                && this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getPredicateValue().hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Predicate a2 = (Predicate) obj;
        return this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getPredicateValue().hashCode();
        return hashCode;
    }

    @Override
    public boolean isPredicate(){ return true;}

    @Override
    public boolean isRuleResolvable(){ return false;}

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        if (!(parentAtom instanceof Predicate))
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        Map<String, String> map = new HashMap<>();
        if (!this.getVarName().equals(parentAtom.getVarName()))
            map.put(this.getVarName(), parentAtom.getVarName());
        return map;
    }

    public abstract String getPredicateValue();
    protected abstract T extractPredicate(VarAdmin pattern);
}