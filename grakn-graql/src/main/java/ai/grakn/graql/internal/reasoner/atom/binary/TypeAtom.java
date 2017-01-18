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
package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Type;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

/**
 *
 * <p>
 * Atom implementation defining type atoms of the type $varName {isa|sub|plays-role|has-resource|has-scope} $valueVariable)
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class TypeAtom extends Binary{

    public TypeAtom(VarAdmin pattern, ReasonerQuery par) { this(pattern, null, par);}
    public TypeAtom(VarAdmin pattern, IdPredicate p, ReasonerQuery par) { super(pattern, p, par);}
    protected TypeAtom(TypeAtom a) { super(a);}

    @Override
    protected ConceptId extractTypeId(VarAdmin var) {
        return getPredicate() != null? getPredicate().getPredicate() : null;
    }

    @Override
    protected VarName extractValueVariableName(VarAdmin var) {
        return var.getProperties().findFirst().orElse(null).getInnerVars().findFirst().orElse(null).getVarName();
    }

    @Override
    protected void setValueVariable(VarName var) {
        super.setValueVariable(var);
        atomPattern.asVar().getProperties(IsaProperty.class).forEach(prop -> prop.getType().setVarName(var));
    }

    @Override
    public Atomic copy(){
        return new TypeAtom(this);
    }

    @Override
    public boolean isType(){ return true;}

    @Override
    public Type getType() {
        return getPredicate() != null ?
                getParentQuery().graph().getConcept(getPredicate().getPredicate()) : null;
    }
}

