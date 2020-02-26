/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 *
 */

package grakn.core.graql.reasoner.atom.binary;

import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for isa atoms.
 */
public abstract class IsaAtomBase extends TypeAtom{

    IsaAtomBase(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, ConceptId typeId, Variable predicateVariable,
                ReasonerQueryFactory queryFactory, ConceptManager conceptManager, QueryCache queryCache, RuleCache ruleCache) {
        super(varName, pattern, reasonerQuery, typeId, predicateVariable,
                queryFactory, conceptManager, queryCache, ruleCache);
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Variable> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v ->
                        IsaAtom.create(v, getPredicateVariable(), getTypeId(), this.isDirect(), this.getParentQuery(),
                                queryFactory, conceptManager, queryCache, ruleCache))
                        .collect(Collectors.toSet());
    }
}
