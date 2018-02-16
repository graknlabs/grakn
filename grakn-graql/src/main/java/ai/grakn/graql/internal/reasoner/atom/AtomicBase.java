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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.concept.Rule;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

/**
 *
 * <p>
 * Base {@link Atomic} implementation providing basic functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AtomicBase implements Atomic {

    private final Var varName;
    private final VarPattern atomPattern;
    private Pattern combinedPattern = null;
    private ReasonerQuery parent;

    protected AtomicBase(VarPattern pattern, ReasonerQuery par) {
        this.atomPattern = pattern;
        this.varName = pattern.admin().var();
        this.parent = par;
    }

    protected AtomicBase(AtomicBase a) {
        this.atomPattern = a.atomPattern;
        this.varName = atomPattern.admin().var();
        this.parent = a.getParentQuery();
    }

    @Override
    public abstract Atomic copy();

    @Override
    public void checkValid(){}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.getThen(), rule.getLabel()));
    }

    @Override
    public String toString(){ return atomPattern.toString(); }

    public boolean containsVar(Var name){ return getVarNames().contains(name);}

    public boolean isUserDefined(){ return varName.isUserDefinedName();}
    
    @Override
    public Var getVarName(){ return varName;}

    @Override
    public Set<Var> getVarNames(){
        return varName.isUserDefinedName()? Sets.newHashSet(varName) : Collections.emptySet();
    }

    @Override
    public VarPattern getPattern(){ return atomPattern;}

    protected Pattern createCombinedPattern(){ return atomPattern;}

    @Override
    public Pattern getCombinedPattern(){
        if (combinedPattern == null) combinedPattern = createCombinedPattern();
        return combinedPattern;
    }

    public ReasonerQuery getParentQuery(){ return parent;}

    @Override
    public void setParentQuery(ReasonerQuery q){ parent = q;}

    @Override
    public Atomic inferTypes(){ return inferTypes(new QueryAnswer()); }

    public Atomic inferTypes(Answer sub){ return this; }

    /**
     * @return GraknTx this atomic is defined in
     */
    protected EmbeddedGraknTx<?> tx(){
        // TODO: This cast is unsafe - ReasonerQuery should return an EmbeddedGraknTx
        return (EmbeddedGraknTx<?>) getParentQuery().tx();
    }
}

