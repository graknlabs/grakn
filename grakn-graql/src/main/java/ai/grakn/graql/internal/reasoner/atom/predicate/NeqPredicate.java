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

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an inequality predicate. Corresponds to graql {@link NeqProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class NeqPredicate extends Predicate<Var> {

    public NeqPredicate(Var varName, NeqProperty prop, ReasonerQuery parent){
        super(var(varName).neq(var(prop.getVar().getVarName())).admin(), parent);
    }
    public NeqPredicate(NeqPredicate a){
        super(a);
    }

    @Override
    public String toString(){ return "[" + getVarName() + "!=" + getReferenceVarName() + "]";}

    @Override
    public Atomic copy() { return new NeqPredicate(this);}

    @Override
    public String getPredicateValue() {
        return getPredicate().getValue();
    }

    @Override
    protected Var extractPredicate(VarPatternAdmin pattern) {
        return pattern.getProperties(NeqProperty.class).iterator().next().getVar().getVarName();
    }

    @Override
    public boolean isNeqPredicate(){ return true;}

    @Override
    public Set<Var> getVarNames(){
        Set<Var> vars = super.getVarNames();
        vars.add(getReferenceVarName());
        return vars;
    }

    private Var getReferenceVarName(){
        return getPredicate();
    }

    public static boolean notEqualsOperator(Answer answer, NeqPredicate atom) {
        return !answer.get(atom.getVarName()).equals(answer.get(atom.getReferenceVarName()));
    }

    /**
     * apply the not equals filter to answer set
     * @param answers the filter should be applied to
     * @return filtered answer set
     */
    public QueryAnswers filter(QueryAnswers answers){
        QueryAnswers results = new QueryAnswers();
        Var refVarName = getReferenceVarName();
        answers.stream()
                .filter(answer -> !answer.get(getVarName()).equals(answer.get(refVarName)))
                .forEach(results::add);
        return results;
    }

    /**
     * apply the not equals filter to answer stream
     * @param answers the filter should be applied to
     * @return filtered answer stream
     */
    public Stream<Answer> filter(Stream<Answer> answers){
        Var refVarName = getReferenceVarName();
        return answers.filter(answer -> !answer.get(getVarName()).equals(answer.get(refVarName)));
    }
}
