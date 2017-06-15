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

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.AnswerExplanation;

/**
 *
 * <p>
 * Explanation class for a join explanation - resulting from merging atoms in a conjunction.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class JoinExplanation extends Explanation {


    public JoinExplanation(){ super();}
    private JoinExplanation(JoinExplanation exp){
        super(exp.getQuery(), exp.getAnswers());
    }

    @Override
    public AnswerExplanation copy(){ return new JoinExplanation(this);}

    @Override
    public boolean isJoinExplanation(){ return true;}
}
