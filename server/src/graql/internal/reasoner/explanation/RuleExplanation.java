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

package grakn.core.graql.internal.reasoner.explanation;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Explanation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * <p>
 * Explanation class for rule application.
 * </p>
 *
 *
 */
public class RuleExplanation extends QueryExplanation {

    private final String ruleId;

    public RuleExplanation(String queryPattern, String ruleId){
        super(queryPattern);
        this.ruleId = ruleId;
    }
    private RuleExplanation(String queryPattern, List<ConceptMap> answers, String ruleId){
        super(queryPattern, answers);
        this.ruleId = ruleId;
    }

    @Override
    public Explanation setQueryPattern(String queryPattern){
        return new RuleExplanation(queryPattern, getRuleId());
    }

    @Override
    public Explanation childOf(ConceptMap ans) {
        Explanation explanation = ans.explanation();
        List<ConceptMap> answerList = new ArrayList<>(this.getAnswers());
        answerList.addAll(
                explanation.isLookupExplanation()?
                        Collections.singletonList(ans) :
                        explanation.getAnswers()
        );
        return new RuleExplanation(getQueryPattern(), answerList, getRuleId());
    }

    @Override
    public boolean isRuleExplanation(){ return true;}

    public String getRuleId(){ return ruleId;}
}
