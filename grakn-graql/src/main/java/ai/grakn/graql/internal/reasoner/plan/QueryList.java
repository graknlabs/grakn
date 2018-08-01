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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.plan;

import ai.grakn.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.base.Equivalence;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.plan.ResolutionQueryPlan.prioritise;


/**
 *
 * <p>
 * Helper class for lists of {@link ReasonerQueryImpl} queries with equality comparison {@link ReasonerQueryEquivalence}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class QueryList extends QueryCollection<List<ReasonerQueryImpl>, List<Equivalence.Wrapper<ReasonerQueryImpl>>> {

    private QueryList(){
        this.collection = new ArrayList<>();
        this.wrappedCollection = new ArrayList<>();
    }

    QueryList(Collection<ReasonerQueryImpl> queries){
        this.collection = new ArrayList<>(queries);
        this.wrappedCollection = queries.stream().map(q -> equality().wrap(q)).collect(Collectors.toList());
    }

    /**
     * @return refined list of queries based on {@link ResolutionQueryPlan} priority function
     */
    public QueryList refine(){
        QueryList plan = new QueryList();
        Stack<ReasonerQueryImpl> queryStack = new Stack<>();

        Lists.reverse(prioritise(this)).forEach(queryStack::push);
        while(!plan.containsAll(this)) {
            ReasonerQueryImpl query = queryStack.pop();

            QuerySet candidates = this.getCandidates(query, plan);

            if (!candidates.isEmpty() || this.size() - plan.size() == 1){
                plan.add(query);
                Lists.reverse(prioritise(candidates)).forEach(queryStack::push);
            }
        }

        return plan;
    }
}
