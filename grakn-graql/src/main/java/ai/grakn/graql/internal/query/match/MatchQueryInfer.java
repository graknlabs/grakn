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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.util.ErrorMessage;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.optionalOr;

/**
 * Modifier that specifies the graph to execute the match query with.
 */
class MatchQueryInfer extends MatchQueryModifier {

    private final boolean materialise;

    MatchQueryInfer(AbstractMatchQuery inner, boolean materialise) {
        super(inner);
        this.materialise = materialise;
    }

    @Override
    public Stream<Answer> stream(Optional<GraknGraph> optionalGraph) {
        GraknGraph graph = optionalOr(optionalGraph, inner.getGraph()).orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );

        if (!ReasonerUtils.hasRules(graph)) return inner.stream(optionalGraph);

        Iterator<Conjunction<VarAdmin>> conjIt = getPattern().getDisjunctiveNormalForm().getPatterns().iterator();
        Conjunction<VarAdmin> conj = conjIt.next();
        ReasonerQueryImpl conjQuery = ReasonerQueries.create(conj, graph);
        Stream<Answer> answerStream = conjQuery.isRuleResolvable()? conjQuery.resolve(materialise, true) : graph.graql().match(conj).stream();
        while(conjIt.hasNext()) {
            conj = conjIt.next();
            conjQuery = ReasonerQueries.create(conj, graph);
            Stream<Answer> localStream = conjQuery.isRuleResolvable()? conjQuery.resolve(materialise, true) : graph.graql().match(conj).stream();
            answerStream = Stream.concat(answerStream, localStream);
        }
        return answerStream.map(result -> result.filterVars(getSelectedNames()));
    }

    @Override
    protected String modifierString() {
        return "";
    }
}
