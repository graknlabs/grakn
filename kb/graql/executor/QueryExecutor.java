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

package grakn.core.kb.graql.executor;

import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.kb.graql.planning.GraqlTraversal;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlCompute;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.MatchClause;

import java.util.stream.Stream;

public interface QueryExecutor {
    Stream<ConceptMap> match(MatchClause matchClause);

    Stream<ConceptMap> traverse(Conjunction<Pattern> pattern);

    /**
     * @return resulting answer stream
     */
    Stream<ConceptMap> traverse(Conjunction<Pattern> pattern, GraqlTraversal graqlTraversal);

    ConceptMap define(GraqlDefine query);

    ConceptMap undefine(GraqlUndefine query);

    Stream<ConceptMap> insert(GraqlInsert query);

    ConceptSet delete(GraqlDelete query);

    Stream<ConceptMap> get(GraqlGet query);

    Stream<Numeric> aggregate(GraqlGet.Aggregate query);

    Stream<AnswerGroup<ConceptMap>> get(GraqlGet.Group query);

    Stream<AnswerGroup<Numeric>> get(GraqlGet.Group.Aggregate query);

    Stream<Numeric> compute(GraqlCompute.Statistics query);

    Stream<ConceptList> compute(GraqlCompute.Path query);

    Stream<ConceptSetMeasure> compute(GraqlCompute.Centrality query);

    Stream<ConceptSet> compute(GraqlCompute.Cluster query);
}
