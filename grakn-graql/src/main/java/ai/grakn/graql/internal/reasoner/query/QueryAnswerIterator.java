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
package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * Iterator for query answers maintaining the iterative behaviour of QSQ scheme.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswerIterator implements Iterator<Map<VarName, Concept>> {

    private final ReasonerAtomicQuery query;
    final private QueryAnswers answers = new QueryAnswers();

    private long oldAns = 0;
    private int iter = 0;
    private final boolean materialise;
    private final Set<ReasonerAtomicQuery> subGoals = new HashSet<>();
    private final LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
    private Iterator<Map<VarName, Concept>> answerIterator;
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerAtomicQuery.class);

    public QueryAnswerIterator(ReasonerAtomicQuery q, boolean materialise){
        this.query = q;
        this.materialise = materialise;
        this.answerIterator = query.answerStream(subGoals, cache, materialise).iterator();
    }

    /**
     * @return stream constructed out of the answer iterator
     */
    public Stream<Map<VarName, Concept>> hasStream(){
        Iterable<Map<VarName, Concept>> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false).distinct();
    }

    private void computeNext(){
        oldAns = answerSize();
        iter++;
        subGoals.clear();
        answerIterator = query.answerStream(subGoals, cache, materialise).iterator();
    }

    /**
     * check whether answers available, if answers not fully computed compute more answers
     * @return true if answers available
     */
    @Override
    public boolean hasNext() {
        if (answerIterator.hasNext()) return true;
        //iter finished
        else {
            long dAns = answerSize() - oldAns;
            if (dAns != 0 || iter == 0) {
                LOG.debug("Atom: " + query.getAtom() + " iter: " + iter + " answers: " + answers.size() + " dAns = " + dAns);
                computeNext();
                return answerIterator.hasNext();
            }
            else return false;
        }
    }

    /**
     * @return single answer to the query
     */
    @Override
    public Map<VarName, Concept> next() {
        Map<VarName, Concept> answer = answerIterator.next();
        answers.add(answer);
        return answer;
    }

    private long answerSize(){
        return cache.answerSize(subGoals);
    }
}