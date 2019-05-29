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

package grakn.core.graql.reasoner.query;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.unifier.MultiUnifier;
import grakn.core.graql.reasoner.unifier.Unifier;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper class for a set of ConceptMap objects providing higher level facilities.
 *
 *
 */
public class QueryAnswers implements Iterable<ConceptMap>{

    private final HashSet<ConceptMap> set = new HashSet<>();

    @Nonnull
    @Override
    public Iterator<ConceptMap> iterator() { return set.iterator();}

    @Override
    public boolean equals(Object obj){
        if (obj == this) return true;
        if (obj == null || !(obj instanceof QueryAnswers)) return false;
        QueryAnswers a2 = (QueryAnswers) obj;
        return set.equals(a2.set);
    }

    @Override
    public int hashCode(){return set.hashCode();}

    @Override
    public String toString(){ return set.toString();}

    public Stream<ConceptMap> stream(){ return set.stream();}

    public QueryAnswers(){}
    private QueryAnswers(QueryAnswers ans){ ans.forEach(set::add);}

    public boolean add(ConceptMap a){ return set.add(a);}
    public boolean addAll(QueryAnswers ans){ return set.addAll(ans.set);}

    public boolean removeAll(QueryAnswers ans){ return set.removeAll(ans.set);}

    public boolean contains(ConceptMap a){ return set.contains(a);}
    public boolean isEmpty(){ return set.isEmpty();}

    /**
     * unify the answers by applying unifier to variable set
     * @param unifier map of [key: from/value: to] unifiers
     * @return unified query answers
     */
    public QueryAnswers unify(Unifier unifier){
        if (unifier.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.stream()
            .map(unifier::apply)
            .filter(a -> !a.isEmpty())
            .forEach(unifiedAnswers::add);
        return unifiedAnswers;
    }

    /**
     * unify the answers by applying multiunifier to variable set
     * @param multiUnifier multiunifier to be applied to the query answers
     * @return unified query answers
     */
    public QueryAnswers unify(MultiUnifier multiUnifier){
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.stream()
                .flatMap(multiUnifier::apply)
                .filter(ans -> !ans.isEmpty())
                .forEach(unifiedAnswers::add);
        return unifiedAnswers;
    }

}
