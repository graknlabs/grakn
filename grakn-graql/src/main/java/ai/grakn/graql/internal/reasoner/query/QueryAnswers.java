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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.Binary;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.Utility.getListPermutations;
import static ai.grakn.graql.internal.reasoner.Utility.getUnifiersFromPermutations;

/**
 *
 * <p>
 * Wrapper class for a set of answers providing higher level filtering facilities
 * as well as unification and join operations.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswers extends HashSet<Map<VarName, Concept>> {

    public QueryAnswers(){super();}
    public QueryAnswers(Collection<? extends Map<VarName, Concept>> ans){ super(ans);}

    public Set<VarName> getVars(){
        Optional<Map<VarName, Concept>> map = this.stream().findFirst();
        return map.isPresent()? map.get().keySet() : new HashSet<>();
    }

    public QueryAnswers permute(Atom atom, Atom headAtom){
        if (!(atom.isRelation() && headAtom.isRelation())) return this;
        List<VarName> permuteVars = new ArrayList<>();
        //if atom is match all atom, add type from rule head and find unmapped roles
        Relation relAtom = atom.getValueVariable().getValue().isEmpty()?
                ((Relation) AtomicFactory.create(atom, atom.getParentQuery())).addType(headAtom.getType()) :
                (Relation) atom;
        relAtom.getUnmappedRolePlayers().forEach(permuteVars::add);

        List<List<VarName>> varPermutations = getListPermutations(new ArrayList<>(permuteVars));
        Set<Map<VarName, VarName>> unifierSet = getUnifiersFromPermutations(permuteVars, varPermutations);
        QueryAnswers permutedAnswers = new QueryAnswers();
        unifierSet.forEach(unifiers -> permutedAnswers.addAll(this.unify(unifiers)));

        //filter by checking substitutions
        Set<IdPredicate> subs = atom.getIdPredicates().stream()
                .filter(pred -> permuteVars.contains(pred.getVarName()))
                .collect(Collectors.toSet());
        QueryAnswers filteredOutAnswers = new QueryAnswers();
        subs.forEach( sub -> permutedAnswers.stream()
                .filter(answer -> !answer.get(sub.getVarName()).getId().equals(sub.getPredicate()))
                .forEach(filteredOutAnswers::add));
        permutedAnswers.removeAll(filteredOutAnswers);
        return permutedAnswers;
    }

    /**
     * filter answers by constraining the variable set to the provided one
     * @param vars set of variable names
     * @return filtered answers
     */
    public QueryAnswers filterVars(Set<VarName> vars) {
        return new QueryAnswers(this.stream().map(result -> Maps.filterKeys(result, vars::contains)).collect(Collectors.toSet()));
    }

    /**
     * filter answers by discarding the already known ones
     * @param known set of known answers
     * @return filtered answers
     */
    public QueryAnswers filterKnown(QueryAnswers known){
        if (this.getVars().equals(known.getVars()) || known.isEmpty() ){
            QueryAnswers results = new QueryAnswers(this);
            results.removeAll(known);
            return results;
        }
        QueryAnswers results = new QueryAnswers();
        this.forEach(answer ->{
            boolean isKnown = false;
            Iterator<Map<VarName, Concept>> it = known.iterator();
            while(it.hasNext() && !isKnown) {
                Map<VarName, Concept> knownAnswer = it.next();
                isKnown = knownAnswer.entrySet().containsAll(answer.entrySet());
            }
            if (!isKnown) results.add(answer);
        });
        return results;
    }

    /**
     * filter answers by applying NonEquals filters
     * @param query query containing filters
     * @return filtered answers
     */
    public QueryAnswers filterNonEquals(ReasonerQueryImpl query){
        Set<NotEquals> filters = query.getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
                .collect(Collectors.toSet());
        if(filters.isEmpty()) return this;
        QueryAnswers results = new QueryAnswers(this);
        for (NotEquals filter : filters) results = filter.filter(results);
        return results;
    }

    /**
     * perform a join operation between this and provided answers
     * @param localTuples right operand of join operation
     * @return joined answers
     */
    public QueryAnswers join(QueryAnswers localTuples) {
        if (this.isEmpty() || localTuples.isEmpty()) {
            return new QueryAnswers();
        }
        QueryAnswers join = new QueryAnswers();
        Set<VarName> joinVars = new HashSet<>(this.getVars());
        joinVars.retainAll(localTuples.getVars());

        for( Map<VarName, Concept> lanswer : localTuples){
            for (Map<VarName, Concept> answer : this){
                boolean isCompatible = true;
                Iterator<VarName> vit = joinVars.iterator();
                while(vit.hasNext() && isCompatible) {
                    VarName var = vit.next();
                    isCompatible = answer.get(var).equals(lanswer.get(var));
                }

                if (isCompatible) {
                    Map<VarName, Concept> merged = new HashMap<>(lanswer);
                    merged.putAll(answer);
                    join.add(merged);
                }
            }
        }
        return join;
    }

    /**
     * unify the answers by applying unifiers to variable set
     * @param unifiers map of [key: from/value: to] unifiers
     * @return unified query answers
     */
    public QueryAnswers unify(Map<VarName, VarName> unifiers){
        return unify(unifiers, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    private QueryAnswers unify(Map<VarName, VarName> unifiers, Map<VarName, Concept> subVars,
                               Map<VarName, Concept> valueConstraints, Map<VarName, String> typeConstraints){
        if (unifiers.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.forEach(entry -> {
            Map<VarName, Concept> answer = new HashMap<>(subVars);
            boolean isCompatible = true;
            Iterator<Map.Entry<VarName, Concept>> it = entry.entrySet().iterator();
            while (it.hasNext() && isCompatible) {
                Map.Entry<VarName, Concept> elem = it.next();
                VarName var = elem.getKey();
                Concept con = elem.getValue();
                if (unifiers.containsKey(var)) var = unifiers.get(var);
                if ( ( valueConstraints.containsKey(var) && !valueConstraints.get(var).equals(con) ) ||
                        ( typeConstraints.containsKey(var) && !typeConstraints.get(var).equals(con.getId().getValue()) ) ) {
                    isCompatible = false;
                } else {
                    answer.put(var, con);
                }
            }
            if (isCompatible && !answer.isEmpty()) {
                unifiedAnswers.add(answer);
            }
        });

        return unifiedAnswers;
    }

    /**
     * unify answers of childQuery with parentQuery
     * @param parentQuery parent atomic query containing target variables
     * @return unified answers
     */
    public static QueryAnswers getUnifiedAnswers(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery, QueryAnswers answers){
        if (parentQuery == childQuery) return new QueryAnswers(answers);
        GraknGraph graph = childQuery.graph();
        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);

        //identify extra subs contribute to/constraining answers
        Map<VarName, Concept> subVars = new HashMap<>();
        Map<VarName, Concept> valueConstraints = new HashMap<>();
        Map<VarName, String> typeConstraints = new HashMap<>();

        //find extra type constraints
        Set<TypeAtom> extraTypes =  Utility.subtractSets(parentQuery.getTypeConstraints(), childQuery.getTypeConstraints());
        extraTypes.removeAll(childQuery.getTypeConstraints());
        extraTypes.stream().map(t -> (Binary) t).forEach(type -> {
            Predicate predicate = parentQuery.getIdPredicate(type.getValueVariable());
            if (predicate != null) typeConstraints.put(type.getVarName(), predicate.getPredicateValue());
        });

        //find extra subs
        if (parentQuery.getVarNames().size() != childQuery.getVarNames().size()){
            //get |child - parent| set difference
            Set<IdPredicate> extraSubs = Utility.subtractSets(parentQuery.getIdPredicates(), childQuery.getIdPredicates());
            extraSubs.forEach( sub -> {
                VarName var = sub.getVarName();
                Concept con = graph.getConcept(sub.getPredicate());
                if (unifiers.containsKey(var)) var = unifiers.get(var);
                if (childQuery.getVarNames().size() > parentQuery.getVarNames().size()) {
                    valueConstraints.put(var, con);
                } else {
                    subVars.put(var, con);
                }
            });
        }

        QueryAnswers unifiedAnswers = answers.unify(unifiers, subVars, valueConstraints, typeConstraints);
        return unifiedAnswers.filterVars(parentQuery.getVarNames());
    }
}
