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
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.Utility.isCaptured;
import static ai.grakn.graql.internal.reasoner.Utility.uncapture;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.joinWithInverse;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.nonEqualsFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;

/**
 *
 * <p>
 * Base reasoner query providing resolution and atom handling facilities for conjunctive graql queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerQueryImpl implements ReasonerQuery {

    private final GraknGraph graph;
    private final Set<Atomic> atomSet = new HashSet<>();

    public ReasonerQueryImpl(Conjunction<VarAdmin> pattern, GraknGraph graph) {
        this.graph = graph;
        atomSet.addAll(AtomicFactory.createAtomSet(pattern, this));
        inferTypes();
    }

    public ReasonerQueryImpl(ReasonerQueryImpl q) {
        this.graph = q.graph;
        q.getAtoms().forEach(at -> addAtom(AtomicFactory.create(at, this)));
        inferTypes();
    }

    protected ReasonerQueryImpl(Atom atom) {
        if (atom.getParentQuery() == null) {
            throw new IllegalArgumentException(ErrorMessage.PARENT_MISSING.getMessage(atom.toString()));
        }
        this.graph = atom.getParentQuery().graph();
        addAtom(AtomicFactory.create(atom, this));
        addAtomConstraints(atom);
        inferTypes();
    }

    //alpha-equivalence equality
    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ReasonerQueryImpl a2 = (ReasonerQueryImpl) obj;
        return this.isEquivalent(a2);
    }

    @Override
    public int hashCode(){
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        atomSet.forEach(atom -> hashes.add(atom.equivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    @Override
    public String toString() { return getMatchQuery().toString();}

    private void inferTypes(){
        getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .forEach(Atom::inferTypes);
    }

    public GraknGraph graph(){ return graph;}

    public Conjunction<PatternAdmin> getPattern() {
        Set<PatternAdmin> patterns = new HashSet<>();
        atomSet.stream()
                .map(Atomic::getCombinedPattern)
                .flatMap(p -> p.getVars().stream())
                .forEach(patterns::add);
        return Patterns.conjunction(patterns);
    }

    /**
     * @return true if any of the atoms constituting the query can be resolved through a rule
     */
    public boolean isRuleResolvable(){
        Iterator<Atom> it = atomSet.stream().filter(Atomic::isAtom).map(at -> (Atom) at).iterator();
        while(it.hasNext()) {
            if (it.next().isRuleResolvable()){
                return true;
            }
        }
        return false;
    }

    private boolean isTransitive(){
        return getAtoms().stream().filter(this::containsEquivalentAtom).count() == 2;
    }

    /**
     * @return atom set constituting this query
     */
    public Set<Atomic> getAtoms() { return Sets.newHashSet(atomSet);}

    /**
     * @return set of id predicates contained in this query
     */
    public Set<IdPredicate> getIdPredicates(){
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(Predicate::isIdPredicate).map(predicate -> (IdPredicate) predicate)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates contained in this query
     */
    public Set<ValuePredicate> getValuePredicates(){
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(Predicate::isValuePredicate).map(at -> (ValuePredicate) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of atoms constituting constraints (by means of types) for this atom
     */
    public Set<TypeAtom> getTypeConstraints(){
        return getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isType).map(at -> (TypeAtom) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of filter atoms (currently only NotEquals) contained in this query
     */
    public Set<NotEquals> getFilters(){
        return getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of variables appearing in this query
     */
    public Set<VarName> getVarNames() {
        Set<VarName> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    /**
     * @param atom in question
     * @return true if query contains an equivalent atom
     */
    public boolean containsEquivalentAtom(Atomic atom){
        return !getEquivalentAtoms(atom).isEmpty();
    }

    public Set<Atomic> getEquivalentAtoms(Atomic atom){
        return atomSet.stream().filter(at -> at.isEquivalent(atom)).collect(Collectors.toSet());
    }

    private void exchangeRelVarNames(VarName from, VarName to){
        unify(to, VarName.of("temp"));
        unify(from, to);
        unify(VarName.of("temp"), from);
    }

    @Override
    public Map<VarName, VarName> getUnifiers(ReasonerQuery parent) {
        //TODO
        return new HashMap<>();
    }

    /**
     * change each variable occurrence in the query (apply unifier [from/to])
     * @param from variable name to be changed
     * @param to new variable name
     */
    public void unify(VarName from, VarName to) {
        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream().filter(atom -> atom.getVarNames().contains(from)).forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(ImmutableMap.of(from, to)));
        toAdd.forEach(this::addAtom);
    }

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * @param unifiers contain unifiers (variable mappings) to be applied
     */
    public void unify(Map<VarName, VarName> unifiers) {
        if (unifiers.size() == 0) return;
        Map<VarName, VarName> mappings = new HashMap<>(unifiers);
        Map<VarName, VarName> appliedMappings = new HashMap<>();
        //do bidirectional mappings if any
        for (Map.Entry<VarName, VarName> mapping: mappings.entrySet()) {
            VarName varToReplace = mapping.getKey();
            VarName replacementVar = mapping.getValue();
            //bidirectional mapping
            if (!replacementVar.equals(appliedMappings.get(varToReplace)) && varToReplace.equals(mappings.get(replacementVar))) {
                exchangeRelVarNames(varToReplace, replacementVar);
                appliedMappings.put(varToReplace, replacementVar);
                appliedMappings.put(replacementVar, varToReplace);
            }
        }
        mappings.entrySet().removeIf(e ->
                appliedMappings.containsKey(e.getKey()) && appliedMappings.get(e.getKey()).equals(e.getValue()));

        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream()
                .filter(atom -> {
                    Set<VarName> keyIntersection = atom.getVarNames();
                    Set<VarName> valIntersection = atom.getVarNames();
                    keyIntersection.retainAll(mappings.keySet());
                    valIntersection.retainAll(mappings.values());
                    return (!keyIntersection.isEmpty() || !valIntersection.isEmpty());
                })
                .forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(mappings));
        toAdd.forEach(this::addAtom);

        mappings.putAll(resolveCaptures());
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     * @return new mappings resulting from capture resolution
     */
    private Map<VarName, VarName> resolveCaptures() {
        Map<VarName, VarName> newMappings = new HashMap<>();
        //find captures
        Set<VarName> captures = new HashSet<>();
        getVarNames().forEach(v -> {
            // TODO: This could cause bugs if a user has a variable including the word "capture"
            if (isCaptured(v)) captures.add(v);
        });

        captures.forEach(cap -> {
            VarName old = uncapture(cap);
            VarName fresh = Utility.createFreshVariable(getVarNames(), old);
            unify(cap, fresh);
            newMappings.put(old, fresh);
        });
        return newMappings;
    }

    /**
     * @return corresponding MatchQuery
     */
    public MatchQuery getMatchQuery() {
        return graph.graql().infer(false).match(getPattern());
    }

    /**
     * @return map of variable name - type pairs
     */
    public Map<VarName, Type> getVarTypeMap() {
        Map<VarName, Type> map = new HashMap<>();
        getTypeConstraints().forEach(atom -> map.putIfAbsent(atom.getVarName(), atom.getType()));
        return map;
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
    public IdPredicate getIdPredicate(VarName var) {
        Set<IdPredicate> relevantSubs = getIdPredicates().stream()
                .filter(sub -> sub.getVarName().equals(var))
                .collect(Collectors.toSet());
        return relevantSubs.isEmpty() ? null : relevantSubs.iterator().next();
    }

    /**
     * @param atom to be added
     * @return true if the atom set did not already contain the specified atom
     */
    public boolean addAtom(Atomic atom) {
        if(atomSet.add(atom)) {
            atom.setParentQuery(this);
            return true;
        }
        else return false;
    }

    /**
     * @param atom to be removed
     * @return true if the atom set contained the specified atom
     */
    public boolean removeAtom(Atomic atom) {return atomSet.remove(atom);}

    private void addAtomConstraints(Atom atom){
        addAtomConstraints(atom.getPredicates());
        Set<Atom> types = atom.getTypeConstraints().stream()
                .filter(at -> !at.isSelectable())
                .filter(at -> !at.isRuleResolvable())
                .collect(Collectors.toSet());
        addAtomConstraints(types);
    }

    /**
     * adds a set of constraints (types, predicates) to the atom set
     * @param cstrs set of constraints
     */
    public void addAtomConstraints(Set<? extends Atomic> cstrs){
        cstrs.forEach(con -> addAtom(AtomicFactory.create(con, this)));
    }

    private Atom findFirstJoinable(Set<Atom> atoms){
        for (Atom next : atoms) {
            Atom atom = findNextJoinable(Sets.difference(atoms, Sets.newHashSet(next)), next.getVarNames());
            if (atom != null) return atom;
        }
        return atoms.iterator().next();
    }

    private Atom findNextJoinable(Set<Atom> atoms, Set<VarName> vars){
        for (Atom next : atoms) {
            if (!Sets.intersection(vars, next.getVarNames()).isEmpty()) return next;
        }
        return null;
    }

    public Atom findNextJoinable(Atom atom){
        Set<Atom> atoms = getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(at -> at != atom)
                .collect(Collectors.toSet());
        return findNextJoinable(atoms, atom.getVarNames());
    }

    /**
     * atom selection function
     * @return selected atoms
     */
    public Set<Atom> selectAtoms() {
        Set<Atom> atoms = new HashSet<>(atomSet).stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        if (atoms.size() == 1) return atoms;

        //pass relations or rule-resolvable types and resources
        Set<Atom> atomsToSelect = atoms.stream()
                .filter(Atomic::isSelectable)
                .collect(Collectors.toSet());

        Set<Atom> orderedSelection = new LinkedHashSet<>();

        Atom atom = findFirstJoinable(atomsToSelect);
        Set<VarName> joinedVars = new HashSet<>();
        while(!atomsToSelect.isEmpty() && atom != null) {
            orderedSelection.add(atom);
            atomsToSelect.remove(atom);
            joinedVars.addAll(atom.getVarNames());
            atom = findNextJoinable(atomsToSelect, joinedVars);
        }
        //if disjoint select at random
        if (!atomsToSelect.isEmpty()) atomsToSelect.forEach(orderedSelection::add);

        if (orderedSelection.isEmpty()) {
            throw new IllegalStateException(ErrorMessage.NO_ATOMS_SELECTED.getMessage(this.toString()));
        }
        return orderedSelection;
    }

    /**
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    public boolean isEquivalent(ReasonerQueryImpl q) {
        Set<Atom> atoms = atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        if(atoms.size() != q.getAtoms().stream().filter(Atomic::isAtom).count()) return false;
        for (Atom atom : atoms){
            if(!q.containsEquivalentAtom(atom)){
                return false;
            }
        }
        return true;
    }

    private Stream<Map<VarName, Concept>> fullJoin(Set<ReasonerAtomicQuery> subGoals,
                                                   Cache<ReasonerAtomicQuery, ?> cache,
                                                   Cache<ReasonerAtomicQuery, ?> dCache,
                                                   boolean materialise){
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Iterator<ReasonerAtomicQuery> qit = queries.iterator();
        ReasonerAtomicQuery childAtomicQuery = qit.next();
        Stream<Map<VarName, Concept>> join = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, false);
        Set<VarName> joinedVars = childAtomicQuery.getVarNames();
        while(qit.hasNext()){
            childAtomicQuery = qit.next();
            Set<VarName> joinVars = Sets.intersection(joinedVars, childAtomicQuery.getVarNames());
            Stream<Map<VarName, Concept>> localSubs = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, false);
            join = joinWithInverse(
                    join,
                    localSubs,
                    cache.getInverseAnswerMap(childAtomicQuery, joinVars),
                    ImmutableSet.copyOf(joinVars));
            joinedVars.addAll(childAtomicQuery.getVarNames());
        }
        return join;
    }

    private Stream<Map<VarName, Concept>> differentialJoin(Set<ReasonerAtomicQuery> subGoals,
                                                           Cache<ReasonerAtomicQuery, ?> cache,
                                                           Cache<ReasonerAtomicQuery, ?> dCache,
                                                           boolean materialise){
        Stream<Map<VarName, Concept>> join = Stream.empty();
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Set<ReasonerAtomicQuery> uniqueQueries = queries.stream().collect(Collectors.toSet());
        //only do one join for transitive queries
        List<ReasonerAtomicQuery> queriesToJoin  = isTransitive()? Lists.newArrayList(uniqueQueries) : queries;

        for(ReasonerAtomicQuery qi : queriesToJoin){
            Stream<Map<VarName, Concept>> subs = qi.answerStream(subGoals, cache, dCache, materialise, true);
            Set<VarName> joinedVars = qi.getVarNames();
            for(ReasonerAtomicQuery qj : queries){
                if ( qj != qi ){
                    Set<VarName> joinVars = Sets.intersection(joinedVars, qj.getVarNames());
                    subs = joinWithInverse(
                            subs,
                            cache.getAnswerStream(qj),
                            cache.getInverseAnswerMap(qj, joinVars),
                            ImmutableSet.copyOf(joinVars));
                    joinedVars.addAll(qj.getVarNames());
                }
            }
            join = Stream.concat(join, subs);
        }
        return join.distinct();
    }

    Stream<Map<VarName, Concept>> computeJoin(Set<ReasonerAtomicQuery> subGoals,
                                              Cache<ReasonerAtomicQuery, ?> cache,
                                              Cache<ReasonerAtomicQuery, ?> dCache,
                                              boolean materialise,
                                              boolean differentialJoin) {
        if (differentialJoin){
            return differentialJoin(subGoals, cache, dCache, materialise);
        } else {
            return fullJoin(subGoals, cache, dCache, materialise);
        }
    }

    /**
     * resolves the query
     * @param materialise materialisation flag
     * @return stream of answers
     */
    @Override
    public Stream<Map<VarName, Concept>> resolve(boolean materialise) {
        if (!this.isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames();
        }
        Iterator<Atom> atIt = this.selectAtoms().iterator();
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(atIt.next());
        Stream<Map<VarName, Concept>> answerStream = atomicQuery.resolve(materialise);
        while (atIt.hasNext()) {
            atomicQuery = new ReasonerAtomicQuery(atIt.next());
            Stream<Map<VarName, Concept>> subAnswerStream = atomicQuery.resolve(materialise);
            answerStream = join(answerStream, subAnswerStream);
        }
        return answerStream
                .filter(a -> nonEqualsFilter(a, this.getFilters()))
                .flatMap(a -> varFilterFunction.apply(a, this.getVarNames()));
    }
}
