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
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.BinaryBase;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.graql.internal.reasoner.Utility.uncapture;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
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

    @Override
    public ReasonerQuery copy() {
        return new ReasonerQueryImpl(this);
    }

    //alpha-equivalence equality
    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ReasonerQueryImpl a2 = (ReasonerQueryImpl) obj;
        return this.isEquivalent(a2);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        atomSet.forEach(atom -> hashes.add(atom.equivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    @Override
    public String toString() {
        return getPattern().toString();
    }

    private void inferTypes() {
        getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .forEach(Atom::inferTypes);
    }

    public GraknGraph graph() {
        return graph;
    }

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
    @Override
    public boolean isRuleResolvable() {
        Iterator<Atom> it = atomSet.stream().filter(Atomic::isAtom).map(at -> (Atom) at).iterator();
        while (it.hasNext()) {
            if (it.next().isRuleResolvable()) {
                return true;
            }
        }
        return false;
    }

    private boolean isTransitive() {
        return getAtoms().stream().filter(this::containsEquivalentAtom).count() == 2;
    }

    private boolean isAtomic() {
        return selectAtoms().size() == 1;
    }

    /**
     * @return atom that should be prioritised for resolution
     */
    private Atom getTopAtom() {
        Set<Atom> atoms = selectAtoms();
        Set<Atom> subbedAtoms = atoms.stream().filter(Atom::hasSubstitution).collect(Collectors.toSet());
        if (!subbedAtoms.isEmpty()) return subbedAtoms.iterator().next();

        //order resources by number of value predicates in descdending order
        List<Resource> resources = atoms.stream()
                .filter(Atom::isResource).map(at -> (Resource) at)
                .sorted(Comparator.comparing(r -> -r.getMultiPredicate().size()))
                .collect(Collectors.toList());
        if (!resources.isEmpty()) return resources.iterator().next();

        Set<Atom> relations = atoms.stream()
                .filter(Atom::isRelation)
                .filter(at -> !at.isRuleResolvable())
                .collect(Collectors.toSet());

        if (!relations.isEmpty()) return relations.iterator().next();

        return selectAtoms().stream().findFirst().orElse(null);

    }

    /**
     * @return atom set constituting this query
     */
    @Override
    public Set<Atomic> getAtoms() {
        return Sets.newHashSet(atomSet);
    }

    /**
     * @return set of id predicates contained in this query
     */
    public Set<IdPredicate> getIdPredicates() {
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(Predicate::isIdPredicate).map(predicate -> (IdPredicate) predicate)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates contained in this query
     */
    public Set<ValuePredicate> getValuePredicates() {
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(Predicate::isValuePredicate).map(at -> (ValuePredicate) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of atoms constituting constraints (by means of types) for this atom
     */
    public Set<TypeAtom> getTypeConstraints() {
        return getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isType).map(at -> (TypeAtom) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of filter atoms (currently only NotEquals) contained in this query
     */
    public Set<NotEquals> getFilters() {
        return getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of variables appearing in this query
     */
    @Override
    public Set<VarName> getVarNames() {
        Set<VarName> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    /**
     * @param atom in question
     * @return true if query contains an equivalent atom
     */
    public boolean containsEquivalentAtom(Atomic atom) {
        return !getEquivalentAtoms(atom).isEmpty();
    }

    Set<Atomic> getEquivalentAtoms(Atomic atom) {
        return atomSet.stream().filter(at -> at.isEquivalent(atom)).collect(Collectors.toSet());
    }

    private void exchangeRelVarNames(VarName from, VarName to) {
        unify(to, VarName.of("temp"));
        unify(from, to);
        unify(VarName.of("temp"), from);
    }

    @Override
    public Unifier getUnifier(ReasonerQuery parent) {
        throw new IllegalStateException("Attempted to obtain unifiers on non-atomic queries.");
    }

    /**
     * change each variable occurrence in the query (apply unifier [from/to])
     *
     * @param from variable name to be changed
     * @param to   new variable name
     */
    @Override
    public void unify(VarName from, VarName to) {
        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream().filter(atom -> atom.getVarNames().contains(from)).forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(new UnifierImpl(ImmutableMap.of(from, to))));
        toAdd.forEach(this::addAtom);
    }

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * @param unifier (variable mappings) to be applied
     */
    @Override
    public void unify(Unifier unifier) {
        if (unifier.size() == 0) return;
        Unifier mappings = new UnifierImpl(unifier);
        Unifier appliedMappings = new UnifierImpl();
        //do bidirectional mappings if any
        for (Map.Entry<VarName, VarName> mapping: mappings.getMappings()) {
            VarName varToReplace = mapping.getKey();
            VarName replacementVar = mapping.getValue();
            //bidirectional mapping
            if (!replacementVar.equals(appliedMappings.get(varToReplace)) && varToReplace.equals(mappings.get(replacementVar))) {
                exchangeRelVarNames(varToReplace, replacementVar);
                appliedMappings.addMapping(varToReplace, replacementVar);
                appliedMappings.addMapping(replacementVar, varToReplace);
            }
        }
        mappings.getMappings().removeIf(e ->
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

        //NB:captures not resolved in place as resolution in-place alters respective atom hash
        mappings.merge(resolveCaptures());
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     *
     * @return new mappings resulting from capture resolution
     */
    private Unifier resolveCaptures() {
        Unifier newMappings = new UnifierImpl();
        //find and resolve captures
        // TODO: This could cause bugs if a user has a variable including the word "capture"
        getVarNames().stream().filter(Utility::isCaptured)
                .forEach(cap -> {
                    VarName old = uncapture(cap);
                    VarName fresh = VarName.anon();
                    unify(cap, fresh);
                    newMappings.addMapping(old, fresh);
                });
        return newMappings;
    }

    /**
     * @return corresponding MatchQuery
     */
    @Override
    public MatchQuery getMatchQuery() {
        return graph.graql().infer(false).match(getPattern());
    }

    /**
     * @return map of variable name - type pairs
     */
    @Override
    public Map<VarName, Type> getVarTypeMap() {
        Map<VarName, Type> map = new HashMap<>();
        getTypeConstraints().stream()
                .filter(at -> Objects.nonNull(at.getType()))
                .forEach(atom -> map.putIfAbsent(atom.getVarName(), atom.getType()));
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
        if (atomSet.add(atom)) {
            atom.setParentQuery(this);
            return true;
        } else return false;
    }

    public Answer getSubstitution(){
        Set<IdPredicate> predicates = this.getTypeConstraints().stream()
                        .map(TypeAtom::getPredicate)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        predicates.addAll(getIdPredicates());
        return new QueryAnswer(predicates.stream()
                .collect(Collectors.toMap(IdPredicate::getVarName, p -> graph().getConcept(p.getPredicate())))
        );
    }

    public boolean addSubstitution(Answer sub){
        Set<VarName> varNames = getVarNames();

        //skip predicates from types
        getTypeConstraints().stream().map(BinaryBase::getValueVariable).forEach(varNames::remove);

        Set<IdPredicate> predicates = sub.entrySet().stream()
                .filter(e -> varNames.contains(e.getKey()))
                .map(e -> new IdPredicate(e.getKey(), e.getValue(), this))
                .collect(Collectors.toSet());
        return atomSet.addAll(predicates);
    }

    public boolean hasFullSubstitution(){
        return getSubstitution().keySet().containsAll(getVarNames());
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

    private Stream<Answer> fullJoin(Set<ReasonerAtomicQuery> subGoals,
                                    Cache<ReasonerAtomicQuery, ?> cache,
                                    Cache<ReasonerAtomicQuery, ?> dCache,
                                    boolean materialise,
                                    boolean explanation){
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Iterator<ReasonerAtomicQuery> qit = queries.iterator();
        ReasonerAtomicQuery childAtomicQuery = qit.next();
        Stream<Answer> join = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, explanation, false);
        Set<VarName> joinedVars = childAtomicQuery.getVarNames();
        while(qit.hasNext()){
            childAtomicQuery = qit.next();
            Set<VarName> joinVars = Sets.intersection(joinedVars, childAtomicQuery.getVarNames());
            Stream<Answer> localSubs = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, explanation, false);
            join = join(join, localSubs, ImmutableSet.copyOf(joinVars), explanation);
            joinedVars.addAll(childAtomicQuery.getVarNames());
        }
        return join;
    }

    private Stream<Answer> differentialJoin(Set<ReasonerAtomicQuery> subGoals,
                                            Cache<ReasonerAtomicQuery, ?> cache,
                                            Cache<ReasonerAtomicQuery, ?> dCache,
                                            boolean materialise,
                                            boolean explanation){
        Stream<Answer> join = Stream.empty();
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Set<ReasonerAtomicQuery> uniqueQueries = queries.stream().collect(Collectors.toSet());
        //only do one join for transitive queries
        List<ReasonerAtomicQuery> queriesToJoin  = isTransitive()? Lists.newArrayList(uniqueQueries) : queries;

        for(ReasonerAtomicQuery qi : queriesToJoin){
            Stream<Answer> subs = qi.answerStream(subGoals, cache, dCache, materialise, explanation, true);
            Set<VarName> joinedVars = qi.getVarNames();
            for(ReasonerAtomicQuery qj : queries){
                if ( qj != qi ){
                    Set<VarName> joinVars = Sets.intersection(joinedVars, qj.getVarNames());
                    subs = join(subs, cache.getAnswerStream(qj), ImmutableSet.copyOf(joinVars), explanation);
                    joinedVars.addAll(qj.getVarNames());
                }
            }
            join = Stream.concat(join, subs);
        }
        return join;
    }

    Stream<Answer> computeJoin(Set<ReasonerAtomicQuery> subGoals,
                               Cache<ReasonerAtomicQuery, ?> cache,
                               Cache<ReasonerAtomicQuery, ?> dCache,
                               boolean materialise,
                               boolean explanation,
                               boolean differentialJoin) {
        Stream<Answer> join = differentialJoin?
                differentialJoin(subGoals, cache, dCache, materialise, explanation) :
                fullJoin(subGoals, cache, dCache, materialise, explanation);

        Set<NotEquals> filters = getFilters();
        return join
                .filter(a -> nonEqualsFilter(a, filters));
    }

    @Override
    public Stream<Answer> resolve(boolean materialise, boolean explanation) {
        //TODO temporary switch
        if (materialise || isAtomic()) {
            return resolve(materialise, explanation, new LazyQueryCache<>(explanation), new LazyQueryCache<>(explanation));
        } else {
            return resolve();
        }
    }

    /**
     * resolves the query
     * @param materialise materialisation flag
     * @return stream of answers
     */
    public Stream<Answer> resolve(boolean materialise, boolean explanation, LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache) {
        if (!this.isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames()
                    .map(QueryAnswer::new);
        }

        Iterator<Atom> atIt = this.selectAtoms().iterator();
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(atIt.next());
        Stream<Answer> answerStream = atomicQuery.resolve(materialise, explanation, cache, dCache);
        Set<VarName> joinedVars = atomicQuery.getVarNames();
        while (atIt.hasNext()) {
            atomicQuery = new ReasonerAtomicQuery(atIt.next());
            Stream<Answer> subAnswerStream = atomicQuery.resolve(materialise, explanation, cache, dCache);
            Set<VarName> joinVars = Sets.intersection(joinedVars, atomicQuery.getVarNames());
            answerStream = join(answerStream, subAnswerStream, ImmutableSet.copyOf(joinVars), explanation);
            joinedVars.addAll(atomicQuery.getVarNames());
        }

        Set<NotEquals> filters = this.getFilters();
        Set<VarName> vars = this.getVarNames();
        return answerStream
                .filter(a -> nonEqualsFilter(a, filters))
                .flatMap(a -> varFilterFunction.apply(a, vars));
    }

    public Stream<Answer> resolve() {
        if (!this.isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames().map(QueryAnswer::new);
        }
        return new ReasonerQueryImplIterator().hasStream();
    }


    public Iterator<Answer> iterator(Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        return new ReasonerQueryImplIterator(subGoals, cache);
    }

    private class ReasonerQueryImplIterator implements Iterator<Answer> {

        private final Answer partialSubstitution;
        private final QueryCache<ReasonerAtomicQuery> cache;
        private final Set<ReasonerAtomicQuery> subGoals;

        private Iterator<Answer> queryIterator = Collections.emptyIterator();
        private final Iterator<Answer> atomicQueryIterator;

        ReasonerQueryImplIterator(){ this(new HashSet<>(), new QueryCache<>());}
        ReasonerQueryImplIterator(Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
            this.partialSubstitution = getSubstitution();
            this.subGoals = subGoals;
            this.cache = cache;

            //get prioritised atom and construct atomic query from it
            ReasonerAtomicQuery q = new ReasonerAtomicQuery(getTopAtom());
            atomicQueryIterator = q.iterator(subGoals, cache);
        }

        Stream<Answer> hasStream(){
            Iterable<Answer> iterable = () -> this;
            return StreamSupport.stream(iterable.spliterator(), false).distinct();
        }

        @Override
        public boolean hasNext() {
            if (queryIterator.hasNext()) return true;
            else {
                if (atomicQueryIterator.hasNext()) {
                    Answer sub = atomicQueryIterator.next();
                    queryIterator = getQueryPrime(sub).iterator(subGoals, cache);
                    return hasNext();
                }
                else return false;
            }
        }

        @Override
        public Answer next() {
            Answer sub = queryIterator.next();
            return sub.merge(partialSubstitution);
        }

        private ReasonerQueryImpl getQueryPrime(Answer sub){
            //construct new reasoner query with the top atom removed
            ReasonerQueryImpl newQuery = new ReasonerQueryImpl(ReasonerQueryImpl.this);
            newQuery.removeAtom(newQuery.getTopAtom());
            newQuery.addSubstitution(sub);

            return newQuery.isAtomic()?
                    new ReasonerAtomicQuery(newQuery.selectAtoms().iterator().next()) :
                    newQuery;
        }

    }
}
