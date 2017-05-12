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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.Objects;
import java.util.stream.Collectors;
import javafx.util.Pair;

import java.util.HashSet;
import java.util.Set;

import static ai.grakn.graql.internal.reasoner.ReasonerUtils.checkTypesCompatible;

/**
 *
 * <p>
 * {@link AtomBase} extension defining specialised functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Atom extends AtomBase {

    protected Type type = null;
    protected ConceptId typeId = null;
    protected int priority = Integer.MAX_VALUE;

    protected Atom(VarAdmin pattern, ReasonerQuery par) { super(pattern, par);}
    protected Atom(Atom a) {
        super(a);
        this.type = a.type;
        this.typeId = a.getTypeId() != null? ConceptId.of(a.getTypeId().getValue()) : null;
    }

    @Override
    public boolean isAtom(){ return true;}

    public boolean isBinary(){return false;}

    /**
     * @return true if the atom corresponds to a atom
     * */
    public boolean isType(){ return false;}

    /**
     * @return true if the atom corresponds to a non-unary atom
     * */
    public boolean isRelation(){return false;}

    /**
     * @return true if the atom corresponds to a resource atom
     * */
    public boolean isResource(){ return false;}

    /**
     * @return partial substitutions for this atom (NB: instances)
     */
    public Set<IdPredicate> getPartialSubstitutions(){ return new HashSet<>();}

    /**
     * @return measure of priority with which this atom should be resolved
     */
    public int resolutionPriority(){
        if (priority == Integer.MAX_VALUE) {
            priority = 0;
            priority += getPartialSubstitutions().size() * ResolutionStrategy.PARTIAL_SUBSTITUTION;
            priority += isRuleResolvable()? ResolutionStrategy.RULE_RESOLVABLE_ATOM : 0;
            priority += isRecursive()? ResolutionStrategy.RECURSIVE_ATOM : 0;

            priority += getTypeConstraints().size() * ResolutionStrategy.GUARD;
            Set<VarName> otherVars = getParentQuery().getAtoms().stream()
                    .filter(a -> a != this)
                    .flatMap(at -> at.getVarNames().stream())
                    .collect(Collectors.toSet());
            priority += Sets.intersection(getVarNames(), otherVars).size() * ResolutionStrategy.BOUND_VARIABLE;

        }
        return priority;
    }

    public abstract boolean isRuleApplicable(InferenceRule child);

    /**
     * @return set of potentially applicable rules - does shallow (fast) check for applicability
     */
    private Set<Rule> getPotentialRules(){
        Type type = getType();
        return type != null ?
                type.subTypes().stream().flatMap(t -> t.getRulesOfConclusion().stream()).collect(Collectors.toSet()) :
                ReasonerUtils.getRules(graph());
    }

    /**
     * @return set of applicable rules - does detailed (slow) check for applicability
     */
    public Set<InferenceRule> getApplicableRules() {
        return getPotentialRules().stream()
                .map(rule -> new InferenceRule(rule, graph()))
                .filter(this::isRuleApplicable)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isRuleResolvable() {
        Type type = getType();
        if (type != null) {
            return !this.getPotentialRules().isEmpty()
                    && !this.getApplicableRules().isEmpty();
        } else {
            return !this.getApplicableRules().isEmpty();
        }
    }

    @Override
    public boolean isRecursive(){
        if (isResource() || getType() == null) return false;
        Type type = getType();
        return getPotentialRules().stream()
                .map(rule -> new InferenceRule(rule, graph()))
                .filter(rule -> rule.getBody().selectAtoms().stream()
                        .filter(at -> Objects.nonNull(at.getType()))
                        .filter(at -> checkTypesCompatible(type, at.getType())).findFirst().isPresent())
                .filter(this::isRuleApplicable)
                .findFirst().isPresent();
    }

    /**
     * @return true if the atom can constitute a head of a rule
     */
    public boolean isAllowedToFormRuleHead(){ return false; }

    /**
     * @return true if the atom requires materialisation in order to be referenced
     */
    public boolean requiresMaterialisation(){ return false; }

    /**
     * @return corresponding type if any
     */
    public Type getType(){
        if (type == null && typeId != null) {
            type = getParentQuery().graph().getConcept(typeId).asType();
        }
        return type;
    }

    /**
     * @return type id of the corresponding type if any
     */
    public ConceptId getTypeId(){ return typeId;}

    /**
     * @return value variable name
     */
    public VarName getValueVariable() {
        throw new IllegalArgumentException("getValueVariable called on Atom object " + getPattern());
    }

    /**
     * @return set of predicates relevant to this atom
     */
    public Set<Predicate> getPredicates() {
        Set<Predicate> predicates = new HashSet<>();
        predicates.addAll(getValuePredicates());
        predicates.addAll(getIdPredicates());
        return predicates;
    }

    /**
     * @return set of id predicates relevant to this atom
     */
    public Set<IdPredicate> getIdPredicates() {
        return ((ReasonerQueryImpl) getParentQuery()).getIdPredicates().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates relevant to this atom
     */
    public Set<ValuePredicate> getValuePredicates(){
        return ((ReasonerQueryImpl) getParentQuery()).getValuePredicates().stream()
                .filter(atom -> atom.getVarName().equals(getValueVariable()))
                .collect(Collectors.toSet());
    }

    /**
     * @return set of types relevant to this atom
     */
    public Set<TypeAtom> getTypeConstraints(){
        Set<TypeAtom> relevantTypes = new HashSet<>();
        //ids from indirect types
        ((ReasonerQueryImpl) getParentQuery()).getTypeConstraints().stream()
                .filter(atom -> atom != this)
                .filter(atom -> containsVar(atom.getVarName()))
                .forEach(relevantTypes::add);
        return relevantTypes;
    }

    /**
     * @return set of constraints of this atom (predicates + types) that are not selectable
     */
    public Set<Atomic> getNonSelectableConstraints() {
        Set<Atom> types = getTypeConstraints().stream()
                .filter(at -> !at.isSelectable())
                .collect(Collectors.toSet());
        return Sets.union(types, getPredicates());
    }

    public Set<IdPredicate> getUnmappedIdPredicates(){ return new HashSet<>();}
    public Set<TypeAtom> getUnmappedTypeConstraints(){ return new HashSet<>();}
    public Set<TypeAtom> getMappedTypeConstraints() { return new HashSet<>();}
    public Set<Unifier> getPermutationUnifiers(Atom headAtom){ return new HashSet<>();}

    //TODO move down to relation only
    /**
     * @return map of role type- (var name, var type) pairs
     */
    public Multimap<RoleType, Pair<VarName, Type>> getRoleVarTypeMap() { return ArrayListMultimap.create();}

    /**
     * infers types (type, role types) fo the atom if applicable/possible
     */
    public void inferTypes(){}

    /**
     * rewrites the atom to one with user defined name
     * @return pair of (rewritten atom, unifiers required to unify child with rewritten atom)
     */
    public Atom rewriteToUserDefined(){ return this;}

    /**
     * rewrites the atom to one with user defined name, need unifiers for cases when we have variable clashes
     * between the relation variable and relation players
     * @return pair of (rewritten atom, unifiers required to unify child with rewritten atom)
     */
    public Pair<Atom, Unifier> rewriteToUserDefinedWithUnifiers(){ return new Pair<>(this, new UnifierImpl());}
}
