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
package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.checkDisjoint;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a {@link HasResourceProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Resource extends MultiPredicateBinary<ValuePredicate>{

    public Resource(VarPatternAdmin pattern, ReasonerQuery par) { this(pattern, Collections.emptySet(), par);}
    public Resource(VarPatternAdmin pattern, Set<ValuePredicate> p, ReasonerQuery par){ super(pattern, p, par);}
    private Resource(Resource a) {
        super(a);
        Set<ValuePredicate> multiPredicate = getMultiPredicate();
        a.getMultiPredicate().stream()
                .map(pred -> (ValuePredicate) AtomicFactory.create(pred, getParentQuery()))
                .forEach(multiPredicate::add);
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                getValueVariable().toString() :
                getMultiPredicate().stream().map(Predicate::getPredicate).collect(Collectors.toSet()).toString();
        return getVarName() + " has " + getOntologyConcept().getLabel() + " " +
                multiPredicateString +
                getIdPredicates().stream().map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected boolean hasEquivalentPredicatesWith(BinaryBase at) {
        if (!(at instanceof Resource)) return false;
        Resource atom = (Resource) at;
        if(this.getMultiPredicate().size() != atom.getMultiPredicate().size()) return false;
        for (ValuePredicate predicate : getMultiPredicate()) {
            Iterator<ValuePredicate> objIt = atom.getMultiPredicate().iterator();
            boolean predicateHasEquivalent = false;
            while (objIt.hasNext() && !predicateHasEquivalent) {
                predicateHasEquivalent = predicate.isEquivalent(objIt.next());
            }
            if (!predicateHasEquivalent) return false;
        }
        return true;
    }

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if(!(ruleAtom.isResource())) return false;

        Resource childAtom = (Resource) ruleAtom;

        //check types
        TypeAtom parentTypeConstraint = this.getTypeConstraints().stream().findFirst().orElse(null);
        TypeAtom childTypeConstraint = childAtom.getTypeConstraints().stream().findFirst().orElse(null);

        OntologyConcept parentType = parentTypeConstraint != null? parentTypeConstraint.getOntologyConcept() : null;
        OntologyConcept childType = childTypeConstraint != null? childTypeConstraint.getOntologyConcept() : null;
        if (parentType != null && childType != null && checkDisjoint(parentType, childType)) return false;

        //check predicates
        if (childAtom.getMultiPredicate().isEmpty() || getMultiPredicate().isEmpty()) return true;
        for (ValuePredicate childPredicate : childAtom.getMultiPredicate()) {
            Iterator<ValuePredicate> parentIt = getMultiPredicate().iterator();
            boolean predicateCompatible = false;
            while (parentIt.hasNext() && !predicateCompatible) {
                ValuePredicate parentPredicate = parentIt.next();
                predicateCompatible = parentPredicate.getPredicate().isCompatibleWith(childPredicate.getPredicate());
            }
            if (!predicateCompatible) return false;
        }
        return true;
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = super.getVarNames();
        getMultiPredicate().stream().flatMap(p -> p.getVarNames().stream()).forEach(vars::add);
        return vars;
    }

    @Override
    protected ConceptId extractTypeId(VarPatternAdmin var) {
        HasResourceProperty resProp = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        Label label = resProp != null? resProp.getType() : null;
        return label != null ? getParentQuery().graph().getOntologyConcept(label).getId() : null;
    }

    @Override
    protected Var extractValueVariableName(VarPatternAdmin var){
        HasResourceProperty prop = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        VarPatternAdmin resVar = prop.getResource();
        return resVar.getVarName().isUserDefinedName()? resVar.getVarName() : Graql.var("");
    }

    @Override
    public Atomic copy(){ return new Resource(this);}

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    @Override
    public boolean isAllowedToFormRuleHead(){
        if (getOntologyConcept() == null || getMultiPredicate().size() > 1) return false;
        if (getMultiPredicate().isEmpty()) return true;

        ValuePredicate predicate = getMultiPredicate().iterator().next();
        return predicate.getPredicate().isSpecific();
    }

    @Override
    public boolean requiresMaterialisation(){ return true;}

    @Override
    public int computePriority(Set<Var> subbedVars){
        int priority = super.computePriority(subbedVars);
        Set<ValuePredicateAdmin> vps = getValuePredicates().stream().map(ValuePredicate::getPredicate).collect(Collectors.toSet());
        priority += ResolutionStrategy.IS_RESOURCE_ATOM;

        if (vps.isEmpty()) {
            if (subbedVars.contains(getVarName())
                    || subbedVars.contains(getValueVariable())) {
                    priority += ResolutionStrategy.SPECIFIC_VALUE_PREDICATE;
            } else{
                    priority += ResolutionStrategy.VARIABLE_VALUE_PREDICATE;
            }
        } else {
            int vpsPriority = 0;
            for (ValuePredicateAdmin vp : vps) {
                //vp with a value
                if (vp.isSpecific()) {
                    vpsPriority += ResolutionStrategy.SPECIFIC_VALUE_PREDICATE;
                } //vp with a variable
                else if (vp.getInnerVar().isPresent()) {
                    VarPatternAdmin inner = vp.getInnerVar().orElse(null);
                    //variable mapped inside the query
                    if (subbedVars.contains(getVarName())
                        || subbedVars.contains(inner.getVarName())) {
                        vpsPriority += ResolutionStrategy.SPECIFIC_VALUE_PREDICATE;
                    } //variable equality
                    else if (vp.equalsValue().isPresent()){
                        vpsPriority += ResolutionStrategy.VARIABLE_VALUE_PREDICATE;
                    } //variable inequality
                    else {
                        vpsPriority += ResolutionStrategy.COMPARISON_VARIABLE_VALUE_PREDICATE;
                    }
                } else {
                    vpsPriority += ResolutionStrategy.NON_SPECIFIC_VALUE_PREDICATE;
                }
            }
            //normalise
            vpsPriority = vpsPriority/vps.size();
            priority += vpsPriority;
        }

        boolean reifiesRelation =  getNeighbours()
                .filter(Atom::isRelation)
                .filter(at -> at.getVarName().equals(this.getVarName()))
                .findFirst().isPresent();

        priority += reifiesRelation ? ResolutionStrategy.RESOURCE_REIFIYING_RELATION : 0;

        return priority;
    }

    @Override
    public Unifier getUnifier(Atom parentAtom) {
        if (!(parentAtom instanceof TypeAtom)) return super.getUnifier(parentAtom);

        //case when parent is a type atom
        Unifier unifier = new UnifierImpl();
        unifier.addMapping(this.getValueVariable(), parentAtom.getVarName());
        return unifier;
    }

    @Override
    public Set<ValuePredicate> getValuePredicates(){
        Set<ValuePredicate> valuePredicates = super.getValuePredicates();
        getMultiPredicate().forEach(valuePredicates::add);
        return valuePredicates;
    }

    @Override
    public Set<TypeAtom> getSpecificTypeConstraints() {
        return getTypeConstraints().stream()
                .filter(t -> t.getVarName().equals(getVarName()))
                .filter(t -> Objects.nonNull(t.getOntologyConcept()))
                .collect(Collectors.toSet());
    }

}
