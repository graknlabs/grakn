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

package ai.grakn.graql.internal.reasoner.rule;

import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.UnifierType;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.cache.SimpleQueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.state.QueryStateBase;
import ai.grakn.graql.internal.reasoner.state.ResolutionState;
import ai.grakn.graql.internal.reasoner.state.RuleState;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Class providing resolution and higher level facilities for {@link Rule} objects.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class InferenceRule {

    private final EmbeddedGraknTx<?> tx;
    private final Rule rule;
    private final ReasonerQueryImpl body;
    private final ReasonerAtomicQuery head;

    private long priority = Long.MAX_VALUE;
    private Boolean requiresMaterialisation = null;

    public InferenceRule(Rule rule, EmbeddedGraknTx<?> tx){
        this.tx = tx;
        this.rule = rule;
        //TODO simplify once changes propagated to rule objects
        this.body = ReasonerQueries.create(conjunction(rule.when().admin()), tx);
        this.head = ReasonerQueries.atomic(conjunction(rule.then().admin()), tx);
    }

    private InferenceRule(ReasonerAtomicQuery head, ReasonerQueryImpl body, Rule rule, EmbeddedGraknTx<?> tx){
        this.tx = tx;
        this.rule = rule;
        this.head = head;
        this.body = body;
    }

    @Override
    public String toString(){
        return  "\n" + this.body.toString() + "->\n" + this.head.toString() + "[" + resolutionPriority() +"]\n";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        InferenceRule rule = (InferenceRule) obj;
        return this.getBody().equals(rule.getBody())
                && this.getHead().equals(rule.getHead());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + getBody().hashCode();
        hashCode = hashCode * 37 + getHead().hashCode();
        return hashCode;
    }

    /**
     * @return the priority with which the rule should be fired
     */
    public long resolutionPriority(){
        if (priority == Long.MAX_VALUE) {
            //NB: only checking locally as checking full tree (getDependentRules) is expensive
            priority = -getBody().selectAtoms().flatMap(Atom::getApplicableRules).count();
        }
        return priority;
    }

    private Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    public Rule getRule(){ return rule;}

    /**
     * @return true if the rule has disconnected head, i.e. head and body do not share any variables
     */
    private boolean hasDisconnectedHead(){
        return Sets.intersection(body.getVarNames(), head.getVarNames()).isEmpty();
    }

    /**
     * @return true if head satisfies the pattern specified in the body of the rule
     */
    boolean headSatisfiesBody(){
        Set<Atomic> atoms = new HashSet<>(getHead().getAtoms());
        Set<Var> headVars = getHead().getVarNames();
        getBody().getAtoms(TypeAtom.class)
                .filter(t -> !t.isRelation())
                .filter(t -> !Sets.intersection(t.getVarNames(), headVars).isEmpty())
                .forEach(atoms::add);
        return getBody().isEquivalent(ReasonerQueries.create(atoms, tx));
    }

    /**
     * rule requires materialisation in the context of resolving parent atom
     * if parent atom requires materialisation, head atom requires materialisation or if the head contains only fresh variables
     *
     * @return true if the rule needs to be materialised
     */
    public boolean requiresMaterialisation(Atom parentAtom){
        if (requiresMaterialisation == null) {
            requiresMaterialisation = parentAtom.requiresMaterialisation()
                    || getHead().getAtom().requiresMaterialisation()
                    || hasDisconnectedHead();
        }
        return requiresMaterialisation;
    }

    /**
     * @return body of the rule of the form head :- body
     */
    public ReasonerQueryImpl getBody(){ return body;}

    /**
     * @return head of the rule of the form head :- body
     */
    public ReasonerAtomicQuery getHead(){ return head;}

    /**
     * @return reasoner query formed of combining head and body queries
     */
    private ReasonerQueryImpl getCombinedQuery(){
        Set<Atomic> allAtoms = new HashSet<>();
        allAtoms.add(head.getAtom());
        allAtoms.addAll(body.getAtoms());
        return ReasonerQueries.create(allAtoms, tx);
    }

    /**
     * @return a conclusion atom which parent contains all atoms in the rule
     */
    public Atom getRuleConclusionAtom() {
        return getCombinedQuery().getAtoms(Atom.class).filter(at -> at.equals(head.getAtom())).findFirst().orElse(null);
    }

    /**
     * @param parentAtom atom containing constraints (parent)
     * @param unifier unifier unifying parent with the rule
     * @return rule with propagated constraints from parent
     */
    private InferenceRule propagateConstraints(Atom parentAtom, Unifier unifier){
        if (!parentAtom.isRelation() && !parentAtom.isResource()) return this;
        Atom headAtom = head.getAtom();
        Set<Atomic> bodyAtoms = new HashSet<>(body.getAtoms());

        //transfer value predicates
        parentAtom.getPredicates(ValuePredicate.class)
                .flatMap(vp -> vp.unify(unifier).stream())
                .forEach(bodyAtoms::add);

        //if head is a resource merge vps into head
        if (headAtom.isResource() && ((ResourceAtom) headAtom).getMultiPredicate().isEmpty()) {
            ResourceAtom resourceHead = (ResourceAtom) headAtom;
            Set<ValuePredicate> innerVps = parentAtom.getInnerPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier).stream())
                    .peek(bodyAtoms::add)
                    .collect(toSet());
            headAtom = ResourceAtom.create(
                    headAtom.getPattern(),
                    headAtom.getPredicateVariable(),
                    resourceHead.getRelationVariable(),
                    resourceHead.getTypeId(),
                    innerVps,
                    headAtom.getParentQuery()
            );
        }

        Set<TypeAtom> unifiedTypes = parentAtom.getTypeConstraints()
                .flatMap(type -> type.unify(unifier).stream())
                .collect(toSet());

        //set rule body types to sub types of combined query+rule types
        Set<TypeAtom> ruleTypes = body.getAtoms(TypeAtom.class).filter(t -> !t.isRelation()).collect(toSet());
        Set<TypeAtom> allTypes = Sets.union(unifiedTypes, ruleTypes);
        allTypes.stream()
                .filter(ta -> {
                    SchemaConcept schemaConcept = ta.getSchemaConcept();
                    SchemaConcept subType = allTypes.stream()
                            .map(Atom::getSchemaConcept)
                            .filter(Objects::nonNull)
                            .filter(t -> ReasonerUtils.supers(t).contains(schemaConcept))
                            .findFirst().orElse(null);
                    return schemaConcept == null || subType == null;
                }).forEach(t -> bodyAtoms.add(t.copy(body)));
        return new InferenceRule(
                ReasonerQueries.atomic(headAtom),
                ReasonerQueries.create(bodyAtoms, tx),
                rule,
                tx
        );
    }

    private InferenceRule rewriteHeadToRelation(Atom parentAtom){
        if (parentAtom.isRelation() && getHead().getAtom().isResource()){
            return new InferenceRule(
                    ReasonerQueries.atomic(getHead().getAtom().toRelationshipAtom()),
                    ReasonerQueries.create(getBody().getAtoms(), tx),
                    rule,
                    tx
            );
        }
        return this;
    }

    public boolean isAppendRule(){
        Atom headAtom = getHead().getAtom();
        SchemaConcept headType = headAtom.getSchemaConcept();
        if (headType.isRelationshipType()
                && headAtom.getVarName().isUserDefinedName()) {
            RelationshipAtom bodyAtom = getBody().getAtoms(RelationshipAtom.class)
                    .filter(at -> Objects.nonNull(at.getSchemaConcept()))
                    .filter(at -> at.getSchemaConcept().equals(headType))
                    .filter(at -> at.getVarName().isUserDefinedName())
                    .findFirst().orElse(null);
            return bodyAtom != null;
        }
        return false;
    }

    private InferenceRule rewriteVariables(Atom parentAtom){
        if (parentAtom.isUserDefined() || parentAtom.requiresRoleExpansion()) {
            ReasonerAtomicQuery rewrittenHead = ReasonerQueries.atomic(head.getAtom().rewriteToUserDefined(parentAtom));
            List<Atom> bodyRewrites = new ArrayList<>();
            body.getAtoms(Atom.class)
                    .map(at -> {
                        if (at.isRelation()
                                && !at.isUserDefined()
                                && Objects.equals(at.getSchemaConcept(), head.getAtom().getSchemaConcept())) {
                            return at.rewriteToUserDefined(parentAtom);
                        } else {
                            return at;
                        }
                    }).forEach(bodyRewrites::add);

            ReasonerQueryImpl rewrittenBody = ReasonerQueries.create(bodyRewrites, tx);
            return new InferenceRule(rewrittenHead, rewrittenBody, rule, tx);
        }
        return this;
    }

    private InferenceRule rewriteBodyAtoms(){
        if (getBody().requiresDecomposition()) {
            return new InferenceRule(getHead(), getBody().rewrite(), rule, tx);
        }
        return this;
    }

    /**
     * rewrite the rule to a form with user defined variables
     * @param parentAtom reference parent atom
     * @return rewritten rule
     */
    public InferenceRule rewrite(Atom parentAtom){
        return this
                .rewriteBodyAtoms()
                .rewriteHeadToRelation(parentAtom)
                .rewriteVariables(parentAtom);
    }

    /**
     * @param parentAtom atom to which this rule is applied
     * @param ruleUnifier unifier with parent state
     * @param parent parent state
     * @param visitedSubGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoal formed from this rule
     */
    public ResolutionState subGoal(Atom parentAtom, Unifier ruleUnifier, QueryStateBase parent, Set<ReasonerAtomicQuery> visitedSubGoals, SimpleQueryCache<ReasonerAtomicQuery> cache){
        Unifier ruleUnifierInverse = ruleUnifier.inverse();

        //delta' = theta . thetaP . delta
        ConceptMap partialSubPrime = parentAtom.getParentQuery()
                .getSubstitution()
                .unify(ruleUnifierInverse);

        return new RuleState(this.propagateConstraints(parentAtom, ruleUnifierInverse), partialSubPrime, ruleUnifier, parent, visitedSubGoals, cache);
    }

    /**
     * @param parentAtom atom to unify the rule with
     * @return corresponding unifier
     */
    public MultiUnifier getMultiUnifier(Atom parentAtom) {
        Atom childAtom = getRuleConclusionAtom();
        if (parentAtom.getSchemaConcept() != null){
            return childAtom.getMultiUnifier(parentAtom, UnifierType.RULE);
        }
        //case of match all atom (atom without type)
        else{
            Atom extendedParent = parentAtom
                    .addType(childAtom.getSchemaConcept())
                    .inferTypes();
            return childAtom.getMultiUnifier(extendedParent, UnifierType.RULE);
        }
    }
}
