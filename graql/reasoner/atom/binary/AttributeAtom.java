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
package grakn.core.graql.reasoner.atom.binary;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.core.AttributeValueConverter;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.cache.VariableDefinition;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static grakn.core.graql.reasoner.utils.ReasonerUtils.isEquivalentCollection;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a HasAttributeProperty.
 * The resource structure is the following:
 *
 * has($varName, $attributeVariable), type($attributeVariable)
 *
 * or in graql terms:
 *
 * $varName has <type> $attributeVariable;
 * $attributeVariable isa $predicateVariable; [$predicateVariable/<type id>]
 *
 * </p>
 *
 *
 */
public class AttributeAtom extends Binary{

    private final ImmutableSet<ValuePredicate> multiPredicate;
    private final Variable attributeVariable;

    private KeyspaceStatistics keyspaceStatistics;

    private final Variable relationVariable;

    private AttributeAtom(
            Variable varName,
            Statement pattern,
            ReasonerQuery parentQuery,
            @Nullable ConceptId typeId,
            Variable predicateVariable,
            Variable relationVariable,
            Variable attributeVariable,
            ImmutableSet<ValuePredicate> multiPredicate,
            ReasonerQueryFactory reasonerQueryFactory,
            ConceptManager conceptManager,
            RuleCache ruleCache,
            QueryCache queryCache,
            KeyspaceStatistics keyspaceStatistics) {
        super(varName, pattern, parentQuery, typeId, predicateVariable, reasonerQueryFactory, conceptManager, queryCache, ruleCache);
        this.keyspaceStatistics = keyspaceStatistics;

        this.relationVariable = relationVariable;
        this.attributeVariable = attributeVariable;
        this.multiPredicate = multiPredicate;
    }

    public Variable getRelationVariable() {
        return relationVariable;
    }
    public Variable getAttributeVariable() {
        return attributeVariable;
    }
    public ImmutableSet<ValuePredicate> getMultiPredicate() {
        return multiPredicate;
    }

    public static AttributeAtom create(Statement pattern, Variable attributeVariable,
                                       Variable relationVariable, Variable predicateVariable, ConceptId predicateId,
                                       Set<ValuePredicate> ps, ReasonerQuery parent,
                                       ReasonerQueryFactory reasonerQueryFactory, ConceptManager conceptManager,
                                       QueryCache queryCache, RuleCache ruleCache, KeyspaceStatistics keyspaceStatistics) {
        return new AttributeAtom(
                pattern.var(), pattern, parent, predicateId, predicateVariable, relationVariable, attributeVariable, ImmutableSet.copyOf(ps),
                reasonerQueryFactory,conceptManager, ruleCache, queryCache, keyspaceStatistics);
    }

    private static AttributeAtom create(AttributeAtom a, ReasonerQuery parent) {
        return create(a.getPattern(), a.getAttributeVariable(),
                a.getRelationVariable(), a.getPredicateVariable(), a.getTypeId(), a.getMultiPredicate(), parent,
                a.queryFactory, a.conceptManager, a.queryCache, a.ruleCache, a.keyspaceStatistics);
    }

    private AttributeAtom convertValues(){
        SchemaConcept type = getSchemaConcept();
        AttributeType<Object> attributeType = type.isAttributeType()? type.asAttributeType() : null;
        if (attributeType == null || Schema.MetaSchema.isMetaLabel(attributeType.label())) return this;

        AttributeType.DataType<Object> dataType = attributeType.dataType();
        Set<ValuePredicate> newMultiPredicate = this.getMultiPredicate().stream().map(vp -> {
            Object value = vp.getPredicate().value();
            if (value == null) return vp;
            Object convertedValue;
            try {
                convertedValue = AttributeValueConverter.of(dataType).convert(value);
            } catch (ClassCastException e){
                throw GraqlSemanticException.incompatibleAttributeValue(dataType, value);
            }
            ValueProperty.Operation operation = ValueProperty.Operation.Comparison.of(vp.getPredicate().comparator(), convertedValue);
            return ValuePredicate.create(vp.getVarName(), operation, getParentQuery());
        }).collect(Collectors.toSet());
        return create(
                getPattern(), getAttributeVariable(), getRelationVariable(), getPredicateVariable(), getTypeId(), newMultiPredicate, getParentQuery(),
                queryFactory, conceptManager, queryCache, ruleCache, keyspaceStatistics);
    }


    /**
     * TODO remove this, short term workaround
     * copy constructor that overrides the predicates
     */
    public AttributeAtom copy(Set<ValuePredicate> newPredicates) {
        return create(getPattern(), getAttributeVariable(),
                getRelationVariable(),
                getPredicateVariable(),
                getTypeId(),
                newPredicates,
                getParentQuery(),
                queryFactory, conceptManager, queryCache, ruleCache, keyspaceStatistics);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent);}

    @Override
    public Atomic simplify() {
        return this.convertValues();
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeProperty.class;}

    @Override
    public AttributeAtom toAttributeAtom(){ return this; }

    @Override
    public RelationAtom toRelationAtom(){
        SchemaConcept type = getSchemaConcept();
        if (type == null) throw ReasonerException.illegalAtomConversion(this, RelationAtom.class);
        Label typeLabel = Schema.ImplicitType.HAS.getLabel(type.label());

        RelationAtom relationAtom = RelationAtom.create(
                Graql.var(getRelationVariable())
                        .rel(Schema.ImplicitType.HAS_OWNER.getLabel(type.label()).getValue(), new Statement(getVarName()))
                        .rel(Schema.ImplicitType.HAS_VALUE.getLabel(type.label()).getValue(), new Statement(getAttributeVariable()))
                        .isa(typeLabel.getValue()),
                getPredicateVariable(),
                conceptManager.getSchemaConcept(typeLabel).id(),
                getParentQuery(),
                queryFactory, conceptManager, queryCache, ruleCache, keyspaceStatistics
        );

        Set<Statement> patterns = new HashSet<>(relationAtom.getCombinedPattern().statements());
        this.getPredicates().map(Predicate::getPattern).forEach(patterns::add);
        this.getMultiPredicate().stream().map(Predicate::getPattern).forEach(patterns::add);
        return queryFactory.atomic(Graql.and(patterns)).getAtom().toRelationAtom();
    }

    /**
     * NB: this is somewhat ambiguous cause from {$x has resource $r;} we can extract:
     * - $r isa owner-type;
     * - $x isa attribute-type;
     * We pick the latter as the type information is available.
     *
     * @return corresponding isa atom
     */
    @Override
    public IsaAtom toIsaAtom() {
        IsaAtom isaAtom = IsaAtom.create(getAttributeVariable(), getPredicateVariable(), getTypeId(), false, getParentQuery(),
                queryFactory, conceptManager, queryCache, ruleCache);
        Set<Statement> patterns = new HashSet<>(isaAtom.getCombinedPattern().statements());
        this.getPredicates().map(Predicate::getPattern).forEach(patterns::add);
        this.getMultiPredicate().stream().map(Predicate::getPattern).forEach(patterns::add);
        return queryFactory.atomic(Graql.and(patterns)).getAtom().toIsaAtom();
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                "" :
                getMultiPredicate().stream().map(Predicate::getPredicate).collect(Collectors.toSet()).toString();
        return getVarName() + " has " + getSchemaConcept().label() + " " +
                getAttributeVariable() + " " +
                multiPredicateString +
                (getRelationVariable().isReturned()? "(" + getRelationVariable() + ")" : "") +
                getPredicates(IdPredicate.class).map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        AttributeAtom a2 = (AttributeAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName())
                && this.multiPredicateEqual(a2);
    }

    @Override
    boolean isBaseEquivalent(Object obj){
        if (!super.isBaseEquivalent(obj)) return false;
        AttributeAtom that = (AttributeAtom) obj;
        return this.getRelationVariable().isReturned() == that.getRelationVariable().isReturned();
    }

    private boolean multiPredicateEqual(AttributeAtom that){
        return isEquivalentCollection(this.getMultiPredicate(), that.getMultiPredicate(), AtomicEquivalence.Equality);
    }

    @Override
    public final int hashCode() {
        int hashCode = this.alphaEquivalenceHashCode();
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        return hashCode;
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + AtomicEquivalence.equivalenceHash(this.getMultiPredicate(), AtomicEquivalence.AlphaEquivalence);
        return hashCode;
    }

    @Override
    public void checkValid(){
        super.checkValid();
        SchemaConcept type = getSchemaConcept();
        if (type != null && !type.isAttributeType()) {
            throw GraqlSemanticException.attributeWithNonAttributeType(type.label());
        }
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<Statement> vars = getMultiPredicate().stream()
                .map(Atomic::getPattern)
                .collect(Collectors.toSet());
        vars.add(getPattern());
        return Graql.and(vars);
    }

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        //findbugs complains about cast without it
        if (!(ruleAtom instanceof AttributeAtom)) return false;

        AttributeAtom childAtom = (AttributeAtom) ruleAtom;
        return childAtom.isUnifiableWith(this);
    }

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    public boolean isValueEquality(){ return getMultiPredicate().stream().anyMatch(p -> p.getPredicate().isValueEquality());}

    @Override
    public boolean hasUniqueAnswer(){
        ConceptMap sub = getParentQuery().getSubstitution();
        if (sub.vars().containsAll(getVarNames())) return true;

        SchemaConcept type = getSchemaConcept();
        if (type == null) return false;

        boolean isBottomType = type.subs().allMatch(t -> t.equals(type));
        boolean relationVarMapped = !getRelationVariable().isReturned() || sub.containsVar(getRelationVariable());
        return isBottomType
                && isValueEquality() && sub.containsVar(getVarName())
                && relationVarMapped;
    }

    @Override
    public boolean requiresMaterialisation(){ return true;}

    @Override
    public Set<String> validateAsRuleHead(Rule rule){
        Set<String> errors = super.validateAsRuleHead(rule);
        if (getSchemaConcept() == null || getMultiPredicate().size() > 1){
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATTRIBUTE_WITH_AMBIGUOUS_PREDICATES.getMessage(rule.then(), rule.label()));
        }
        if (getMultiPredicate().isEmpty()){
            boolean predicateBound = getParentQuery().getAtoms(Atom.class)
                    .filter(at -> !at.equals(this))
                    .anyMatch(at -> at.getVarNames().contains(getAttributeVariable()));
            if (!predicateBound) {
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE.getMessage(rule.then(), rule.label()));
            }
        }

        getMultiPredicate().stream()
                .filter(p -> !p.getPredicate().isValueEquality())
                .forEach( p ->
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATTRIBUTE_WITH_NONSPECIFIC_PREDICATE.getMessage(rule.then(), rule.label()))
                );

        if(getMultiPredicate().isEmpty()) {
            Variable attrVar = getAttributeVariable();
            AttributeType.DataType<Object> dataType = getSchemaConcept().asAttributeType().dataType();
            ResolvableQuery body = CacheCasting.ruleCacheCast(ruleCache).getRule(rule).getBody();
            ErrorMessage incompatibleValuesMsg = ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_COPYING_INCOMPATIBLE_ATTRIBUTE_VALUES;
            body.getAtoms(AttributeAtom.class)
                    .filter(at -> at.getAttributeVariable().equals(attrVar))
                    .map(Binary::getSchemaConcept)
                    .filter(type -> !type.asAttributeType().dataType().equals(dataType))
                    .forEach(type ->
                            errors.add(incompatibleValuesMsg.getMessage(getSchemaConcept().label(), rule.label(), type.label()))
                    );
        }
        return errors;
    }

    @Override
    public Set<String> validateAsRuleBody(Label ruleLabel) {
        SchemaConcept type = getSchemaConcept();
        Set<String> errors = new HashSet<>();
        if (type == null) return errors;

        if (!type.isAttributeType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage(ruleLabel, type.label()));
            return errors;
        }

        Type ownerType = getParentQuery().getUnambiguousType(getVarName(), false);

        if (ownerType != null
                && ownerType.attributes().noneMatch(rt -> rt.equals(type.asAttributeType()))){
            errors.add(ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage(ruleLabel, type.label(), ownerType.label()));
        }
        return errors;
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> varNames = super.getVarNames();
        varNames.add(getAttributeVariable());
        if (getRelationVariable().isReturned()) varNames.add(getRelationVariable());
        getMultiPredicate().forEach(p -> varNames.addAll(p.getVarNames()));
        return varNames;
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierType unifierType) {
        if (!(parentAtom instanceof AttributeAtom)) {
            // in general this >= parent, hence for rule unifiers we can potentially specialise child to match parent
            if (unifierType.equals(UnifierType.RULE)) {
                if (parentAtom instanceof IsaAtom) return this.toIsaAtom().getUnifier(parentAtom, unifierType);
                else if (parentAtom instanceof RelationAtom){
                    return this.toRelationAtom().getUnifier(parentAtom, unifierType);
                }
            }
            return UnifierImpl.nonExistent();
        }

        AttributeAtom parent = (AttributeAtom) parentAtom;
        Unifier unifier = super.getUnifier(parentAtom, unifierType);
        if (unifier == null) return UnifierImpl.nonExistent();

        //unify attribute vars
        Variable childAttributeVarName = this.getAttributeVariable();
        Variable parentAttributeVarName = parent.getAttributeVariable();
        if (parentAttributeVarName.isReturned()){
            unifier = unifier.merge(new UnifierImpl(ImmutableMap.of(childAttributeVarName, parentAttributeVarName)));
        }

        //unify relation vars
        Variable childRelationVarName = this.getRelationVariable();
        Variable parentRelationVarName = parent.getRelationVariable();
        if (parentRelationVarName.isReturned()){
            unifier = unifier.merge(new UnifierImpl(ImmutableMap.of(childRelationVarName, parentRelationVarName)));
        }

        return isPredicateCompatible(parentAtom, unifier, unifierType)?
                unifier : UnifierImpl.nonExistent();
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(super.getInnerPredicates(), getMultiPredicate().stream());
    }

    @Override
    public SemanticDifference semanticDifference(Atom child, Unifier unifier) {
        SemanticDifference baseDiff = super.semanticDifference(child, unifier);
        if (!child.isResource()) return baseDiff;
        AttributeAtom childAtom = (AttributeAtom) child;
        Set<VariableDefinition> diff = new HashSet<>();

        Variable parentVar = this.getAttributeVariable();
        Unifier unifierInverse = unifier.inverse();
        Set<ValuePredicate> predicatesToSatisfy = childAtom.getMultiPredicate().stream()
                .flatMap(vp -> vp.unify(unifierInverse).stream()).collect(toSet());
        this.getMultiPredicate().forEach(predicatesToSatisfy::remove);

        diff.add(new VariableDefinition(parentVar, null, null, new HashSet<>(), predicatesToSatisfy));
        return baseDiff.merge(new SemanticDifference(diff));
    }

    /**
     * rewrites the atom to one with relation variable
     * @param parentAtom parent atom that triggers rewrite
     * @return rewritten atom
     */
    private AttributeAtom rewriteWithRelationVariable(Atom parentAtom){
        if (parentAtom.isResource() && ((AttributeAtom) parentAtom).getRelationVariable().isReturned()) return rewriteWithRelationVariable();
        return this;
    }

    @Override
    public AttributeAtom rewriteWithRelationVariable(){
        Variable attributeVariable = getAttributeVariable();
        Variable relationVariable = getRelationVariable().asReturnedVar();
        Statement newVar = new Statement(getVarName())
                .has(getSchemaConcept().label().getValue(), new Statement(attributeVariable), new Statement(relationVariable));
        return create(newVar, attributeVariable, relationVariable, getPredicateVariable(), getTypeId(),
                getMultiPredicate(), getParentQuery(),
                queryFactory, conceptManager, queryCache, ruleCache, keyspaceStatistics);
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getPattern(), getAttributeVariable(), getRelationVariable(),
                getPredicateVariable().asReturnedVar(), getTypeId(), getMultiPredicate(), getParentQuery(),
                queryFactory, conceptManager, queryCache, ruleCache, keyspaceStatistics);
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        return this
                .rewriteWithRelationVariable(parentAtom)
                .rewriteWithTypeVariable(parentAtom);
    }
}
