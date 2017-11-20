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

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationshipProperty;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.type.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.utils.conversion.RoleConverter;
import ai.grakn.graql.internal.reasoner.utils.conversion.TypeConverter;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.Iterator;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.areDisjointTypes;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.compatibleRelationTypesWithRoles;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.predicatesCompatible;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.supers;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.multimapIntersection;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.compatibleRoles;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.top;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Atom implementation defining a relation atom corresponding to a combined {@link RelationshipProperty}
 * and (optional) {@link IsaProperty}. The relation atom is a {@link TypeAtom} with relationship players.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RelationshipAtom extends IsaAtom {

    private int hashCode = 0;
    private ImmutableMultimap<Role, Var> roleVarMap = null;
    private ImmutableMultimap<Role, Type> roleTypeMap = null;
    private ImmutableMultimap<Role, String> roleConceptIdMap = null;
    private ImmutableList<RelationshipType> possibleRelations = null;
    private final ImmutableList<RelationPlayer> relationPlayers;
    private final ImmutableSet<Label> roleLabels;

    public RelationshipAtom(VarPatternAdmin pattern, Var predicateVar, @Nullable IdPredicate predicate, ReasonerQuery par) {
        super(pattern, predicateVar, predicate, par);
        List<RelationPlayer> rps = new ArrayList<>();
        getPattern().asVarPattern()
                .getProperty(RelationshipProperty.class)
                .ifPresent(prop -> prop.relationPlayers().forEach(rps::add));
        this.relationPlayers = ImmutableList.copyOf(rps);
        this.roleLabels = ImmutableSet.<Label>builder().addAll(
                relationPlayers.stream()
                        .map(RelationPlayer::getRole)
                        .flatMap(CommonUtil::optionalToStream)
                        .map(VarPatternAdmin::getTypeLabel)
                        .flatMap(CommonUtil::optionalToStream)
                        .iterator()
        ).build();
    }

    private RelationshipAtom(VarPatternAdmin pattern, Var predicateVar, @Nullable IdPredicate predicate, ImmutableList<RelationshipType> possibleRelations, ReasonerQuery par) {
        this(pattern, predicateVar, predicate, par);
        this.possibleRelations = possibleRelations;
    }

    private RelationshipAtom(RelationshipAtom a) {
        super(a);
        this.relationPlayers = a.relationPlayers;
        this.roleLabels = a.roleLabels;
        this.roleVarMap = a.roleVarMap;
        this.possibleRelations = a.possibleRelations;
    }

    @Override
    public RelationshipAtom toRelationshipAtom(){ return this;}

    @Override
    public String toString(){
        String typeString = getSchemaConcept() != null?
                getSchemaConcept().getLabel().getValue() :
                "{" + inferPossibleRelationTypes(new QueryAnswer()).stream().map(rt -> rt.getLabel().getValue()).collect(Collectors.joining(", ")) + "}";
        String relationString = (isUserDefined()? getVarName() + " ": "") +
                typeString +
                getRelationPlayers().toString();
        return relationString + getPredicates(Predicate.class).map(Predicate::toString).collect(Collectors.joining(""));
    }

    private Set<Label> getRoleLabels() { return roleLabels;}
    private ImmutableList<RelationPlayer> getRelationPlayers() { return relationPlayers;}

    /**
     * @return set constituting the role player var names
     */
    public Set<Var> getRolePlayers() {
        return getRelationPlayers().stream().map(c -> c.getRolePlayer().var()).collect(toSet());
    }

    private Set<Var> getRoleVariables(){
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::var)
                .filter(Var::isUserDefinedName)
                .collect(Collectors.toSet());
    }

    @Override
    public Atomic copy() {
        return new RelationshipAtom(this);
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     * @param varName variable name
     * @return corresponding {@link VarPatternAdmin}
     */
    private VarPatternAdmin relationPattern(Var varName, List<RelationPlayer> relationPlayers) {
        VarPattern var = varName;
        for (RelationPlayer rp : relationPlayers) {
            VarPattern rolePattern = rp.getRole().orElse(null);
            var = rolePattern == null? var.rel(rp.getRolePlayer()) : var.rel(rolePattern, rp.getRolePlayer());
        }
        return var.admin();
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = 1;
            hashCode = hashCode * 37 + (getTypeId() != null ? getTypeId().hashCode() : 0);
            hashCode = hashCode * 37 + getVarNames().hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationshipAtom a2 = (RelationshipAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && getVarNames().equals(a2.getVarNames())
                && getRelationPlayers().equals(a2.getRelationPlayers());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationshipAtom a2 = (RelationshipAtom) obj;
        return (isUserDefined() == a2.isUserDefined())
                && Objects.equals(this.getTypeId(), a2.getTypeId())
                //check relation players equivalent
                && getRolePlayers().size() == a2.getRolePlayers().size()
                && getRelationPlayers().size() == a2.getRelationPlayers().size()
                && getRoleLabels().equals(a2.getRoleLabels())
                //check bindings
                && getRoleConceptIdMap().equals(a2.getRoleConceptIdMap())
                && getRoleTypeMap().equals(a2.getRoleTypeMap());
    }

    @Override
    public int equivalenceHashCode() {
        int equivalenceHashCode = 1;
        equivalenceHashCode = equivalenceHashCode * 37 + (this.getTypeId() != null ? this.getTypeId().hashCode() : 0);
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleConceptIdMap().hashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleTypeMap().hashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleLabels().hashCode();
        return equivalenceHashCode;
    }

    @Override
    public boolean isRelation() {
        return true;
    }

    @Override
    public boolean isSelectable() {
        return true;
    }

    @Override
    public boolean isType() {
        return getSchemaConcept() != null;
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefined();
    }

    @Override
    public boolean requiresRoleExpansion() {
        return !getRoleVariables().isEmpty();
    }

    @Override
    public boolean isAllowedToFormRuleHead(){
        //can form a rule head if specified type, type is not implicit and all relation players are insertable
        return getSchemaConcept() != null
                && !getSchemaConcept().asType().isImplicit()
                && hasAllRelationPlayersInsertable();
    }

    private boolean hasAllRelationPlayersInsertable(){
        return getExplicitRoles()
                .filter(role -> !role.isImplicit())
                .filter(role -> !Schema.MetaSchema.isMetaLabel(role.getLabel()))
                .count() == getRelationPlayers().size();
    }

    @Override
    public Set<String> validateOntologically() {
        Set<String> errors = new HashSet<>();
        SchemaConcept type = getSchemaConcept();
        if (type != null && !type.isRelationshipType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage(type.getLabel()));
            return errors;
        }

        //check role-tyoe compatibility
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();
        for (Map.Entry<Role, Collection<Var>> e : getRoleVarMap().asMap().entrySet() ){
            Role role = e.getKey();
            if (!Schema.MetaSchema.isMetaLabel(role.getLabel())) {
                //check whether this role can be played in this relation
                if (type != null && type.asRelationshipType().relates().noneMatch(r -> r.equals(role))) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage(role.getLabel(), type.getLabel()));
                }

                //check whether the role player's type allows playing this role
                for (Var player : e.getValue()) {
                    Type playerType = varTypeMap.get(player);
                    if (playerType != null && playerType.plays().noneMatch(plays -> plays.equals(role))) {
                        errors.add(ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage(playerType.getLabel(), role.getLabel(), type == null? "" : type.getLabel()));
                    }
                }
            }
        }
        return errors;
    }

    @Override
    public int computePriority(Set<Var> subbedVars) {
        int priority = super.computePriority(subbedVars);
        priority += ResolutionPlan.IS_RELATION_ATOM;
        return priority;
    }

    @Override
    public Stream<IdPredicate> getPartialSubstitutions() {
        Set<Var> rolePlayers = getRolePlayers();
        return getPredicates(IdPredicate.class)
                .filter(pred -> rolePlayers.contains(pred.getVarName()));
    }

    public Stream<IdPredicate> getRolePredicates(){
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(var -> var.var().isUserDefinedName())
                .filter(vp -> vp.getTypeLabel().isPresent())
                .map(vp -> {
                    Label label = vp.getTypeLabel().orElse(null);
                    return new IdPredicate(vp.var(), tx().getRole(label.getValue()), getParentQuery());
                });
    }

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    private Multimap<Role, String> getRoleConceptIdMap() {
        if (roleConceptIdMap == null) {
            ImmutableMultimap.Builder<Role, String> builder = ImmutableMultimap.builder();

            Map<Var, IdPredicate> varSubMap = getPartialSubstitutions()
                    .collect(Collectors.toMap(Atomic::getVarName, pred -> pred));
            Multimap<Role, Var> roleMap = getRoleVarMap();

            roleMap.entries().stream()
                    .filter(e -> varSubMap.containsKey(e.getValue()))
                    .sorted(Comparator.comparing(e -> varSubMap.get(e.getValue()).getPredicateValue()))
                    .forEach(e -> builder.put(e.getKey(), varSubMap.get(e.getValue()).getPredicateValue()));
            this.roleConceptIdMap = builder.build();
        }
        return roleConceptIdMap;
    }

    private Multimap<Role, Type> getRoleTypeMap() {
        if (roleTypeMap == null) {
            ImmutableMultimap.Builder<Role, Type> builder = ImmutableMultimap.builder();
            Multimap<Role, Var> roleMap = getRoleVarMap();
            Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();

            roleMap.entries().stream()
                    .filter(e -> varTypeMap.containsKey(e.getValue()))
                    .sorted(Comparator.comparing(e -> varTypeMap.get(e.getValue()).getLabel()))
                    .forEach(e -> builder.put(e.getKey(), varTypeMap.get(e.getValue())));
            this.roleTypeMap = builder.build();
        }
        return roleTypeMap;
    }

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        if(ruleAtom.isResource()) return isRuleApplicableViaAtom(ruleAtom.toRelationshipAtom());
        //findbugs complains about cast without it
        if (!(ruleAtom instanceof RelationshipAtom)) return false;

        RelationshipAtom headAtom = (RelationshipAtom) ruleAtom;
        RelationshipAtom atomWithType = this.addType(headAtom.getSchemaConcept()).inferRoles(new QueryAnswer());

        //rule head atom is applicable if it is unifiable
        return headAtom.getRelationPlayers().size() >= atomWithType.getRelationPlayers().size()
                && !headAtom.getRelationPlayerMappings(atomWithType).isEmpty();
    }

    private Stream<Role> getExplicitRoles() {
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        GraknTx graph = parent.tx();

        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<Role>getSchemaConcept);
    }

    public RelationshipAtom addType(SchemaConcept type) {
        Pair<VarPatternAdmin, IdPredicate> typedPair = getTypedPair(type);
        return new RelationshipAtom(typedPair.getKey(), typedPair.getValue().getVarName(), typedPair.getValue(), this.getParentQuery());
    }

    /**
     * infer {@link RelationshipType}s that this {@link RelationshipAtom} can potentially have
     * NB: {@link EntityType}s and link {@link Role}s are treated separately as they behave differently:
     * {@link EntityType}s only play the explicitly defined {@link Role}s (not the relevant part of the hierarchy of the specified {@link Role}) and the {@link Role} inherited from parent
     * @return list of {@link RelationshipType}s this atom can have ordered by the number of compatible {@link Role}s
     */
    private Set<Type> inferPossibleEntityTypes(Answer sub){
        return inferPossibleRelationConfigurations(sub).asMap().entrySet().stream()
                .flatMap(e -> {
                    Set<Role> rs = e.getKey().relates().collect(toSet());
                    rs.removeAll(e.getValue());
                    return rs.stream().flatMap(Role::playedByTypes);
                }).collect(Collectors.toSet());
    }

    /**
     * @param sub partial answer
     * @return a map of relationships and corresponding roles that could be played by this atom
     */
    private Multimap<RelationshipType, Role> inferPossibleRelationConfigurations(Answer sub){
        Set<Role> roles = getExplicitRoles().filter(r -> !Schema.MetaSchema.isMetaLabel(r.getLabel())).collect(toSet());
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap(sub);
        Set<Type> types = getRolePlayers().stream().filter(varTypeMap::containsKey).map(varTypeMap::get).collect(toSet());

        if (roles.isEmpty() && types.isEmpty()){
            RelationshipType metaRelationType = tx().admin().getMetaRelationType();
            Multimap<RelationshipType, Role> compatibleTypes = HashMultimap.create();
            metaRelationType.subs()
                    .filter(rt -> !rt.equals(metaRelationType))
                    .forEach(rt -> compatibleTypes.putAll(rt, rt.relates().collect(toSet())));
            return compatibleTypes;
        }

        //intersect relation types from roles and types
        Multimap<RelationshipType, Role> compatibleTypes;

        Multimap<RelationshipType, Role> compatibleTypesFromRoles = compatibleRelationTypesWithRoles(roles, new RoleConverter());
        Multimap<RelationshipType, Role> compatibleTypesFromTypes = compatibleRelationTypesWithRoles(types, new TypeConverter());

        if (roles.isEmpty()){
            compatibleTypes = compatibleTypesFromTypes;
        }
        //no types from roles -> roles correspond to mutually exclusive relations
        else if(compatibleTypesFromRoles.isEmpty() || types.isEmpty()){
            compatibleTypes = compatibleTypesFromRoles;
        } else {
            compatibleTypes = multimapIntersection(compatibleTypesFromTypes, compatibleTypesFromRoles);
        }
        return compatibleTypes;
    }

    /**
     * infer {@link RelationshipType}s that this {@link RelationshipAtom} can potentially have
     * NB: {@link EntityType}s and link {@link Role}s are treated separately as they behave differently:
     * {@link EntityType}s only play the explicitly defined {@link Role}s (not the relevant part of the hierarchy of the specified {@link Role}) and the {@link Role} inherited from parent
     * @return list of {@link RelationshipType}s this atom can have ordered by the number of compatible {@link Role}s
     */
    public ImmutableList<RelationshipType> inferPossibleRelationTypes(Answer sub) {
        if (getSchemaConcept() != null) return ImmutableList.of(getSchemaConcept().asRelationshipType());
        if (possibleRelations == null) {
            Multimap<RelationshipType, Role> compatibleConfigurations = inferPossibleRelationConfigurations(sub);
            Set<Var> untypedRoleplayers = Sets.difference(getRolePlayers(), getParentQuery().getVarTypeMap().keySet());
            Set<RelationshipAtom> untypedNeighbours = getNeighbours(RelationshipAtom.class)
                    .filter(at -> !Sets.intersection(at.getVarNames(), untypedRoleplayers).isEmpty())
                    .collect(toSet());

            ImmutableList.Builder<RelationshipType> builder = ImmutableList.builder();
            //sort the types in order to prioritise relations with higher chance of yielding answers
            compatibleConfigurations.asMap().entrySet().stream()
                    //sort by number of allowed roles
                    .sorted(Comparator.comparing(e -> -e.getValue().size()))
                    .sorted(Comparator.comparing(e -> e.getKey().relates().count() != getRelationPlayers().size()))
                    .sorted(Comparator.comparing(e -> e.getKey().isImplicit()))
                    //sort by number of types untyped role players can have
                    .map(e -> {
                        if (untypedNeighbours.isEmpty()) return new Pair<>(e.getKey(), 0L);

                        Iterator<RelationshipAtom> neighbourIterator = untypedNeighbours.iterator();
                        Set<Type> typesFromNeighbour = neighbourIterator.next().inferPossibleEntityTypes(sub);
                        while(neighbourIterator.hasNext()){
                            typesFromNeighbour = Sets.intersection(typesFromNeighbour, neighbourIterator.next().inferPossibleEntityTypes(sub));
                        }

                        Set<Role> rs = e.getKey().relates().collect(toSet());
                        rs.removeAll(e.getValue());
                        return new Pair<>(
                                e.getKey(),
                                rs.stream().flatMap(Role::playedByTypes).filter(typesFromNeighbour::contains).count()
                        );

                    })
                    .sorted(Comparator.comparing(p -> -p.getValue()))
                    .map(Pair::getKey)
                    //all supers are also compatible
                    .filter(t -> Sets.intersection(supers(t), compatibleConfigurations.keySet()).isEmpty())
                    .forEach(builder::add);
            this.possibleRelations = builder.build();
        }
        return possibleRelations;
    }

    /**
     * attempt to infer the relation type of this relationship
     * @param sub extra instance information to aid entity type inference
     * @return either this if relation type can't be inferred or a fresh relationship with inferred relationship type
     */
    private RelationshipAtom inferRelationshipType(Answer sub){
        if (getTypePredicate() != null) return this;

        List<RelationshipType> relationshipTypes = inferPossibleRelationTypes(sub);
        if (relationshipTypes.size() == 1){
            return addType(relationshipTypes.iterator().next());
        } else {
            return this;
        }
    }

    @Override
    public RelationshipAtom inferTypes() {
        return this
                .inferRelationshipType(new QueryAnswer())
                .inferRoles(new QueryAnswer());
    }

    @Override
    public List<Atom> atomOptions(Answer sub) {
        return this.inferPossibleRelationTypes(sub).stream()
                .map(this::addType)
                .map(at -> at.inferRoles(sub))
                //order by number of distinct roles
                .sorted(Comparator.comparing(at -> -at.getRoleLabels().size()))
                .sorted(Comparator.comparing(Atom::isRuleResolvable))
                .collect(Collectors.toList());
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        vars.addAll(getRoleVariables());
        return vars;
    }

    @Override
    public Set<Var> getRoleExpansionVariables(){
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(p -> p.var().isUserDefinedName())
                .filter(p -> !p.getTypeLabel().isPresent())
                .map(VarPatternAdmin::var)
                .collect(Collectors.toSet());
    }

    private Set<Var> getSpecificRolePlayers() {
        return getRoleVarMap().entries().stream()
                .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey().getLabel()))
                .map(Map.Entry::getValue)
                .collect(toSet());
    }

    @Override
    public Set<TypeAtom> getSpecificTypeConstraints() {
        Set<Var> mappedVars = getSpecificRolePlayers();
        return getTypeConstraints()
                .filter(t -> mappedVars.contains(t.getVarName()))
                .filter(t -> Objects.nonNull(t.getSchemaConcept()))
                .collect(toSet());
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(
                super.getInnerPredicates(),
                getRelationPlayers().stream()
                        .map(RelationPlayer::getRole)
                        .flatMap(CommonUtil::optionalToStream)
                        .filter(vp -> vp.var().isUserDefinedName())
                        .map(vp -> new Pair<>(vp.var(), vp.getTypeLabel().orElse(null)))
                        .filter(p -> Objects.nonNull(p.getValue()))
                        .map(p -> new IdPredicate(p.getKey(), p.getValue(), getParentQuery()))
        );
    }

    /**
     * attempt to infer role types of this relation and return a fresh relationship with inferred role types
     * @return either this if nothing/no roles can be inferred or fresh relation with inferred role types
     */
    private RelationshipAtom inferRoles(Answer sub){
        //return if all roles known and non-meta
        List<Role> explicitRoles = getExplicitRoles().collect(Collectors.toList());
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap(sub);
        boolean allRolesMeta = explicitRoles.stream().allMatch(role ->
                Schema.MetaSchema.isMetaLabel(role.getLabel())
        );
        boolean roleRecomputationViable = allRolesMeta && (!sub.isEmpty() || !Sets.intersection(varTypeMap.keySet(), getRolePlayers()).isEmpty());
        if (explicitRoles.size() == getRelationPlayers().size() && !roleRecomputationViable) return this;

        GraknTx graph = getParentQuery().tx();
        Role metaRole = graph.admin().getMetaRole();
        List<RelationPlayer> allocatedRelationPlayers = new ArrayList<>();
        RelationshipType relType = getSchemaConcept() != null? getSchemaConcept().asRelationshipType() : null;

        //explicit role types from castings
        List<RelationPlayer> inferredRelationPlayer = new ArrayList<>();
        getRelationPlayers().forEach(rp -> {
            Var varName = rp.getRolePlayer().var();
            VarPatternAdmin rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                Label roleLabel = rolePattern.getTypeLabel().orElse(null);
                //allocate if variable role or if label non meta
                if (roleLabel == null || !Schema.MetaSchema.isMetaLabel(roleLabel)) {
                    inferredRelationPlayer.add(RelationPlayer.of(rolePattern, varName.admin()));
                    allocatedRelationPlayers.add(rp);
                }
            }
        });

        //remaining roles
        //role types can repeat so no matter what has been allocated still the full spectrum of possibilities is present
        //TODO make restrictions based on cardinality constraints
        Set<Role> possibleRoles = relType != null?
                relType.relates().collect(toSet()) :
                inferPossibleRelationTypes(sub).stream().flatMap(RelationshipType::relates).collect(toSet());

        //possible role types for each casting based on its type
        Map<RelationPlayer, Set<Role>> mappings = new HashMap<>();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Var varName = rp.getRolePlayer().var();
                    Type type = varTypeMap.get(varName);
                    mappings.put(rp, top(type != null? compatibleRoles(type, possibleRoles) : possibleRoles));
                });


        //allocate all unambiguous mappings
        mappings.entrySet().stream()
                .filter(entry -> entry.getValue().size() == 1)
                .forEach(entry -> {
                    RelationPlayer rp = entry.getKey();
                    Var varName = rp.getRolePlayer().var();
                    Role role = Iterables.getOnlyElement(entry.getValue());
                    VarPatternAdmin rolePattern = Graql.var().label(role.getLabel()).admin();
                    inferredRelationPlayer.add(RelationPlayer.of(rolePattern, varName.admin()));
                    allocatedRelationPlayers.add(rp);
                });

        //fill in unallocated roles with metarole
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Var varName = rp.getRolePlayer().var();
                    VarPatternAdmin rolePattern = rp.getRole().orElse(null);
                    rolePattern = rolePattern != null ?
                            rolePattern.var().label(metaRole.getLabel()).admin() :
                            Graql.var().label(metaRole.getLabel()).admin();
                    inferredRelationPlayer.add(RelationPlayer.of(rolePattern, varName.admin()));
                });

        VarPatternAdmin newPattern = relationPattern(getVarName(), inferredRelationPlayer)
                .asVarPattern()
                .isa(getPredicateVariable()).admin();
        return new RelationshipAtom(newPattern, getPredicateVariable(), getTypePredicate(), possibleRelations, getParentQuery());
    }

    /**
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    public Multimap<Role, Var> getRoleVarMap() {
        if (roleVarMap == null){
            ImmutableMultimap.Builder<Role, Var> builder = ImmutableMultimap.builder();

            GraknTx graph = getParentQuery().tx();
            getRelationPlayers().forEach(c -> {
                Var varName = c.getRolePlayer().var();
                VarPatternAdmin role = c.getRole().orElse(null);
                if (role != null) {
                    //try directly
                    Label typeLabel = role.getTypeLabel().orElse(null);
                    Role roleType = typeLabel != null ? graph.getRole(typeLabel.getValue()) : null;
                    //try indirectly
                    if (roleType == null && role.var().isUserDefinedName()) {
                        IdPredicate rolePredicate = getIdPredicate(role.var());
                        if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                    }
                    if (roleType != null) builder.put(roleType, varName);
                }
            });
            this.roleVarMap = builder.build();
        }
        return roleVarMap;
    }

    private Multimap<Role, RelationPlayer> getRoleRelationPlayerMap(){
        Multimap<Role, RelationPlayer> roleRelationPlayerMap = ArrayListMultimap.create();
        Multimap<Role, Var> roleVarMap = getRoleVarMap();
        List<RelationPlayer> relationPlayers = getRelationPlayers();
        roleVarMap.asMap().entrySet()
                .forEach(e -> {
                    Role role = e.getKey();
                    Label roleLabel = role.getLabel();
                    relationPlayers.stream()
                            .filter(rp -> rp.getRole().isPresent())
                            .forEach(rp -> {
                                VarPatternAdmin roleTypeVar = rp.getRole().orElse(null);
                                Label rl = roleTypeVar != null ? roleTypeVar.getTypeLabel().orElse(null) : null;
                                if (roleLabel != null && roleLabel.equals(rl)) {
                                    roleRelationPlayerMap.put(role, rp);
                                }
                            });
                });
        return roleRelationPlayerMap;
    }

    private Set<List<Pair<RelationPlayer, RelationPlayer>>> getRelationPlayerMappings(RelationshipAtom parentAtom) {
        return getRelationPlayerMappings(parentAtom, false);
    }

    /**
     * @param parentAtom reference atom defining the mapping
     * @return set of possible COMPLETE mappings between this (child) and parent relation players
     */
    private Set<List<Pair<RelationPlayer, RelationPlayer>>> getRelationPlayerMappings(RelationshipAtom parentAtom, boolean exact) {
        Multimap<Role, RelationPlayer> childRoleRPMap = this.getRoleRelationPlayerMap();
        Map<Var, Type> childVarTypeMap = this.getParentQuery().getVarTypeMap();
        Map<Var, Type> parentVarTypeMap = parentAtom.getParentQuery().getVarTypeMap();


        //establish compatible castings for each parent casting
        List<Set<Pair<RelationPlayer, RelationPlayer>>> compatibleMappingsPerParentRP = new ArrayList<>();
        ReasonerQueryImpl childQuery = (ReasonerQueryImpl) getParentQuery();
        Set<Role> childRoles = childRoleRPMap.keySet();
        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRole().isPresent())
                .forEach(prp -> {
                    VarPatternAdmin parentRolePattern = prp.getRole().orElse(null);
                    if (parentRolePattern == null){
                        throw GraqlQueryException.rolePatternAbsent(this);
                    }
                    Label parentRoleLabel = parentRolePattern.getTypeLabel().orElse(null);

                    if (parentRoleLabel != null) {
                        Var parentRolePlayer = prp.getRolePlayer().var();
                        Type parentType = parentVarTypeMap.get(parentRolePlayer);

                        Set<Role> compatibleChildRoles = compatibleRoles(
                                tx().getSchemaConcept(parentRoleLabel),
                                parentType,
                                childRoles);

                        List<RelationPlayer> compatibleRelationPlayers = new ArrayList<>();
                        compatibleChildRoles.stream()
                                .filter(childRoleRPMap::containsKey)
                                .forEach(role -> {
                                    childRoleRPMap.get(role).stream()
                                            //check for inter-type compatibility
                                            .filter(crp -> {
                                                Var childVar = crp.getRolePlayer().var();
                                                Type childType = childVarTypeMap.get(childVar);
                                                if (exact) return childQuery.isTypeRoleCompatible(childVar, parentType) && !areDisjointTypes(parentType, childType);
                                                else return childQuery.isTypeRoleCompatible(childVar, parentType)
                                                        && (childType == null || !areDisjointTypes(parentType, childType));
                                            })
                                            //check for substitution compatibility
                                            .filter(crp ->
                                                    predicatesCompatible(
                                                            parentAtom.getIdPredicate(prp.getRolePlayer().var()),
                                                            this.getIdPredicate(crp.getRolePlayer().var()),
                                                            exact)
                                            )
                                            //check for value predicate compatibility
                                            .filter(crp ->
                                                    predicatesCompatible(
                                                            parentAtom.getPredicate(prp.getRolePlayer().var(), ValuePredicate.class),
                                                            this.getPredicate(crp.getRolePlayer().var(), ValuePredicate.class),
                                                            exact)
                                            )
                                            .forEach(compatibleRelationPlayers::add);
                                });
                        if (!compatibleRelationPlayers.isEmpty()) {
                            compatibleMappingsPerParentRP.add(
                                    compatibleRelationPlayers.stream()
                                            .map(crp -> new Pair<>(crp, prp))
                                            .collect(Collectors.toSet())
                            );
                        }
                    } else {
                        compatibleMappingsPerParentRP.add(
                                getRelationPlayers().stream()
                                        .map(crp -> new Pair<>(crp, prp))
                                        .collect(Collectors.toSet())
                        );
                    }
                });

        return Sets.cartesianProduct(compatibleMappingsPerParentRP).stream()
                .filter(list -> !list.isEmpty())
                //check the same child rp is not mapped to multiple parent rps
                .filter(list -> {
                    List<RelationPlayer> listChildRps = list.stream().map(Pair::getKey).collect(Collectors.toList());
                    //NB: this preserves cardinality instead of removing all occuring instances which is what we want
                    return ReasonerUtils.subtract(listChildRps, this.getRelationPlayers()).isEmpty();
                })
                //check all parent rps mapped
                .filter(list -> {
                    List<RelationPlayer> listParentRps = list.stream().map(Pair::getValue).collect(Collectors.toList());
                    return listParentRps.containsAll(parentAtom.getRelationPlayers());
                })
                .collect(toSet());
    }

    @Override
    public Unifier getUnifier(Atom pAtom){
        return getMultiUnifier(pAtom, true).getUnifier();
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom pAtom, boolean exact) {
        if (this.equals(pAtom))  return new MultiUnifierImpl();

        Unifier baseUnifier = super.getUnifier(pAtom);
        Set<Unifier> unifiers = new HashSet<>();
        if (pAtom.isRelation()) {
            assert(pAtom instanceof RelationshipAtom); // This is safe due to the check above
            RelationshipAtom parentAtom = (RelationshipAtom) pAtom;

            boolean unifyRoleVariables = parentAtom.getRelationPlayers().stream()
                    .map(RelationPlayer::getRole)
                    .flatMap(CommonUtil::optionalToStream)
                    .filter(rp -> rp.var().isUserDefinedName())
                    .findFirst().isPresent();
            getRelationPlayerMappings(parentAtom, exact)
                    .forEach(mappings -> {
                        Unifier unifier = new UnifierImpl(baseUnifier);
                        mappings.forEach(rpm -> {
                            //add role player mapping
                            unifier.addMapping(rpm.getKey().getRolePlayer().var(), rpm.getValue().getRolePlayer().var());

                            //add role var mapping if needed
                            VarPattern childRolePattern = rpm.getKey().getRole().orElse(null);
                            VarPattern parentRolePattern = rpm.getValue().getRole().orElse(null);
                            if (parentRolePattern != null && childRolePattern != null && unifyRoleVariables){
                                unifier.addMapping(childRolePattern.admin().var(), parentRolePattern.admin().var());
                            }

                        });
                        unifiers.add(unifier);
                    });
        } else {
            unifiers.add(baseUnifier);
        }
        return new MultiUnifierImpl(unifiers);
    }

    /**
     * if any {@link Role} variable of the parent is user defined rewrite ALL {@link Role} variables to user defined (otherwise unification is problematic)
     * @param parentAtom parent atom that triggers rewrite
     * @return new relation atom with user defined {@link Role} variables if necessary or this
     */
    private RelationshipAtom rewriteWithVariableRoles(Atom parentAtom){
        if (!parentAtom.requiresRoleExpansion()) return this;

        VarPattern relVar = getPattern().asVarPattern().getProperty(IsaProperty.class)
                .map(prop -> getVarName().isa(prop.type())).orElse(getVarName());

        for (RelationPlayer rp: getRelationPlayers()) {
            VarPatternAdmin rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                Var roleVar = rolePattern.var();
                Label roleLabel = rolePattern.getTypeLabel().orElse(null);
                relVar = relVar.rel(roleVar.asUserDefined().label(roleLabel), rp.getRolePlayer());
            } else {
                relVar = relVar.rel(rp.getRolePlayer());
            }
        }
        return new RelationshipAtom(relVar.admin(), getPredicateVariable(), getTypePredicate(), getParentQuery());
    }

    /**
     * @param parentAtom parent atom that triggers rewrite
     * @return new relation atom with user defined name if necessary or this
     */
    private RelationshipAtom rewriteWithRelationVariable(Atom parentAtom){
        if (!parentAtom.getVarName().isUserDefinedName()) return this;

        VarPattern newVar = Graql.var().asUserDefined();
        VarPattern relVar = getPattern().asVarPattern().getProperty(IsaProperty.class)
                .map(prop -> newVar.isa(prop.type()))
                .orElse(newVar);

        for (RelationPlayer c: getRelationPlayers()) {
            VarPatternAdmin roleType = c.getRole().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, c.getRolePlayer());
            } else {
                relVar = relVar.rel(c.getRolePlayer());
            }
        }
        return new RelationshipAtom(relVar.admin(), getPredicateVariable(), getTypePredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        return this
                .rewriteWithRelationVariable(parentAtom)
                .rewriteWithVariableRoles(parentAtom);
    }
}
