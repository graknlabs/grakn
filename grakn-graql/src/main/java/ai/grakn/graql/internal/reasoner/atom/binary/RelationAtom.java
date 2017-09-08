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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.type.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.utils.conversion.RoleTypeConverter;
import ai.grakn.graql.internal.reasoner.utils.conversion.SchemaConceptConverterImpl;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.checkDisjoint;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getCompatibleRelationTypesWithRoles;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getListPermutations;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getSupers;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getUnifiersFromPermutations;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.multimapIntersection;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Atom implementation defining a relation atom corresponding to a combined {@link RelationProperty}
 * and (optional) {@link IsaProperty}. The relation atom is a {@link TypeAtom} with relationship players.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RelationAtom extends IsaAtom {

    private int hashCode = 0;
    private Multimap<Role, Var> roleVarMap = null;
    private Multimap<Role, String> roleConceptIdMap = null;
    private final ImmutableList<RelationPlayer> relationPlayers;

    public RelationAtom(VarPatternAdmin pattern, Var predicateVar, @Nullable IdPredicate predicate, ReasonerQuery par) {
        super(pattern, predicateVar, predicate, par);
        this.relationPlayers = ImmutableList.copyOf(getRelationPlayers());
    }

    private RelationAtom(RelationAtom a) {
        super(a);
        this.relationPlayers = a.relationPlayers;
        this.roleVarMap = a.roleVarMap;
    }

    @Override
    public String toString(){
        String relationString = (isUserDefined()? getVarName() + " ": "") +
                (getSchemaConcept() != null? getSchemaConcept().getLabel() : "") +
                getRelationPlayers().toString();
        return relationString + getPredicates(IdPredicate.class).map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    private List<RelationPlayer> getRelationPlayers() {
        List<RelationPlayer> rps = new ArrayList<>();
        getPattern().asVarPattern()
                .getProperty(RelationProperty.class)
                .ifPresent(prop -> prop.relationPlayers().forEach(rps::add));
        return rps;
    }

    private Set<Label> getRoleLabels() {
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    /**
     * @return set constituting the role player var names
     */
    public Set<Var> getRolePlayers() {
        return getRelationPlayers().stream().map(c -> c.getRolePlayer().var()).collect(toSet());
    }

    @Override
    public Atomic copy() {
        return new RelationAtom(this);
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     *
     * @param varName            variable name
     * @param typeVariable       type variable name
     * @param rolePlayerMappings list of rolePlayer-roleType mappings
     * @return corresponding {@link VarPatternAdmin}
     */
    private static VarPatternAdmin constructRelationVarPattern(Var varName, Var typeVariable, List<Pair<Var, VarPattern>> rolePlayerMappings) {
        VarPattern var = !varName.getValue().isEmpty()? varName : Graql.var();
        for (Pair<Var, VarPattern> mapping : rolePlayerMappings) {
            Var rp = mapping.getKey();
            VarPattern role = mapping.getValue();
            var = role == null? var.rel(rp) : var.rel(role, rp);
        }
        if (!typeVariable.getValue().isEmpty()) var = var.isa(typeVariable);
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
        RelationAtom a2 = (RelationAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarNames().equals(a2.getVarNames())
                && getRelationPlayers().equals(a2.getRelationPlayers());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationAtom a2 = (RelationAtom) obj;
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
    public boolean isAllowedToFormRuleHead(){
        //can form a rule head if specified type and all relation players have a specified/unambiguously inferrable role type
        return super.isAllowedToFormRuleHead()
                && !hasMetaRoles();
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        //add user specified role type vars
        getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(v -> v.var().isUserDefinedName())
                .forEach(r -> vars.add(r.var()));
        return vars;
    }

    @Override
    public Set<String> validateOntologically() {
        Set<String> errors = new HashSet<>();
        SchemaConcept type = getSchemaConcept();
        if (type != null && !type.isRelationshipType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage(type.getLabel()));
            return errors;
        }

        //check roles are ok
        Map<Var, SchemaConcept> varSchemaConceptMap = getParentQuery().getVarSchemaConceptMap();

        for (Map.Entry<Role, Collection<Var>> e : getRoleVarMap().asMap().entrySet() ){
            Role role = e.getKey();
            if (!Schema.MetaSchema.isMetaLabel(role.getLabel())) {
                //check whether this role can be played in this relation
                if (type != null && type.asRelationshipType().relates().noneMatch(r -> r.equals(role))) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage(role.getLabel(), type.getLabel()));
                }

                //check whether the role player's type allows playing this role
                for (Var player : e.getValue()) {
                    SchemaConcept playerType = varSchemaConceptMap.get(player);
                    if (playerType != null && playerType.asType().plays().noneMatch(plays -> plays.equals(role))) {
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

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    private Multimap<Role, String> getRoleConceptIdMap() {
        if (roleConceptIdMap == null) {
            roleConceptIdMap = ArrayListMultimap.create();

            Map<Var, IdPredicate> varSubMap = getPartialSubstitutions()
                    .collect(Collectors.toMap(Atomic::getVarName, pred -> pred));
            Multimap<Role, Var> roleMap = getRoleVarMap();

            roleMap.entries().stream()
                    .filter(e -> varSubMap.containsKey(e.getValue()))
                    .sorted(Comparator.comparing(e -> varSubMap.get(e.getValue()).getPredicateValue()))
                    .forEach(e -> roleConceptIdMap.put(e.getKey(), varSubMap.get(e.getValue()).getPredicateValue()));
        }
        return roleConceptIdMap;
    }

    private Multimap<Role, SchemaConcept> getRoleTypeMap() {
        Multimap<Role, SchemaConcept> roleTypeMap = ArrayListMultimap.create();
        Multimap<Role, Var> roleMap = getRoleVarMap();
        Map<Var, SchemaConcept> varTypeMap = getParentQuery().getVarSchemaConceptMap();

        roleMap.entries().stream()
                .filter(e -> varTypeMap.containsKey(e.getValue()))
                .sorted(Comparator.comparing(e -> varTypeMap.get(e.getValue()).getLabel()))
                .forEach(e -> roleTypeMap.put(e.getKey(), varTypeMap.get(e.getValue())));
        return roleTypeMap;
    }

    //rule head atom is applicable if it is unifiable
    private boolean isRuleApplicableViaAtom(RelationAtom headAtom) {
        return headAtom.getRelationPlayers().size() >= this.getRelationPlayers().size()
                && headAtom.getRelationPlayerMappings(this).size() == this.getRelationPlayers().size();
    }

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if (!(ruleAtom.isRelation())) return false;

        RelationAtom headAtom = (RelationAtom) ruleAtom;
        RelationAtom atomWithType = this.addType(headAtom.getSchemaConcept()).inferRoleTypes();
        return atomWithType.isRuleApplicableViaAtom(headAtom);
    }

    /**
     * @return true if any of the relation's role types are meta role types
     */
    private boolean hasMetaRoles(){
        Set<Role> parentRoles = getRoleVarMap().keySet();
        for(Role role : parentRoles) {
            if (Schema.MetaSchema.isMetaLabel(role.getLabel())) return true;
        }
        return false;
    }

    private Set<Role> getExplicitRoleTypes() {
        Set<Role> roles = new HashSet<>();
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        GraknTx graph = parent.tx();

        Set<VarPatternAdmin> roleVars = getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .collect(Collectors.toSet());
        //try directly
        roleVars.stream()
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<Role>getSchemaConcept)
                .forEach(roles::add);

        //try indirectly
        roleVars.stream()
                .filter(v -> v.var().isUserDefinedName())
                .map(VarPatternAdmin::var)
                .map(this::getIdPredicate)
                .filter(Objects::nonNull)
                .map(Predicate::getPredicate)
                .map(graph::<Role>getConcept)
                .forEach(roles::add);
        return roles;
    }

    /**
     * @param type to be added to this relation
     * @return new relation with specified type
     */
    public RelationAtom addType(SchemaConcept type) {
        ConceptId typeId = type.getId();
        Var typeVariable = getPredicateVariable().getValue().isEmpty() ? Graql.var().asUserDefined() : getPredicateVariable();

        VarPatternAdmin newPattern = getPattern().asVarPattern().isa(typeVariable).admin();
        IdPredicate newPredicate = new IdPredicate(typeVariable.id(typeId).admin(), getParentQuery());

        return new RelationAtom(newPattern, typeVariable, newPredicate, this.getParentQuery());
    }

    /**
     * @param sub answer
     * @return entity types inferred from answer entity information
     */
    private Set<Type> inferEntityTypes(Answer sub) {
        if (sub.isEmpty()) return Collections.emptySet();

        Set<Var> subbedVars = Sets.intersection(getRolePlayers(), sub.vars());
        Set<Var> untypedVars = Sets.difference(subbedVars, getParentQuery().getVarSchemaConceptMap().keySet());
        return untypedVars.stream()
                .map(v -> new Pair<>(v, sub.get(v)))
                .filter(p -> p.getValue().isThing())
                .map(e -> {
                    Concept c = e.getValue();
                    return c.asThing().type();
                })
                .collect(toSet());
    }

    /**
     * infer relation types that this relationship atom can potentially have
     * NB: entity types and role types are treated separately as they behave differently:
     * entity types only play the explicitly defined roles (not the relevant part of the hierarchy of the specified role)
     * @return list of relation types this atom can have ordered by the number of compatible role types
     */
    public List<RelationshipType> inferPossibleRelationTypes(Answer sub) {
        if (getTypePredicate() != null) return Collections.singletonList(getSchemaConcept().asRelationshipType());

        //look at available role types
        Multimap<RelationshipType, Role> compatibleTypesFromRoles = getCompatibleRelationTypesWithRoles(getExplicitRoleTypes(), new RoleTypeConverter());

        //look at entity types
        Map<Var, SchemaConcept> varTypeMap = getParentQuery().getVarSchemaConceptMap();

        //explicit types
        Set<SchemaConcept> types = getRolePlayers().stream()
                .filter(varTypeMap::containsKey)
                .map(varTypeMap::get)
                .collect(toSet());

        //types deduced from substitution
        inferEntityTypes(sub).forEach(types::add);

        Multimap<RelationshipType, Role> compatibleTypesFromTypes = getCompatibleRelationTypesWithRoles(types, new SchemaConceptConverterImpl());

        Multimap<RelationshipType, Role> compatibleTypes;
        //intersect relation types from roles and types
        if (compatibleTypesFromRoles.isEmpty()){
            compatibleTypes = compatibleTypesFromTypes;
        } else if (!compatibleTypesFromTypes.isEmpty()){
            compatibleTypes = multimapIntersection(compatibleTypesFromTypes, compatibleTypesFromRoles);
        } else {
            compatibleTypes = compatibleTypesFromRoles;
        }

        return compatibleTypes.asMap().entrySet().stream()
                .sorted(Comparator.comparing(e -> -e.getValue().size()))
                .map(Map.Entry::getKey)
                .filter(t -> Sets.intersection(getSupers(t), compatibleTypes.keySet()).isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * attempt to infer the relation type of this relationship
     * @param sub extra instance information to aid entity type inference
     * @return either this if relation type can't be inferred or a fresh relationship with inferred relationship type
     */
    private RelationAtom inferRelationType(Answer sub){
        if (getTypePredicate() != null) return this;

        List<RelationshipType> relationshipTypes = inferPossibleRelationTypes(sub);
        if (relationshipTypes.size() == 1){
            return addType(relationshipTypes.iterator().next());
        } else {
            return this;
        }
    }

    @Override
    public Atom inferTypes() {
        return this
                .inferRelationType(new QueryAnswer())
                .inferRoleTypes();
    }

    private Set<Var> getSpecificRolePlayers() {
        return getRoleVarMap().entries().stream()
                .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey().getLabel()))
                .map(Map.Entry::getValue)
                .collect(toSet());
    }

    /**
     * @return set constituting the role player var names that do not have a specified role type
     */
    private Set<Var> getNonSpecificRolePlayers() {
        Set<Var> unmappedVars = getRolePlayers();
        unmappedVars.removeAll(getSpecificRolePlayers());
        return unmappedVars;
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
    public Set<Unifier> getPermutationUnifiers(Atom headAtom) {
        if (!headAtom.isRelation()) return Collections.singleton(new UnifierImpl());

        //if this atom is a match all atom, add type from rule head and find unmapped roles
        RelationAtom relAtom = getPredicateVariable().getValue().isEmpty() ? this.addType(headAtom.getSchemaConcept()) : this;
        List<Var> permuteVars = new ArrayList<>(relAtom.getNonSpecificRolePlayers());
        if (permuteVars.isEmpty()) return Collections.singleton(new UnifierImpl());

        List<List<Var>> varPermutations = getListPermutations(
                new ArrayList<>(permuteVars)).stream()
                .filter(l -> !l.isEmpty())
                .collect(Collectors.toList()
                );
        return getUnifiersFromPermutations(permuteVars, varPermutations);
    }

    /**
     * attempt to infer role types of this relation and return a fresh relationship with inferred role types
     * @return either this if nothing/no roles can be inferred or fresh relation with inferred role types
     */
    private RelationAtom inferRoleTypes(){
        if (getExplicitRoleTypes().size() == getRelationPlayers().size() || getSchemaConcept() == null) return this;

        GraknTx graph = getParentQuery().tx();
        Role metaRole = graph.admin().getMetaRole();
        RelationshipType relType = (RelationshipType) getSchemaConcept();
        Map<Var, SchemaConcept> varSchemaConceptMap = getParentQuery().getVarSchemaConceptMap();

        List<RelationPlayer> allocatedRelationPlayers = new ArrayList<>();

        //explicit role types from castings
        List<Pair<Var, VarPattern>> rolePlayerMappings = new ArrayList<>();
        getRelationPlayers().forEach(c -> {
            Var varName = c.getRolePlayer().var();
            VarPatternAdmin role = c.getRole().orElse(null);
            if (role != null) {
                rolePlayerMappings.add(new Pair<>(varName, role));
                //try directly
                Label typeLabel = role.getTypeLabel().orElse(null);
                Role roleType = typeLabel != null ? graph.getRole(typeLabel.getValue()) : null;

                //try indirectly
                if (roleType == null && role.var().isUserDefinedName()) {
                    IdPredicate rolePredicate = getIdPredicate(role.var());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                }
                allocatedRelationPlayers.add(c);
            }
        });

        //remaining roles
        //role types can repeat so no matter what has been allocated still the full spectrum of possibilities is present
        //TODO make restrictions based on cardinality constraints
        Set<Role> possibleRoles = relType.relates().collect(toSet());

        //possible role types for each casting based on its type
        Map<RelationPlayer, Set<Role>> mappings = new HashMap<>();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(casting -> {
                    Var varName = casting.getRolePlayer().var();
                    SchemaConcept schemaConcept = varSchemaConceptMap.get(varName);
                    if (schemaConcept != null && !Schema.MetaSchema.isMetaLabel(schemaConcept.getLabel()) && schemaConcept.isType()) {
                        mappings.put(casting, ReasonerUtils.getCompatibleRoleTypes(schemaConcept.asType(), possibleRoles.stream()));
                    } else {
                        mappings.put(casting, ReasonerUtils.getSchemaConcepts(possibleRoles));
                    }
                });


        //resolve ambiguities until no unambiguous mapping exist
        while( mappings.values().stream().filter(s -> s.size() == 1).count() != 0) {
            Map.Entry<RelationPlayer, Set<Role>> entry = mappings.entrySet().stream()
                    .filter(e -> e.getValue().size() == 1)
                    .findFirst().orElse(null);

            RelationPlayer casting = entry.getKey();
            Var varName = casting.getRolePlayer().var();
            Role role = entry.getValue().iterator().next();
            VarPatternAdmin roleVar = Graql.var().label(role.getLabel()).admin();

            //TODO remove from all mappings if it follows from cardinality constraints
            mappings.get(casting).remove(role);

            rolePlayerMappings.add(new Pair<>(varName, roleVar));
            allocatedRelationPlayers.add(casting);
        }

        //fill in unallocated roles with metarole
        VarPatternAdmin metaRoleVar = Graql.var().label(metaRole.getLabel()).admin();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(casting -> {
                    Var varName = casting.getRolePlayer().var();
                    rolePlayerMappings.add(new Pair<>(varName, metaRoleVar));
                });

        PatternAdmin newPattern = constructRelationVarPattern(getVarName(), getPredicateVariable(), rolePlayerMappings);
        return new RelationAtom(newPattern.asVarPattern(), getPredicateVariable(), getTypePredicate(), getParentQuery());
    }

    /**
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    private Multimap<Role, Var> computeRoleVarMap() {
        Multimap<Role, Var> roleMap = ArrayListMultimap.create();
        if (getParentQuery() == null || getSchemaConcept() == null){ return roleMap;}

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
                if (roleType != null) roleMap.put(roleType, varName);
            }
        });
        return roleMap;
    }

    public Multimap<Role, Var> getRoleVarMap() {
        if (roleVarMap == null){
            roleVarMap = computeRoleVarMap();
        }
        return roleVarMap;
    }

    private Multimap<Role, RelationPlayer> getRoleRelationPlayerMap(){
        Multimap<Role, RelationPlayer> roleRelationPlayerMap = ArrayListMultimap.create();
        Multimap<Role, Var> roleVarTypeMap = getRoleVarMap();
        List<RelationPlayer> relationPlayers = getRelationPlayers();
        roleVarTypeMap.asMap().entrySet()
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

    private List<Pair<RelationPlayer, RelationPlayer>> getRelationPlayerMappings(RelationAtom parentAtom) {
        List<Pair<RelationPlayer, RelationPlayer>> rolePlayerMappings = new ArrayList<>();

        //establish compatible castings for each parent casting
        List<Pair<RelationPlayer, List<RelationPlayer>>> compatibleMappings = new ArrayList<>();
        parentAtom.getRoleRelationPlayerMap();
        Multimap<Role, RelationPlayer> childRoleRPMap = getRoleRelationPlayerMap();
        Map<Var, SchemaConcept> parentVarSchemaConceptMap = parentAtom.getParentQuery().getVarSchemaConceptMap();
        Map<Var, SchemaConcept> childVarSchemaConceptMap = this.getParentQuery().getVarSchemaConceptMap();

        Set<Role> childRoles = new HashSet<>(childRoleRPMap.keySet());

        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRole().isPresent())
                .forEach(prp -> {
                    VarPatternAdmin parentRoleTypeVar = prp.getRole().orElse(null);
                    Label parentRoleLabel = parentRoleTypeVar.getTypeLabel().orElse(null);

                    //TODO take into account indirect roles
                    Role parentRole = parentRoleLabel != null ? tx().getSchemaConcept(parentRoleLabel) : null;

                    if (parentRole != null) {
                        boolean isMetaRole = Schema.MetaSchema.isMetaLabel(parentRole.getLabel());
                        Var parentRolePlayer = prp.getRolePlayer().var();
                        SchemaConcept parent = parentVarSchemaConceptMap.get(parentRolePlayer);

                        Set<Role> compatibleChildRoles = isMetaRole? childRoles : Sets.intersection(parentRole.subs().collect(toSet()), childRoles);

                        if (parent != null && parent.isType()){
                            boolean isMetaType = Schema.MetaSchema.isMetaLabel(parent.getLabel());
                            Set<Role> typeRoles = isMetaType? childRoles : parent.asType().plays().collect(toSet());

                            //incompatible type
                            if (Sets.intersection(getSchemaConcept().asRelationshipType().relates().collect(toSet()), typeRoles).isEmpty()) compatibleChildRoles = new HashSet<>();
                            else {
                                compatibleChildRoles = compatibleChildRoles.stream()
                                        .filter(rc -> Schema.MetaSchema.isMetaLabel(rc.getLabel()) || typeRoles.contains(rc))
                                        .collect(toSet());
                            }
                        }

                        List<RelationPlayer> compatibleRelationPlayers = new ArrayList<>();
                        compatibleChildRoles.stream()
                                .filter(childRoleRPMap::containsKey)
                                .forEach(r -> {
                                    Collection<RelationPlayer> childRPs = parent != null ?
                                            childRoleRPMap.get(r).stream()
                                                    .filter(rp -> {
                                                        Var childRolePlayer = rp.getRolePlayer().var();
                                                        SchemaConcept childType = childVarSchemaConceptMap.get(childRolePlayer);
                                                        return childType == null || !checkDisjoint(parent, childType);
                                                    }).collect(Collectors.toList()) :
                                            childRoleRPMap.get(r);

                                    childRPs.forEach(compatibleRelationPlayers::add);
                                });
                        compatibleMappings.add(new Pair<>(prp, compatibleRelationPlayers));
                    }
                });

        //self-consistent procedure until no non-empty mappings present
        while( compatibleMappings.stream().map(Pair::getValue).filter(s -> !s.isEmpty()).count() > 0) {
            //find optimal parent-child RP pair
            Pair<RelationPlayer, RelationPlayer> rpPair = compatibleMappings.stream()
                    .filter(e -> e.getValue().size() == 1).map(e -> new Pair<>(e.getKey(), e.getValue().iterator().next()))
                    .findFirst().orElse(
                            compatibleMappings.stream()
                                    .flatMap(e -> e.getValue().stream().map(childRP -> new Pair<>(e.getKey(), childRP)))
                                    //prioritise mappings with equivalent types and unambiguous mappings
                                    .sorted(Comparator.comparing(e -> {
                                        SchemaConcept parentType = parentVarSchemaConceptMap.get(e.getKey().getRolePlayer().var());
                                        SchemaConcept childType = childVarSchemaConceptMap.get(e.getValue().getRolePlayer().var());
                                        return !(parentType != null && childType != null && parentType.equals(childType));
                                    }))
                                    //prioritise mappings with sam var substitution (idpredicates)
                                    .sorted(Comparator.comparing(e -> {
                                        IdPredicate parentId = parentAtom.getPredicates(IdPredicate.class)
                                                .filter(p -> p.getVarName().equals(e.getKey().getRolePlayer().var()))
                                                .findFirst().orElse(null);
                                        IdPredicate childId = getPredicates(IdPredicate.class)
                                                .filter(p -> p.getVarName().equals(e.getValue().getRolePlayer().var()))
                                                .findFirst().orElse(null);
                                        return !(parentId != null && childId != null && parentId.getPredicate().equals(childId.getPredicate()));
                                    }))
                                    .findFirst().orElse(null)
                    );

            RelationPlayer parentCasting = rpPair.getKey();
            RelationPlayer childCasting = rpPair.getValue();
            rolePlayerMappings.add(new Pair<>(childCasting, parentCasting));

            //remove corresponding entries
            Pair<RelationPlayer, List<RelationPlayer>> entryToRemove = compatibleMappings.stream()
                    .filter(e -> e.getKey() == parentCasting)
                    .findFirst().orElse(null);
            compatibleMappings.remove(entryToRemove);
            compatibleMappings.stream()
                    .filter(e -> e.getValue().contains(childCasting))
                    .forEach(e -> e.getValue().remove(childCasting));
        }
        return rolePlayerMappings;
    }

    @Override
    public Atom rewriteToUserDefined(){
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
        return new RelationAtom(relVar.admin(), getPredicateVariable(), getTypePredicate(), getParentQuery());
    }

    @Override
    public Unifier getUnifier(Atom pAtom) {
        if (this.equals(pAtom)) return new UnifierImpl();

        Unifier unifier = super.getUnifier(pAtom);
        if (pAtom.isRelation()) {
            RelationAtom parentAtom = (RelationAtom) pAtom;

            getRelationPlayerMappings(parentAtom)
                    .forEach(rpm -> unifier.addMapping(rpm.getKey().getRolePlayer().var(), rpm.getValue().getRolePlayer().var()));
        }
        return unifier.removeTrivialMappings();
    }
}
