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

import ai.grakn.GraknGraph;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.AtomicMatchQuery;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.AbstractMap;
import java.util.stream.Collectors;
import javafx.util.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.graql.internal.reasoner.Utility.CAPTURE_MARK;
import static ai.grakn.graql.internal.reasoner.Utility.checkTypesCompatible;
import static ai.grakn.graql.internal.reasoner.Utility.getCompatibleRelationTypes;
import static ai.grakn.graql.internal.reasoner.Utility.getNonMetaTopRole;
import static ai.grakn.graql.internal.reasoner.Utility.roleToRelationTypes;
import static ai.grakn.graql.internal.reasoner.Utility.typeToRelationTypes;


/**
 *
 * <p>
 * Atom implementation defining a relation atom.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Relation extends TypeAtom {

    private Set<RelationPlayer> relationPlayers;
    private Map<RoleType, Pair<String, Type>> roleVarTypeMap = null;
    private Map<String, Pair<Type, RoleType>> varTypeRoleMap = null;

    public Relation(VarAdmin pattern) {
        this(pattern, null, null);
    }
    public Relation(VarAdmin pattern, Query par) {
        this(pattern, null, par);
    }
    public Relation(VarAdmin pattern, Predicate predicate, Query par) {
        super(pattern, predicate, par);
        this.relationPlayers = getRelationPlayers(pattern);
    }

    public Relation(String name, String typeVariable, Map<String, Var> roleMap, Predicate pred, Query par) {
        super(constructRelationVar(name, typeVariable, roleMap), pred, par);
        this.relationPlayers = getRelationPlayers(getPattern().asVar());
    }

    private Relation(Relation a) {
        super(a);
        this.relationPlayers = getRelationPlayers();
        this.roleVarTypeMap = a.roleVarTypeMap != null? Maps.newHashMap(a.roleVarTypeMap) : null;
        this.varTypeRoleMap = a.varTypeRoleMap != null? Maps.newHashMap(a.varTypeRoleMap) : null;
    }

    private Set<RelationPlayer> getRelationPlayers() {
        return getRelationPlayers(this.atomPattern.asVar());
    }
    private Set<RelationPlayer> getRelationPlayers(VarAdmin pattern) {
        Set<RelationPlayer> rps = new HashSet<>();
        pattern.getProperty(RelationProperty.class)
                .ifPresent(prop -> prop.getRelationPlayers().forEach(rps::add));
        return rps;
    }

    @Override
    protected String extractValueVariableName(VarAdmin var) {
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        return isaProp != null ? isaProp.getType().getVarName() : "";
    }

    @Override
    protected void setValueVariable(String var) {
        IsaProperty isaProp = atomPattern.asVar().getProperty(IsaProperty.class).orElse(null);
        if (isaProp != null) {
            super.setValueVariable(var);
            atomPattern.asVar().getProperties(IsaProperty.class).forEach(prop -> prop.getType().setVarName(var));
        }
    }

    @Override
    public Atomic clone() {
        return new Relation(this);
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     * @param varName variable name
     * @param typeVariable type variable name
     * @param roleMap      rolePlayer-roleType roleMap
     * @return corresponding Var
     */
    private static VarAdmin constructRelationVar(String varName, String typeVariable, Map<String, Var> roleMap) {
        Var var;
        if (!varName.isEmpty()) var = Graql.var(varName);
        else var = Graql.var();
        roleMap.forEach((player, role) -> {
            if (role == null) var.rel(player);
            else var.rel(role, player);
        });
        var.isa(Graql.var(typeVariable));
        return var.admin().asVar();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        return this.getTypeId().equals(a2.getTypeId())
                && this.getVarNames().equals(a2.getVarNames())
                && relationPlayers.equals(a2.relationPlayers);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + getTypeId().hashCode();
        hashCode = hashCode * 37 + getVarNames().hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        Map<RoleType, String> map = getRoleConceptIdMap();
        Map<RoleType, String> map2 = a2.getRoleConceptIdMap();
        return this.getTypeId().equals(a2.getTypeId()) && map.equals(map2);
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.getRoleConceptIdMap().hashCode();
        return hashCode;
    }

    @Override
    public boolean isRelation() {
        return true;
    }

    @Override
    public boolean isSelectable() {
        return true;
    }

    private boolean isRuleApplicableViaType(RelationType relType) {
        boolean ruleRelevant = true;
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();
        Iterator<Map.Entry<String, Type>> it = varTypeMap.entrySet().stream()
                .filter(entry -> containsVar(entry.getKey())).iterator();
        while (it.hasNext() && ruleRelevant) {
            Map.Entry<String, Type> entry = it.next();
            Type type = entry.getValue();
            if (type != null) {
                Collection<RoleType> roleIntersection = relType.hasRoles();
                roleIntersection.retainAll(type.playsRoles());
                ruleRelevant = !roleIntersection.isEmpty();
            }
        }
        return ruleRelevant;
    }

    private boolean isRuleApplicableViaAtom(Atom childAtom, InferenceRule child) {
        boolean ruleRelevant = true;
        Query parent = getParentQuery();
        Map<RoleType, Pair<String, Type>> childRoleVarTypeMap = childAtom.getRoleVarTypeMap();
        Map<RoleType, Pair<String, Type>> parentRoleVarTypeMap = getRoleVarTypeMap();

        Iterator<Map.Entry<RoleType, Pair<String, Type>>> it = parentRoleVarTypeMap.entrySet().iterator();
        while (it.hasNext() && ruleRelevant) {
            Map.Entry<RoleType, Pair<String, Type>> entry = it.next();
            RoleType parentRole = entry.getKey();

            //check roletypes compatible
            Iterator<RoleType> childRolesIt = childRoleVarTypeMap.keySet().iterator();
            //if child roles are unspecified then compatible
            boolean roleCompatible = !childRolesIt.hasNext();
            while (childRolesIt.hasNext() && !roleCompatible)
                roleCompatible = checkTypesCompatible(parentRole, childRolesIt.next());
            ruleRelevant = roleCompatible;

            //check type compatibility
            Type pType = entry.getValue().getValue();
            if (pType != null && ruleRelevant) {
                //vars can be matched by role types
                if (childRoleVarTypeMap.containsKey(parentRole)) {
                    Type chType = childRoleVarTypeMap.get(parentRole).getValue();
                    //check type compatibility
                    if (chType != null) {
                        ruleRelevant = checkTypesCompatible(pType, chType);

                        //Check for any constraints on the variables
                        String chVar = childRoleVarTypeMap.get(parentRole).getKey();
                        String pVar = entry.getValue().getKey();
                        Predicate childPredicate = child.getBody().getIdPredicate(chVar);
                        Predicate parentPredicate = parent.getIdPredicate(pVar);
                        if (childPredicate != null && parentPredicate != null)
                            ruleRelevant &= childPredicate.getPredicateValue().equals(parentPredicate.getPredicateValue());
                    }
                }
            }
        }
        return ruleRelevant;
    }

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if (!(ruleAtom instanceof Relation)) return false;

        Relation childAtom = (Relation) ruleAtom;
        //discard if child has less rolePlayers
        if (childAtom.getRelationPlayers().size() < this.getRelationPlayers().size()) return false;

        Type type = getType();
        //Case: relation without type - match all
        if (type == null)
            return isRuleApplicableViaType((RelationType) childAtom.getType());
        else
            return isRuleApplicableViaAtom(childAtom, child);
    }

    @Override
    public boolean isRuleResolvable() {
        Type t = getType();
        if (t != null)
            return !t.getRulesOfConclusion().isEmpty()
                    && !this.getApplicableRules().isEmpty();
        else {
            GraknGraph graph = getParentQuery().graph();
            Set<Rule> rules = Reasoner.getRules(graph);
            return rules.stream()
                    .flatMap(rule -> rule.getConclusionTypes().stream())
                    .filter(Type::isRelationType).count() != 0
                    & !this.getApplicableRules().isEmpty();
        }
    }

    private Set<RoleType> getExplicitRoleTypes() {
        Set<RoleType> roleTypes = new HashSet<>();
        GraknGraph graph = getParentQuery().graph();
        relationPlayers.stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarAdmin::getTypeName)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::getRoleType)
                .forEach(roleTypes::add);
        return roleTypes;
    }

    private void addPredicate(Predicate pred) {
        if (getParentQuery() == null) throw new IllegalStateException("No parent in addPredicate");
        Query parent = getParentQuery();
        pred.setParentQuery(parent);
        setPredicate(pred);
        getParentQuery().addAtom(pred);
    }

    private void addType(Type type) {
        typeId = type.getId();
        String typeVariable = "rel-" + UUID.randomUUID().toString();
        addPredicate(new IdPredicate(Graql.var(typeVariable).id(typeId).admin()));
        atomPattern = atomPattern.asVar().isa(Graql.var(typeVariable)).admin();
        setValueVariable(typeVariable);
    }

    private void inferTypeFromRoles() {
        if (getParentQuery() != null && !isValueUserDefinedName() && getTypeId().isEmpty()) {
            //look at available roles
            RelationType type = null;
            Set<RelationType> compatibleTypes = getCompatibleRelationTypes(getExplicitRoleTypes(), roleToRelationTypes);
            if (compatibleTypes.size() == 1) type = compatibleTypes.iterator().next();

            //look at types
            if (type == null) {
                Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();
                Set<Type> types = getRolePlayers().stream()
                        .filter(varTypeMap::containsKey)
                        .map(varTypeMap::get)
                        .collect(Collectors.toSet());

                Set<RelationType> compatibleTypesFromTypes = getCompatibleRelationTypes(types, typeToRelationTypes);
                if (compatibleTypesFromTypes.size() == 1) type = compatibleTypesFromTypes.iterator().next();
                else {
                    compatibleTypesFromTypes.retainAll(compatibleTypes);
                    if (compatibleTypesFromTypes.size() == 1) type = compatibleTypesFromTypes.iterator().next();
                }
            }
            if (type != null) addType(type);
        }
    }

    private void inferTypeFromHasRole(){
        if (getPredicate() == null && getParentQuery() != null) {
            Query parent = getParentQuery();
            String valueVariable = getValueVariable();
            HasRole hrAtom = parent.getAtoms().stream()
                    .filter(at -> at.getVarName().equals(valueVariable))
                    .filter(at -> at instanceof HasRole).map(at -> (HasRole) at)
                    .findFirst().orElse(null);
            if (hrAtom != null) {
                AtomicQuery hrQuery = new AtomicMatchQuery(hrAtom, Sets.newHashSet(hrAtom.getVarName()));
                hrQuery.DBlookup();
                if (hrQuery.getAnswers().size() != 1)
                    throw new IllegalStateException("ambigious answer to has-role query");
                IdPredicate newPredicate = new IdPredicate(IdPredicate.createIdVar(hrAtom.getVarName(),
                        hrQuery.getAnswers().stream().findFirst().orElse(null).get(hrAtom.getVarName()).getId()), parent);

                Relation newRelation = new Relation(getPattern().asVar(), newPredicate, parent);
                parent.removeAtom(hrAtom.getPredicate());
                parent.removeAtom(hrAtom);
                parent.removeAtom(this);
                parent.addAtom(newRelation);
                parent.addAtom(newPredicate);
            }
        }
    }

    @Override
    public void inferTypes(){
        inferTypeFromRoles();
        inferTypeFromHasRole();
    }

    @Override
    public boolean containsVar(String name) {
        boolean varFound = false;
        Iterator<RelationPlayer> it = relationPlayers.iterator();
        while(it.hasNext() && !varFound)
            varFound = it.next().getRolePlayer().getVarName().equals(name);
        return varFound;
    }

    @Override
    public Set<Predicate> getIdPredicates() {
        Set<Predicate> idPredicates = super.getIdPredicates();
        //from types
        getTypeConstraints()
                .forEach(atom -> {
                    Predicate predicate = getParentQuery().getIdPredicate(atom.getValueVariable());
                    if (predicate != null) idPredicates.add(predicate);
                });
        return idPredicates;
    }

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        relationPlayers.forEach(c -> {
            String var = c.getRolePlayer().getVarName();
            if (var.equals(from))
                c.getRolePlayer().setVarName(to);
            else if (var.equals(to))
                c.getRolePlayer().setVarName(CAPTURE_MARK + var);
        });
    }

    @Override
    public void unify (Map<String, String> mappings) {
        super.unify(mappings);
        relationPlayers.forEach(c -> {
            String var = c.getRolePlayer().getVarName();
            if (mappings.containsKey(var) ) {
                String target = mappings.get(var);
                c.getRolePlayer().setVarName(target);
            }
            else if (mappings.containsValue(var)) {
                c.getRolePlayer().setVarName(CAPTURE_MARK + var);
            }
        });
    }

    @Override
    public Set<String> getVarNames(){
        Set<String> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        //add user specified role type vars
        relationPlayers.stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .filter(VarAdmin::isUserDefinedName)
                .forEach(r -> vars.add(r.getVarName()));
        return vars;
    }

    @Override
    public Set<String> getSelectedNames(){
        Set<String> vars = super.getSelectedNames();
        vars.addAll(getRolePlayers());
        return vars;
    }
    public Set<String> getRolePlayers(){
        Set<String> vars = new HashSet<>();
        relationPlayers.forEach(c -> vars.add(c.getRolePlayer().getVarName()));
        return vars;
    }

    /**
     * Attempts to infer the implicit roleTypes of vars in a relAtom
     * @return map containing a varName - varType, varRoleType triple
     */
    private Map<String, Pair<Type, RoleType>> computeVarTypeRoleMap() {
        Map<String, Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;

        GraknGraph graph =  getParentQuery().graph();
        Type relType = getType();
        Set<String> vars = getRolePlayers();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars) {
            Type type = varTypeMap.get(var);
            String roleTypeName = "";
            for(RelationPlayer c : relationPlayers) {
                if (c.getRolePlayer().getVarName().equals(var))
                    roleTypeName = c.getRoleType().flatMap(VarAdmin::getTypeName).orElse("");
            }
            //roletype explicit
            if (!roleTypeName.isEmpty())
                roleVarTypeMap.put(var, new Pair<>(type, graph.getRoleType(roleTypeName)));
            else {
                if (type != null && relType != null) {
                    Set<RoleType> cRoles = Utility.getCompatibleRoleTypes(type, relType);
                    //if roleType is unambigous
                    if (cRoles.size() == 1)
                        roleVarTypeMap.put(var, new Pair<>(type, cRoles.iterator().next()));
                    else
                        roleVarTypeMap.put(var, new Pair<>(type, null));
                }
            }
        }
        return roleVarTypeMap;
    }

    @Override
    public Map<String, Pair<Type, RoleType>> getVarTypeRoleMap() {
        if (varTypeRoleMap == null)
            varTypeRoleMap = computeVarTypeRoleMap();
        return varTypeRoleMap;
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types based on contents of the parent query
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    private Map<RoleType, Pair<String, Type>> computeRoleVarTypeMap() {
        Map<Var, Pair<String, Type>> roleVarTypeMap = new HashMap<>();
        Map<RoleType, Pair<String, Type>> roleTypeMap = new HashMap<>();
        if (getParentQuery() == null || getType() == null) return roleTypeMap;
        GraknGraph graph =  getParentQuery().graph();

        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();
        Set<String> allocatedVars = new HashSet<>();
        Set<RoleType> allocatedRoles = new HashSet<>();

        //explicit role types from castings
        relationPlayers.forEach(c -> {
            String var = c.getRolePlayer().getVarName();
            VarAdmin role = c.getRoleType().orElse(null);
            if (role != null) {
                Type type = varTypeMap.get(var);
                roleVarTypeMap.put(role, new Pair<>(var, type));
                //try directly
                String typeName = role.getTypeName().orElse("");
                RoleType roleType = !typeName.isEmpty() ? graph.getRoleType(typeName): null;
                //try indirectly
                if (roleType == null && role.isUserDefinedName()) {
                    Predicate rolePredicate = getParentQuery().getIdPredicate(role.getVarName());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicateValue());
                }
                allocatedVars.add(var);
                if (roleType != null) {
                    allocatedRoles.add(roleType);
                    roleTypeMap.put(roleType, new Pair<>(var, type));
                }
            }
        });

        //remaining roles
        RelationType relType = (RelationType) getType();
        Set<String> varsToAllocate = getRolePlayers();
        varsToAllocate.removeAll(allocatedVars);
        varsToAllocate.forEach(var -> {
            Type type = varTypeMap.get(var);
            if (type != null && relType != null) {
                Set<RoleType> cRoles = Utility.getCompatibleRoleTypes(type, relType);
                //if roleType is unambigous
                if (cRoles.size() == 1) {
                    RoleType roleType = cRoles.iterator().next();
                    VarAdmin roleVar = Graql.var().name(roleType.getName()).admin();
                    roleVarTypeMap.put(roleVar, new Pair<>(var, type));
                    allocatedVars.add(var);
                    allocatedRoles.add(roleType);
                    roleTypeMap.put(roleType, new Pair<>(var, type));
                }
            }
        });

        Collection<RoleType> rolesToAllocate = relType.hasRoles();
        //remove sub and super roles of allocated roles
        allocatedRoles.forEach(role -> {
            RoleType topRole = getNonMetaTopRole(role);
            rolesToAllocate.removeAll(topRole.subTypes());
        });
        varsToAllocate.removeAll(allocatedVars);
        //if unambiguous assign top role
        if (varsToAllocate.size() == 1 && !rolesToAllocate.isEmpty()) {
            RoleType topRole = getNonMetaTopRole(rolesToAllocate.iterator().next());
            String var = varsToAllocate.iterator().next();
            Type type = varTypeMap.get(var);
            roleTypeMap.put(topRole, new Pair<>(var, type));
            roleVarTypeMap.put(Graql.var().name(topRole.getName()).admin(), new Pair<>(var, type));
        }

        //update pattern and castings
        Map<String, Var> roleMap = new HashMap<>();
        roleVarTypeMap.forEach( (r, tp) -> roleMap.put(tp.getKey(), r));
        getRolePlayers().stream()
                .filter(var -> !var.equals(getVarName()))
                .filter(var -> !roleMap.containsKey(var))
                .forEach( var -> roleMap.put(var, null));

        //pattern mutation!
        atomPattern = constructRelationVar(isUserDefinedName()? varName : "", getValueVariable(), roleMap);
        relationPlayers = getRelationPlayers(getPattern().asVar());
        return roleTypeMap;
    }

    @Override
    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap() {
        if (roleVarTypeMap == null) {
            if (varTypeRoleMap != null) {
                roleVarTypeMap = new HashMap<>();
                varTypeRoleMap.forEach((var, tpair) -> {
                    RoleType rt = tpair.getValue();
                    if (rt != null)
                        roleVarTypeMap.put(rt, new Pair<>(var, tpair.getKey()));
                });
            } else
                roleVarTypeMap = computeRoleVarTypeMap();
        }
        return roleVarTypeMap;
    }

    /**
     * @return map of role variable - role type from a predicate
     */
    private Map<String, RoleType> getIndirectRoleMap(){
        GraknGraph graph =  getParentQuery().graph();
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .map(rt -> new AbstractMap.SimpleEntry<>(rt, getParentQuery().getIdPredicate(rt.getVarName())))
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> e.getKey().getVarName(), e -> graph.getConcept(e.getValue().getPredicateValue())));
    }

    private Map<String, String> getUnifiers(Map<String, RoleType> childMap, Map<RoleType, String> parentMap,
                                            Set<String> childBVs, Set<String> varsToAllocate){
        Map<String, String> unifiers = new HashMap<>();
        Set<String> allocatedVars = new HashSet<>();
        childBVs.forEach(chVar -> {
            if(!varsToAllocate.isEmpty()) {
                RoleType role = childMap.get(chVar);
                //map to empty if no var matching
                String pVar = "";
                while(role != null && pVar.isEmpty()
                        && !Schema.MetaSchema.isMetaName(role.getName())) {
                    pVar = parentMap.getOrDefault(role, "");
                    role = role.superType();
                }
                if (!pVar.isEmpty() && !chVar.equals(pVar)){
                    unifiers.put(chVar, pVar);
                    allocatedVars.add(chVar);
                    varsToAllocate.remove(pVar);
                }
            }
        });
        //assign unallocated vars
        childBVs.removeAll(allocatedVars);
        Iterator<String> cit = childBVs.iterator();
        Iterator<String> pit = varsToAllocate.iterator();
        while(pit.hasNext() && cit.hasNext()) unifiers.put(cit.next(), pit.next());
        return unifiers;
    }

    private Map<String, String> getRoleTypeUnifiers(Relation parentAtom){
        Map<String, RoleType> childMap = getIndirectRoleMap();
        Map<RoleType, String> parentMap =  parentAtom.getIndirectRoleMap()
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        Set<String> indirectVarsToAllocate = Sets.newHashSet(parentMap.values());
        return getUnifiers(childMap, parentMap, childMap.keySet(), indirectVarsToAllocate);
    }

    private Map<String, String> getRolePlayerUnifiers(Relation parentAtom){
        Map<String, RoleType> childMap = new HashMap<>();
        Map<RoleType, String> parentMap = new HashMap<>();
        getVarTypeRoleMap().entrySet().forEach( e -> childMap.put(e.getKey(), e.getValue().getValue()));
        parentAtom.getRoleVarTypeMap().entrySet().forEach( e -> parentMap.put(e.getKey(), e.getValue().getKey()));
        return getUnifiers(childMap, parentMap, getRolePlayers(), parentAtom.getRolePlayers());
    }

    @Override
    public Map<String, String> getUnifiers(Atomic pAtom) {
        if (!(pAtom instanceof TypeAtom))
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());

        Map<String, String> unifiers = super.getUnifiers(pAtom);
        if (((Atom) pAtom).isRelation()){
            Relation parentAtom = (Relation) pAtom;
            //get role player unifiers
            unifiers.putAll(getRolePlayerUnifiers(parentAtom));
            //get role type unifiers
            unifiers.putAll(getRoleTypeUnifiers(parentAtom));
        }
        return unifiers;
    }

    /**
     * @return map of pairs: varName - Id predicate (substitution) describing this variable
     */
    private Map<String, Predicate> getVarSubMap() {
        Map<String, Predicate> map = new HashMap<>();
        getPredicates().stream()
            .filter(Predicate::isIdPredicate)
            .forEach( sub -> {
                String var = sub.getVarName();
                map.put(var, sub);
        });
        return map;
    }

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    private Map<RoleType, String> getRoleConceptIdMap(){
        Map<RoleType, String> roleConceptMap = new HashMap<>();
        Map<String, Predicate> varSubMap = getVarSubMap();
        Map<RoleType, Pair<String, Type>> roleVarMap = getRoleVarTypeMap();

        roleVarMap.forEach( (role, varTypePair) -> {
            String var = varTypePair.getKey();
            roleConceptMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
        });
        return roleConceptMap;
    }

    @Override
    public Pair<Atom, Map<String, String>> rewrite(Atom parentAtom, Query parent){
        if(parentAtom.isUserDefinedName()){
            Map<String, String> unifiers = new HashMap<>();
            Var relVar = Graql.var(UUID.randomUUID().toString());
            getPattern().asVar().getProperty(IsaProperty.class).ifPresent(prop -> relVar.isa(prop.getType()));

            relationPlayers
                    .forEach(c -> {
                        VarAdmin rolePlayer = c.getRolePlayer();
                        String rolePlayerVarName = UUID.randomUUID().toString();
                        unifiers.put(rolePlayer.getVarName(), rolePlayerVarName);
                        VarAdmin roleType = c.getRoleType().orElse(null);
                        if (roleType != null)
                            relVar.rel(roleType, rolePlayerVarName);
                        else
                            relVar.rel(rolePlayerVarName);
                    });
            return new Pair<>(new Relation(relVar.admin(), getPredicate(), parent), unifiers);
        }
        else
            return new Pair<>(this, new HashMap<>());
    }
}
