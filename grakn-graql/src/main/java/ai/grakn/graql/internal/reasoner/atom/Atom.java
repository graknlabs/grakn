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

import ai.grakn.GraknGraph;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import javafx.util.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class Atom extends AtomBase {

    protected Type type = null;
    protected String typeId = null;

    public Atom(VarAdmin pattern) {
        super(pattern);
        this.atomPattern = pattern;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    public Atom(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    public Atom(Atom a) {
        super(a);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    private String extractTypeId(VarAdmin var) {
        String vTypeId;
        Iterator<HasResourceProperty> resources = var.getProperties(HasResourceProperty.class).iterator();
        if (resources.hasNext()) {
            HasResourceProperty resource = resources.next();
            if (resources.hasNext())
                throw new IllegalArgumentException(ErrorMessage.MULTIPLE_RESOURCES.getMessage(var.toString()));
            vTypeId = resource.getType().orElse("");
        }
        else
            vTypeId = var.getProperty(IsaProperty.class).map(IsaProperty::getType).flatMap(VarAdmin::getId).orElse("");
        return vTypeId;
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

    protected abstract boolean isRuleApplicable(InferenceRule child);

    public Set<Rule> getApplicableRules() {
        Set<Rule> children = new HashSet<>();
        GraknGraph graph = getParentQuery().getGraph().orElse(null);
        Type type = getType();
        //TODO change if we allow for Types having null type
        if (type == null) {
            Collection<Rule> applicableRules = Reasoner.getRules(graph).stream()
                    .filter(rule -> rule.getConclusionTypes().stream().filter(Type::isRelationType).count() != 0)
                    .collect(Collectors.toSet());
            children.addAll(applicableRules);
        }
        else{
            Collection<Rule> rulesFromType = type.getRulesOfConclusion();
            rulesFromType.forEach(rule -> {
                InferenceRule child = new InferenceRule(rule, graph);
                boolean ruleRelevant = isRuleApplicable(child);
                if (ruleRelevant) children.add(rule);
            });
        }
        return children;
    }

    @Override
    public boolean isRuleResolvable() {
        Type type = getType();
        return type != null && !getType().getRulesOfConclusion().isEmpty();
    }

    @Override
    public boolean isRecursive(){
        if (isResource()) return false;
        boolean atomRecursive = false;

        String typeId = getTypeId();
        if (typeId.isEmpty()) return false;
        Type type = getParentQuery().getGraph().orElse(null).getType(typeId);
        Collection<Rule> presentInConclusion = type.getRulesOfConclusion();
        Collection<Rule> presentInHypothesis = type.getRulesOfHypothesis();

        for(Rule rule : presentInConclusion)
            atomRecursive |= presentInHypothesis.contains(rule);

        return atomRecursive;
    }

    public Type getType(){
        if (type == null)
            type = getParentQuery().getGraph().orElse(null).getType(typeId);
        return type;
    }

    public String getTypeId(){ return typeId;}

    public String getValueVariable() {
        throw new IllegalArgumentException("getValueVariable called on Atom object " + getPattern());
    }

    public Set<Predicate> getIdPredicates() {
        Set<Predicate> relevantPredicates = new HashSet<>();
        getParentQuery().getIdPredicates().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .forEach(relevantPredicates::add);
        //ids from indirect types
        getTypeConstraints()
                .forEach(atom -> {
                    Predicate predicate = getParentQuery().getIdPredicate(atom.getValueVariable());
                    if (predicate != null) relevantPredicates.add(predicate);
                });
        return relevantPredicates;
    }

    public Set<Predicate> getValuePredicates(){
        return getParentQuery().getValuePredicates().stream()
                .filter(atom -> (containsVar(atom.getVarName())))
                .collect(Collectors.toSet());
    }

    public Set<Atom> getTypeConstraints(){
        Set<Atom> relevantTypes = new HashSet<>();
        //ids from indirect types
        getParentQuery().getTypeConstraints().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .forEach(atom -> {
                    relevantTypes.add(atom);
                    relevantTypes.addAll(((Binary)atom).getLinkedAtoms());
                });
        return relevantTypes;
    }

    public Map<String, Predicate> getVarSubMap() {
        Map<String, Predicate> map = new HashMap<>();
        getIdPredicates().forEach( sub -> {
            String var = sub.getVarName();
            map.put(var, sub);
        });
        return map;
    }

    public Map<RoleType, String> getRoleConceptIdMap(){
        Map<RoleType, String> roleConceptMap = new HashMap<>();
        Map<String, Predicate> varSubMap = getVarSubMap();
        Map<RoleType, Pair<String, Type>> roleVarMap = getRoleVarTypeMap();

        roleVarMap.forEach( (role, varTypePair) -> {
            String var = varTypePair.getKey();
            roleConceptMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
        });
        return roleConceptMap;
    }

    public Map<String, javafx.util.Pair<Type, RoleType>> getVarTypeRoleMap() {
        Map<String, javafx.util.Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;

        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        vars.forEach(var -> {
            Type type = varTypeMap.get(var);
            roleVarTypeMap.put(var, new Pair<>(type, null));
        });
        return roleVarTypeMap;
    }

    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap() { return new HashMap<>();}
}
