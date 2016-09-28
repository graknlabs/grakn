/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.reasoner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;

import java.util.*;

public class Utility {

    public static void printMatchQueryResults(MatchQuery sq) {
        List<Map<String, Concept>> results = Lists.newArrayList(sq);

        for (Map<String, Concept> result : results) {
            for (Map.Entry<String, Concept> entry : result.entrySet()) {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : ");

                if (concept.isResource()) {
                    System.out.print(concept.asResource().getValue() + " ");
                }
            }
            System.out.println();
        }
    }

    public static void printAnswers(Set<Map<String, Concept>> answers) {
        for (Map<String, Concept> result : answers) {
            for (Map.Entry<String, Concept> entry : result.entrySet()) {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : ");

                if (concept.isResource()) {
                    System.out.print(concept.asResource().getValue() + " ");
                }
            }
            System.out.println();
        }
    }

    public static AtomicQuery findEquivalentAtomicQuery(AtomicQuery query, Set<AtomicQuery> queries) {
        AtomicQuery equivalentQuery = null;
        Iterator<AtomicQuery> it = queries.iterator();
        while( it.hasNext() && equivalentQuery == null) {
            AtomicQuery current = it.next();
            if (query.isEquivalent(current))
                equivalentQuery = current;
        }
        return equivalentQuery;
    }

    public static Set<RoleType> getCompatibleRoleTypes(String typeId, String relId, MindmapsGraph graph) {
        Set<RoleType> cRoles = new HashSet<>();

        Collection<RoleType> typeRoles = graph.getType(typeId).playsRoles();
        Collection<RoleType> relRoles = graph.getRelationType(relId).hasRoles();
        relRoles.stream().filter(typeRoles::contains).forEach(cRoles::add);
        return cRoles;
    }

    public static boolean checkAtomsCompatible(Atomic a, Atomic b, MindmapsGraph graph) {
        if (!(a.isType() && b.isType()) || (a.isRelation() || b.isRelation())
           || !a.getVarName().equals(b.getVarName()) || a.isResource() || b.isResource()) return true;
        String aTypeId = a.getTypeId();
        Type aType = graph.getType(aTypeId);
        String bTypeId = b.getTypeId();
        Type bType = graph.getType(bTypeId);

        return checkTypesCompatible(aType, bType) && (a.getVal().isEmpty() || b.getVal().isEmpty() || a.getVal().equals(b.getVal()) );
    }

    //rolePlayer-roleType maps
    public static void computeRoleCombinations(Set<String> vars, Set<RoleType> roles, Map<String, String> roleMap,
                                        Set<Map<String, String>> roleMaps){
        Set<String> tempVars = Sets.newHashSet(vars);
        Set<RoleType> tempRoles = Sets.newHashSet(roles);
        String var = vars.iterator().next();

        roles.forEach(role -> {
            tempVars.remove(var);
            tempRoles.remove(role);
            roleMap.put(var, role.getId());
            if (!tempVars.isEmpty() && !tempRoles.isEmpty())
                computeRoleCombinations(tempVars, tempRoles, roleMap, roleMaps);
            else {
                if (!roleMap.isEmpty())
                    roleMaps.add(Maps.newHashMap(roleMap));
                roleMap.remove(var);
            }
            tempVars.add(var);
            tempRoles.add(role);
        });
    }

    /**
     * generate a fresh variable avoiding global variables and variables from the same query
     * @param vars  vars to be avoided
     * @param var        variable to be generated a fresh replacement
     * @return fresh variables
     */
    public static String createFreshVariable(Set<String> vars, String var) {
        String fresh = var;
        while (vars.contains(fresh)) {
            String valFree = fresh.replaceAll("[^0-9]", "");
            int value = valFree.equals("") ? 0 : Integer.parseInt(valFree);
            fresh = fresh.replaceAll("\\d+", "") + (++value);
        }
        return fresh;
    }



    public static boolean checkTypesCompatible(Type aType, Type bType) {
        return aType.equals(bType) || aType.subTypes().contains(bType) || bType.subTypes().contains(aType);
    }
}
