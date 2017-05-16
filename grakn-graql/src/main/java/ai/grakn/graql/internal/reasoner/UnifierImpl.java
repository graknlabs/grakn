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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Implementation of {@link Unifier} interface.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class UnifierImpl implements Unifier {

    private final Multimap<VarName, VarName> unifier = ArrayListMultimap.create();

    /**
     * Identity unifier.
     */
    public UnifierImpl(){}
    public UnifierImpl(Map<VarName, VarName> map){
        map.entrySet().forEach(m -> unifier.put(m.getKey(), m.getValue()));
    }
    public UnifierImpl(Unifier u){
        merge(u);
    }

    @Override
    public String toString(){
        return unifier.toString();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        UnifierImpl u2 = (UnifierImpl) obj;
        return unifier.equals(u2.map());
    }

    @Override
    public int hashCode(){
        return unifier.hashCode();
    }

    @Override
    public boolean isEmpty() {
        return unifier.isEmpty();
    }

    @Override
    public Map<VarName, Collection<VarName>> map() {
        return unifier.asMap();
    }

    @Override
    public Set<VarName> keySet() {
        return unifier.keySet();
    }

    @Override
    public Collection<VarName> values() {
        return unifier.values();
    }

    @Override
    public Collection<Map.Entry<VarName, VarName>> mappings(){ return unifier.entries();}

    public boolean addMapping(VarName key, VarName value){
        return unifier.put(key, value);
    }

    @Override
    public Collection<VarName> get(VarName key) {
        return unifier.get(key);
    }

    @Override
    public boolean containsKey(VarName key) {
        return unifier.containsKey(key);
    }

    @Override
    public boolean containsValue(VarName value) {
        return unifier.containsValue(value);
    }

    @Override
    public boolean containsAll(Unifier u) { return mappings().containsAll(u.mappings());}

    @Override
    public Unifier merge(Unifier d) {
        d.mappings().forEach(m -> unifier.put(m.getKey(), m.getValue()));
        return this;
    }

    @Override
    public Unifier removeTrivialMappings() {
        Set<VarName> toRemove = unifier.entries().stream()
                .filter(e -> e.getKey() == e.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        toRemove.forEach(unifier::removeAll);
        return this;
    }

    @Override
    public Unifier inverse() {
        Unifier inverse = new UnifierImpl();
        unifier.entries().forEach(e -> inverse.addMapping(e.getValue(), e.getKey()));
        return inverse;
    }

    @Override
    public int size(){ return unifier.size();}
}
