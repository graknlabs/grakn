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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.kb.internal.cache.Cache;
import ai.grakn.kb.internal.cache.Cacheable;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *     An ontological element which defines a role which can be played in a relation type.
 * </p>
 *
 * <p>
 *     This ontological element defines the roles which make up a {@link RelationshipType}.
 *     It behaves similarly to {@link Type} when relating to other types.
 *     It has some additional functionality:
 *     1. It cannot play a role to itself.
 *     2. It is special in that it is unique to relation types.
 * </p>
 *
 * @author fppt
 *
 */
public class RoleImpl extends SchemaConceptImpl<Role> implements Role {
    private final Cache<Set<Type>> cachedDirectPlayedByTypes = new Cache<>(Cacheable.set(), () -> this.<Type>neighbours(Direction.IN, Schema.EdgeLabel.PLAYS).collect(Collectors.toSet()));
    private final Cache<Set<RelationshipType>> cachedRelationTypes = new Cache<>(Cacheable.set(), () -> this.<RelationshipType>neighbours(Direction.IN, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    RoleImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    RoleImpl(VertexElement vertexElement, Role type, Boolean isImplicit) {
        super(vertexElement, type, isImplicit);
    }

    @Override
    public void txCacheFlush(){
        super.txCacheFlush();
        cachedDirectPlayedByTypes.flush();
        cachedRelationTypes.flush();
    }

    @Override
    public void txCacheClear(){
        super.txCacheClear();
        cachedDirectPlayedByTypes.clear();
        cachedRelationTypes.clear();
    }

    @Override
    public Stream<RelationshipType> relationTypes() {
        return cachedRelationTypes.get().stream();
    }

    /**
     * Caches a new relation type which this role will be part of. This may result in a DB hit if the cache has not been
     * initialised.
     *
     * @param newRelationshipType The new relation type to cache in the role.
     */
    void addCachedRelationType(RelationshipType newRelationshipType){
        cachedRelationTypes.ifPresent(set -> set.add(newRelationshipType));
    }

    /**
     * Removes an old relation type which this role is no longer part of. This may result in a DB hit if the cache has
     * not been initialised.
     *
     * @param oldRelationshipType The new relation type to cache in the role.
     */
    void deleteCachedRelationType(RelationshipType oldRelationshipType){
        cachedRelationTypes.ifPresent(set -> set.remove(oldRelationshipType));
    }

    /**
     *
     * @return A list of all the Concept Types which can play this role.
     */
    @Override
    public Stream<Type> playedByTypes() {
        return cachedDirectPlayedByTypes.get().stream().flatMap(Type::subs);
    }

    void addCachedDirectPlaysByType(Type newType){
        cachedDirectPlayedByTypes.ifPresent(set -> set.add(newType));
    }

    void deleteCachedDirectPlaysByType(Type oldType){
        cachedDirectPlayedByTypes.ifPresent(set -> set.remove(oldType));
    }

    /**
     *
     * @return Get all the roleplayers of this role type
     */
    public Stream<Casting> rolePlayers(){
        return relationTypes().
                flatMap(RelationshipType::instances).
                map(relation -> RelationshipImpl.from(relation).reified()).
                flatMap(CommonUtil::optionalToStream).
                flatMap(relation -> relation.castingsRelation(this));
    }

    @Override
    boolean deletionAllowed(){
        return super.deletionAllowed() &&
                !neighbours(Direction.IN, Schema.EdgeLabel.RELATES).findAny().isPresent() && // This role is not linked t any relation type
                !neighbours(Direction.IN, Schema.EdgeLabel.PLAYS).findAny().isPresent() && // Nothing can play this role
                !rolePlayers().findAny().isPresent(); // This role has no role players
    }

    @Override
    void trackRolePlayers() {
        //TODO: track the super change when the role super changes
    }

}
