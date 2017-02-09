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

package ai.grakn.graph.internal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     Tracks Graph Mutations.
 * </p>
 *
 * <p>
 *     This package keeps track of changes to the rootGraph that need to be validated. This includes:
 *      new concepts,
 *      concepts that have had edges added/deleted,
 *      edge cases, for example, relationship where a new role player is added.
 * </p>
 *
 * @author fppt
 *
 */
public class ConceptLog {

    //Caches any concept which has been touched before
    private final Map<ConceptId, ConceptImpl> conceptCache = new HashMap<>();

    //We Track Modified Concepts For Validation
    private final Set<ConceptImpl> modifiedConcepts;

    //We Track Casting Explicitly For Post Processing
    private final Set<CastingImpl> modifiedCastings;

    //We Track Resource Explicitly for Post Processing
    private final Set<ResourceImpl> modifiedResources;

    //We Track Relations so that we can look them up before they are completely defined and indexed on commit
    private final Map<String, RelationImpl> modifiedRelations;


    ConceptLog() {
        modifiedCastings = new HashSet<>();
        modifiedConcepts = new HashSet<>();
        modifiedResources = new HashSet<>();
        modifiedRelations = new HashMap<>();
    }

    /**
     * Removes all the concepts from the transaction tracker
     */
    void clearTransaction(){
        modifiedConcepts.clear();
        modifiedCastings.clear();
        modifiedResources.clear();
        modifiedRelations.clear();
        conceptCache.clear();
    }

    /**
     *
     * @param concept The concept to be later validated
     */
    void trackConceptForValidation(ConceptImpl concept) {
        if (!modifiedConcepts.contains(concept)) {
            modifiedConcepts.add(concept);

            if (concept.isCasting()) {
                modifiedCastings.add(concept.asCasting());
            }
            if (concept.isResource()) {
                modifiedResources.add((ResourceImpl) concept);
            }
        }

        //Caching of relations in memory so they can be retrieved without needing a commit
        if (concept.isRelation()) {
            RelationImpl relation = (RelationImpl) concept;
            modifiedRelations.put(RelationImpl.generateNewHash(relation.type(), relation.rolePlayers()), relation);
        }
    }

    /**
     *
     * @return All the concepts which have been affected within the transaction in some way
     */
    Set<ConceptImpl> getModifiedConcepts() {
        return modifiedConcepts.stream().filter(c -> c != null && c.isAlive()).collect(Collectors.toSet());
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    Set<CastingImpl> getModifiedCastings() {
        return modifiedCastings.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    Set<ResourceImpl> getModifiedResources() {
        return modifiedResources.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
    }

    /**
     *
     * @param concept The concept to nio longer track
     */
    void removeConcept(ConceptImpl concept){
        modifiedConcepts.remove(concept);
        modifiedCastings.remove(concept);
        modifiedResources.remove(concept);
        conceptCache.remove(concept.getId());
    }

    /**
     * Gets a cached relation by index. This way we can find non committed relations quickly.
     *
     * @param index The current index of the relation
     */
    RelationImpl getCachedRelation(String index){
        return modifiedRelations.get(index);
    }

    /**
     * Caches a concept so it does not have to be rebuilt later.
     *
     * @param concept The concept to be cached.
     */
    void cacheConcept(ConceptImpl concept){
        conceptCache.put(concept.getId(), concept);
    }

    /**
     * Checks if the concept has been built before and is currently cached
     *
     * @param id The id of the concept
     */
    boolean isConceptCached(ConceptId id){
        return conceptCache.containsKey(id);
    }

    /**
     * Returns a previously built concept
     *
     * @param id The id of the concept
     * @param <X> The type of the concept
     * @return The cached concept
     */
    <X extends Concept> X getCachedConcept(ConceptId id){
        //noinspection unchecked
        return (X) conceptCache.get(id);
    }
}
