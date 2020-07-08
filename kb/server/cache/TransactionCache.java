/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.kb.server.cache;

import com.google.common.annotations.VisibleForTesting;
import grakn.common.util.Pair;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.Casting;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.keyspace.KeyspaceSchemaCache;
import grakn.core.kb.keyspace.LabelCache;
import grakn.core.kb.keyspace.LabelCacheReader;
import graql.lang.statement.Label;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Caches TransactionOLTP specific data this includes:
 * Validation Concepts - Concepts which need to undergo validation.
 * Built Concepts -  Prevents rebuilding when the same vertex is encountered
 * The Schema - Optimises validation checks by preventing db read.
 * Label - Allows mapping type labels to type Ids
 */
public class TransactionCache {
    //Cache which is shared across multiple transactions
    private final KeyspaceSchemaCache keyspaceSchemaCache;

    //Caches any concept which has been touched before
    private final Map<ConceptId, Concept> conceptCache = new HashMap<>();
    private final Map<String, Attribute> attributeCache = new HashMap<>();
    private final Map<Label, SchemaConcept> schemaConceptCache = new HashMap<>();
    private final LabelCache labelCache = new LabelCache();

    //Elements Tracked For Validation
    private final Set<Relation> newRelations = new HashSet<>();
    private final Set<Thing> modifiedThings = new HashSet<>();

    private final Set<Role> modifiedRoles = new HashSet<>();
    private final Set<Casting> modifiedCastings = new HashSet<>();

    private final Set<RelationType> modifiedRelationTypes = new HashSet<>();

    private final Set<Rule> modifiedRules = new HashSet<>();
    private final Set<Thing> inferredConcepts = new HashSet<>();
    private final Set<Thing> inferredConceptsToPersist = new HashSet<>();
    private final Set<Pair<Thing, Attribute<?>>> inferredOwnerships = new HashSet<>();
    private final Set<Pair<Thing, Attribute<?>>> inferredOwnershipsToPersist = new HashSet<>();

    private Map<Label, Long> newShards = new HashMap<>();

    //New attributes are tracked so that we can merge any duplicate attributes at commit time.
    // The label, index and id are directly cached to prevent unneeded reads
    private Map<Pair<Label, String>, ConceptId> newAttributes = new HashMap<>();
    // Track the removed attributes so that we can evict old attribute indexes from attributesCache in session
    // after commit
    private Set<String> removedAttributes = new HashSet<>();
    private Set<String> modifiedKeyIndices = new HashSet<>();

    public TransactionCache(KeyspaceSchemaCache keyspaceSchemaCache) {
        this.keyspaceSchemaCache = keyspaceSchemaCache;
    }

    public void flushSchemaLabelIdsToCache() {
        //Check if the schema has been changed and should be flushed into this cache
        if (!keyspaceSchemaCache.cacheMatches(labelCache)) {
            keyspaceSchemaCache.overwriteCache(labelCache);
        }
    }

    /**
     * Refreshes the transaction schema cache by reading the keyspace schema cache into this transaction cache.
     * This method performs this operation whilst making a deep clone of the cached concepts to ensure transactions
     * do not accidentally break the central schema cache.
     */
    public void updateSchemaCacheFromKeyspaceCache() {
        ReentrantReadWriteLock schemaCacheLock = keyspaceSchemaCache.concurrentUpdateLock();
        try {
            schemaCacheLock.writeLock().lock();
            LabelCache cachedLabelsSnapshot = keyspaceSchemaCache.labelCacheCopy();
            this.labelCache.absorb(cachedLabelsSnapshot);
        } finally {
            schemaCacheLock.writeLock().unlock();
        }
    }

    /**
     * @param concept The element to be later validated
     */
    public void trackForValidation(Concept concept) {
        if (concept.isThing()) {
            modifiedThings.add(concept.asThing());
        } else if (concept.isRole()) {
            modifiedRoles.add(concept.asRole());
        } else if (concept.isRelationType()) {
            modifiedRelationTypes.add(concept.asRelationType());
        } else if (concept.isRule()) {
            modifiedRules.add(concept.asRule());
        }
    }


    public void trackForValidation(Casting casting) {
        modifiedCastings.add(casting);
    }


    public void removeFromValidation(Type type) {
        if (type.isRelationType()) {
            modifiedRelationTypes.remove(type.asRelationType());
        }
    }

    /**
     * @param concept The concept to no longer track
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public void remove(Concept concept) {
        modifiedThings.remove(concept);
        modifiedRoles.remove(concept);
        modifiedRelationTypes.remove(concept);
        modifiedRules.remove(concept);

        if (concept.isAttribute()) {
            Attribute<?> attr = concept.asAttribute();
            // this is probably slower than reading index from vertex but we don't have access to AttributeImpl here (cyclic dep)
            Label attrLabel = attr.type().label();
            String attrIndex = Schema.generateAttributeIndex(attrLabel, attr.value().toString());
            newAttributes.remove(new Pair<>(attrLabel, attrIndex));
            attributeCache.remove(attrIndex);
            removedAttributes.add(attrIndex);
        }

        if (concept.isRelation()) {
            newRelations.remove(concept.asRelation());
        }

        if (concept.isThing()){
            Thing instance = concept.asThing();
            if (instance.isInferred()) removeInferredInstance(instance);
        }

        conceptCache.remove(concept.id());
        if (concept.isSchemaConcept()) {
            Label label = concept.asSchemaConcept().label();
            schemaConceptCache.remove(label);
            labelCache.removeCompleteLabel(label);
        }
    }

    public void deleteCasting(Casting casting) {
        modifiedCastings.remove(casting);
    }

    /**
     * Caches a concept so it does not have to be rebuilt later.
     *
     * @param concept The concept to be cached.
     */
    public void cacheConcept(Concept concept) {
        conceptCache.put(concept.id(), concept);
        if (concept.isSchemaConcept()) {
            SchemaConcept schemaConcept = concept.asSchemaConcept();
            schemaConceptCache.put(schemaConcept.label(), schemaConcept);
            labelCache.cacheCompleteLabel(schemaConcept.label(), schemaConcept.labelId());
        }
        if (concept.isAttribute()){
            Attribute<Object> attribute = concept.asAttribute();
            String index = Schema.generateAttributeIndex(attribute.type().label(), attribute.value().toString());
            attributeCache.put(index, attribute);
        }
    }

    /**
     * Caches the mapping of a type label to a type id. This is necessary in order for ANY types to be looked up.
     *
     * @param label The type label to cache
     * @param id    Its equivalent id which can be looked up quickly in the graph
     */
    public void cacheLabel(Label label, LabelId id) {

    }

    /**
     * Checks if the concept has been built before and is currently cached
     *
     * @param id The id of the concept
     * @return true if the concept is cached
     */
    public boolean isConceptCached(ConceptId id) {
        return conceptCache.containsKey(id);
    }

    /**
     * @param label The label of the type to cache
     * @return true if the concept is cached
     */
    public boolean isTypeCached(Label label) {
        return schemaConceptCache.containsKey(label);
    }

    /**
     * Returns a previously built concept
     *
     * @param id  The id of the concept
     * @param <X> The type of the concept
     * @return The cached concept
     */
    public <X extends Concept> X getCachedConcept(ConceptId id) {
        //noinspection unchecked
        return (X) conceptCache.get(id);
    }

    // ------------- Methods and state enabling persistence of inferred facts -----

    /**
     * Caches an inferred instance for possible persistence later.
     *
     * @param thing The inferred instance to be cached.
     */
    public void inferredInstance(Thing thing){
        inferredConcepts.add(thing);
    }

    /**
     * Remove an inferred instance from tracking.
     *
     * @param thing The inferred instance to be cached.
     */
    public void removeInferredInstance(Thing thing){
        inferredConcepts.remove(thing);
        inferredConceptsToPersist.remove(thing);
    }

    public void inferredInstanceToPersist(Thing t) {
        inferredConceptsToPersist.add(t);
    }

    public boolean anyFactsInferred() {
        return inferredConcepts.size() > 0 || inferredOwnerships.size() > 0;
    }

    public void inferredOwnershipToPersist(Thing owner, Attribute<?> attribute) {
        inferredOwnershipsToPersist.add(new Pair<>(owner, attribute));
    }

    public void hasAttributeCreated(Thing owner, Attribute<?> attribute, boolean isInferred) {
        if (isInferred) {
            inferredOwnerships.add(new Pair<>(owner, attribute));
        }
    }

    public void hasAttributeDeleted(Thing owner, Attribute<?> attribute, boolean isInferred) {
        if (isInferred) {
            Pair<Thing, Attribute<?>> ownership = new Pair<>(owner, attribute);
            inferredOwnerships.remove(ownership);
            inferredOwnershipsToPersist.remove(ownership);
        }
    }

    /**
     * @return cached things that are inferred
     */
    public Set<Thing> getInferredInstancesToDiscard() {
        return inferredConcepts.stream()
                .filter(t -> !inferredConceptsToPersist.contains(t))
                .collect(Collectors.toSet());
    }

    public Set<Pair<Thing, Attribute<?>>> getInferredOwnershipsToDiscard() {
        return inferredOwnerships.stream()
                .filter(pair -> !inferredOwnershipsToPersist.contains(pair))
                .collect(Collectors.toSet());
    }

    /**
     * Returns a previously built type
     *
     * @param label The label of the type
     * @param <X>   The type of the type
     * @return The cached type
     */
    public <X extends SchemaConcept> X getCachedSchemaConcept(Label label) {
        //noinspection unchecked
        return (X) schemaConceptCache.get(label);
    }

    public LabelCacheReader labelCache() {
        return labelCache;
    }

    public void addNewAttribute(Label label, String index, ConceptId conceptId) {
        newAttributes.put(new Pair<>(label, index), conceptId);
    }

    public void addModifiedKeyIndex(String keyIndex){
        modifiedKeyIndices.add(keyIndex);
    }

    public Map<Pair<Label, String>, ConceptId> getNewAttributes() {
        return newAttributes;
    }

    public Map<Label, Long> getNewShards() {
        return newShards;
    }

    //--------------------------------------- Concepts Needed For Validation -------------------------------------------
    public Set<Thing> getModifiedThings() {
        return modifiedThings;
    }

    public Set<Role> getModifiedRoles() {
        return modifiedRoles;
    }

    public Set<RelationType> getModifiedRelationTypes() {
        return modifiedRelationTypes;
    }

    public Set<String> getModifiedKeyIndices(){ return modifiedKeyIndices;}

    public Set<Rule> getModifiedRules() {
        return modifiedRules;
    }

    public Set<Casting> getModifiedCastings() {
        return modifiedCastings;
    }

    public void addNewRelation(Relation relation) {
        newRelations.add(relation);
    }

    public Set<Relation> getNewRelations() {
        return newRelations;
    }

    public Set<String> getRemovedAttributes() {
        return removedAttributes;
    }

    public Map<String, Attribute> getAttributeCache() {
        return attributeCache;
    }


    // --------- visible for testing - code smells, needs further refinement -----

    @VisibleForTesting
    public Map<ConceptId, Concept> getConceptCache() {
        return conceptCache;
    }

    @VisibleForTesting
    public Map<Label, SchemaConcept> getSchemaConceptCache() {
        return schemaConceptCache;
    }

    @VisibleForTesting
    public Set<Thing> getInferredInstances() {
        return inferredConcepts;
    }

    @VisibleForTesting
    public Set<Pair<Thing, Attribute<?>>> getInferredOwnerships() {
        return inferredOwnerships;
    }
}
