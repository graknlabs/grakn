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

import ai.grakn.concept.EntityType;
import ai.grakn.concept.OntologyElement;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeId;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>
 *     Ontology or Schema Specific Element
 * </p>
 *
 * <p>
 *     Allows you to create schema or ontological elements.
 *     These differ from normal graph constructs in two ways:
 *     1. They have a unique {@link TypeLabel} which identifies them
 *     2. You can link them together into a hierarchical structure
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType} or {@link RelationType} or {@link RoleType}
 */
abstract class OntologyElementImpl<T extends OntologyElement> extends ConceptImpl implements OntologyElement {
    private final TypeLabel cachedLabel;
    private final TypeId cachedLabelId;

    private Cache<T> cachedSuperType = new Cache<>(() -> this.<T>neighbours(Direction.OUT, Schema.EdgeLabel.SUB).findFirst().orElse(null));
    private Cache<Set<T>> cachedDirectSubTypes = new Cache<>(() -> this.<T>neighbours(Direction.IN, Schema.EdgeLabel.SUB).collect(Collectors.toSet()));
    private Cache<Boolean> cachedIsImplicit = new Cache<>(() -> vertex().propertyBoolean(Schema.VertexProperty.IS_IMPLICIT));

    OntologyElementImpl(VertexElement vertexElement) {
        super(vertexElement);
        cachedLabel = TypeLabel.of(vertex().property(Schema.VertexProperty.TYPE_LABEL));
        cachedLabelId = TypeId.of(vertex().property(Schema.VertexProperty.TYPE_ID));
    }

    OntologyElementImpl(VertexElement vertexElement, T superType) {
        this(vertexElement);
        if(superType() == null) superType(superType);
    }

    OntologyElementImpl(VertexElement vertexElement, T superType, Boolean isImplicit) {
        this(vertexElement, superType);
        vertex().propertyImmutable(Schema.VertexProperty.IS_IMPLICIT, isImplicit, vertex().property(Schema.VertexProperty.IS_IMPLICIT), Function.identity());
        cachedIsImplicit.set(isImplicit);
    }

    /**
     *
     * @return The internal id which is used for fast lookups
     */
    @Override
    public TypeId getTypeId(){
        return cachedLabelId;
    }

    /**
     *
     * @return The label of this ontological element
     */
    @Override
    public TypeLabel getLabel() {
        return cachedLabel;
    }

    /**
     * Flushes the internal transaction caches so they can refresh with persisted graph
     */
    public void txCacheFlush(){
        cachedSuperType.flush();
        cachedDirectSubTypes.flush();
        cachedIsImplicit.flush();
    }

    /**
     * Clears the internal transaction caches
     */
    void txCacheClear(){
        cachedSuperType.clear();
        cachedDirectSubTypes.clear();
        cachedIsImplicit.clear();
    }

    /**
     *
     * @return The super of this Ontology Element
     */
    public T superType() {
        return cachedSuperType.get();
    }

    /**
     *
     * @return All outgoing sub parents including itself
     */
    Set<T> superSet() {
        Set<T> superSet= new HashSet<>();
        superSet.add(getThis());
        T superParent = superType();

        while(superParent != null && !Schema.MetaSchema.THING.getLabel().equals(superParent.getLabel())){
            superSet.add(superParent);
            //noinspection unchecked
            superParent = (T) superParent.superType();
        }

        return superSet;
    }

    /**
     *
     * @return returns true if the type was created implicitly through the resource syntax
     */
    @Override
    public Boolean isImplicit(){
        return cachedIsImplicit.get();
    }

    /**
     * Deletes the concept as an Ontology Element
     */
    @Override
    public void delete(){
        checkOntologyMutationAllowed();
        boolean hasSubs = neighbours(Direction.IN, Schema.EdgeLabel.SUB).findAny().isPresent();
        boolean hasInstances = currentShard().links().findAny().isPresent();

        if(hasSubs || hasInstances){
            throw GraphOperationException.typeCannotBeDeleted(getLabel());
        } else {
            //Force load of linked concepts whose caches need to be updated
            //noinspection unchecked
            cachedSuperType.get();

            deleteNode();

            //Update neighbouring caches
            //noinspection unchecked
            ((OntologyElementImpl<OntologyElement>) cachedSuperType.get()).deleteCachedDirectedSubType(getThis());

            //Clear internal caching
            txCacheClear();

            //Clear Global Cache
            vertex().graph().txCache().remove(this);
        }
    }

    /**
     *
     * @return All the subs of this concept including itself
     */
    @Override
    public Collection<T> subTypes(){
        return Collections.unmodifiableCollection(nextSubLevel(this));
    }

    /**
     * Adds a new sub type to the currently cached sub types. If no subtypes have been cached then this will hit the database.
     *
     * @param newSubType The new subtype
     */
    private void addCachedDirectSubType(T newSubType){
        cachedDirectSubTypes.ifPresent(set -> set.add(newSubType));
    }

    /**
     *
     * @param root The current Ontology Element
     * @return All the sub children of the root. Effectively calls  the cache {@link OntologyElementImpl#cachedDirectSubTypes} recursively
     */
    @SuppressWarnings("unchecked")
    private Set<T> nextSubLevel(OntologyElementImpl<T> root){
        Set<T> results = new HashSet<>();
        results.add((T) root);

        Set<T> children = root.cachedDirectSubTypes.get();
        for(T child: children){
            results.addAll(nextSubLevel((OntologyElementImpl<T>) child));
        }

        return results;
    }

    /**
     * Checks if we are mutating an ontology element in a valid way. Ontology mutations are valid if:
     * 1. The Ontology Element is not a meta-type
     * 2. The graph is not batch loading
     */
    void checkOntologyMutationAllowed(){
        vertex().graph().checkOntologyMutationAllowed();
        if(Schema.MetaSchema.isMetaLabel(getLabel())){
            throw GraphOperationException.metaTypeImmutable(getLabel());
        }
    }

    /**
     * Removes an old sub type from the currently cached sub types. If no subtypes have been cached then this will hit the database.
     *
     * @param oldSubType The old sub type which should not be cached anymore
     */
    void deleteCachedDirectedSubType(T oldSubType){
        cachedDirectSubTypes.ifPresent(set -> set.remove(oldSubType));
    }

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this type
     * @return The Type itself
     */
    public T subType(T type){
        //noinspection unchecked
        ((TypeImpl) type).superType(this);
        return getThis();
    }

    /**
     *
     * @param newSuperType This type's super type
     * @return The Type itself
     */
    public T superType(T newSuperType) {
        checkOntologyMutationAllowed();

        T oldSuperType = superType();
        if(oldSuperType == null || (!oldSuperType.equals(newSuperType))) {
            //Update the super type of this type in cache
            cachedSuperType.set(newSuperType);

            //Note the check before the actual construction
            if(superLoops()){
                cachedSuperType.set(oldSuperType); //Reset if the new super type causes a loop
                throw GraphOperationException.loopCreated(this, newSuperType);
            }

            //Modify the graph once we have checked no loop occurs
            deleteEdge(Direction.OUT, Schema.EdgeLabel.SUB);
            putEdge(newSuperType, Schema.EdgeLabel.SUB);

            //Update the sub types of the old super type
            if(oldSuperType != null) {
                //noinspection unchecked - Casting is needed to access {deleteCachedDirectedSubTypes} method
                ((OntologyElementImpl<T>) oldSuperType).deleteCachedDirectedSubType(getThis());
            }

            //Add this as the subtype to the supertype
            //noinspection unchecked - Casting is needed to access {addCachedDirectSubTypes} method
            ((OntologyElementImpl<T>) newSuperType).addCachedDirectSubType(getThis());

            //Track any existing data if there is some
            trackSuperChange();
        }

        return getThis();
    }

    /**
     * Method which performs tasks needed in order to track super changes properly
     */
    abstract void trackSuperChange();

    private boolean superLoops(){
        //Check For Loop
        HashSet<OntologyElement> foundTypes = new HashSet<>();
        OntologyElement currentSuperType = superType();
        while (currentSuperType != null){
            foundTypes.add(currentSuperType);
            currentSuperType = currentSuperType.superType();
            if(foundTypes.contains(currentSuperType)){
                return true;
            }
        }
        return false;
    }
}
