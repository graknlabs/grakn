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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static scala.tools.scalap.scalax.rules.scalasig.NoSymbol.isAbstract;

/**
 * <p>
 *     Ontology or Schema Specific Element
 * </p>
 *
 * <p>
 *     Allows you to create schema or ontological elements.
 *     These differ from normal graph constructs in two ways:
 *     1. They have a unique {@link Label} which identifies them
 *     2. You can link them together into a hierarchical structure
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType} or {@link RelationType} or {@link Role}
 */
abstract class OntologyConceptImpl<T extends OntologyConcept> extends ConceptImpl implements OntologyConcept {
    private final Label cachedLabel;
    private final LabelId cachedLabelId;

    private Cache<T> cachedSuperType = new Cache<>(() -> this.<T>neighbours(Direction.OUT, Schema.EdgeLabel.SUB).findFirst().orElse(null));
    private Cache<Set<T>> cachedDirectSubTypes = new Cache<>(() -> this.<T>neighbours(Direction.IN, Schema.EdgeLabel.SUB).collect(Collectors.toSet()));
    private Cache<Boolean> cachedIsImplicit = new Cache<>(() -> vertex().propertyBoolean(Schema.VertexProperty.IS_IMPLICIT));

    OntologyConceptImpl(VertexElement vertexElement) {
        super(vertexElement);
        cachedLabel = Label.of(vertex().property(Schema.VertexProperty.TYPE_LABEL));
        cachedLabelId = LabelId.of(vertex().property(Schema.VertexProperty.TYPE_ID));
    }

    OntologyConceptImpl(VertexElement vertexElement, T superType) {
        this(vertexElement);
        if(sup() == null) sup(superType);
    }

    OntologyConceptImpl(VertexElement vertexElement, T superType, Boolean isImplicit) {
        this(vertexElement, superType);
        vertex().propertyImmutable(Schema.VertexProperty.IS_IMPLICIT, isImplicit, vertex().property(Schema.VertexProperty.IS_IMPLICIT), Function.identity());
        cachedIsImplicit.set(isImplicit);
    }

    /**
     *
     * @return The internal id which is used for fast lookups
     */
    @Override
    public LabelId getTypeId(){
        return cachedLabelId;
    }

    /**
     *
     * @return The label of this ontological element
     */
    @Override
    public Label getLabel() {
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
    public T sup() {
        return cachedSuperType.get();
    }

    /**
     *
     * @return All outgoing sub parents including itself
     */
    Set<T> superSet() {
        Set<T> superSet= new HashSet<>();
        superSet.add(getThis());
        T superParent = sup();

        while(superParent != null && !Schema.MetaSchema.THING.getLabel().equals(superParent.getLabel())){
            superSet.add(superParent);
            //noinspection unchecked
            superParent = (T) superParent.sup();
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
        if(deletionAllowed()){
            //Force load of linked concepts whose caches need to be updated
            //noinspection unchecked
            cachedSuperType.get();

            deleteNode();

            //Update neighbouring caches
            //noinspection unchecked
            ((OntologyConceptImpl<OntologyConcept>) cachedSuperType.get()).deleteCachedDirectedSubType(getThis());

            //Clear internal caching
            txCacheClear();

            //Clear Global Cache
            vertex().graph().txCache().remove(this);
        } else {
            throw GraphOperationException.typeCannotBeDeleted(getLabel());
        }
    }

    boolean deletionAllowed(){
        checkOntologyMutationAllowed();
        return !neighbours(Direction.IN, Schema.EdgeLabel.SUB).findAny().isPresent();
    }

    /**
     *
     * @return All the subs of this concept including itself
     */
    @Override
    public Collection<T> subs(){
        return Collections.unmodifiableCollection(filterImplicitStructures(nextSubLevel(this)));
    }

    <X extends Concept> Collection<X> filterImplicitStructures(Collection<X> types){
        if (!vertex().graph().implicitConceptsVisible() && !types.isEmpty() && types.iterator().next().isOntologyConcept()) {
            return types.stream().filter(t -> !t.asOntologyConcept().isImplicit()).collect(Collectors.toSet());
        }
        return types;
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
     * @return All the sub children of the root. Effectively calls  the cache {@link OntologyConceptImpl#cachedDirectSubTypes} recursively
     */
    @SuppressWarnings("unchecked")
    private Set<T> nextSubLevel(OntologyConceptImpl<T> root){
        Set<T> results = new HashSet<>();
        results.add((T) root);

        Set<T> children = root.cachedDirectSubTypes.get();
        for(T child: children){
            results.addAll(nextSubLevel((OntologyConceptImpl<T>) child));
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
    public T sub(T type){
        //noinspection unchecked
        ((TypeImpl) type).sup(this);
        return getThis();
    }

    /**
     *
     * @param newSuperType This type's super type
     * @return The Type itself
     */
    public T sup(T newSuperType) {
        checkOntologyMutationAllowed();

        T oldSuperType = sup();
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
                ((OntologyConceptImpl<T>) oldSuperType).deleteCachedDirectedSubType(getThis());
            }

            //Add this as the subtype to the supertype
            //noinspection unchecked - Casting is needed to access {addCachedDirectSubTypes} method
            ((OntologyConceptImpl<T>) newSuperType).addCachedDirectSubType(getThis());

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
        HashSet<OntologyConcept> foundTypes = new HashSet<>();
        OntologyConcept currentSuperType = sup();
        while (currentSuperType != null){
            foundTypes.add(currentSuperType);
            currentSuperType = currentSuperType.sup();
            if(foundTypes.contains(currentSuperType)){
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @return A collection of {@link Rule} for which this {@link OntologyConcept} serves as a hypothesis
     */
    @Override
    public Collection<Rule> getRulesOfHypothesis() {
        Set<Rule> rules = new HashSet<>();
        neighbours(Direction.IN, Schema.EdgeLabel.HYPOTHESIS).forEach(concept -> rules.add(concept.asRule()));
        return Collections.unmodifiableCollection(rules);
    }

    /**
     *
     * @return A collection of {@link Rule} for which this {@link OntologyConcept} serves as a conclusion
     */
    @Override
    public Collection<Rule> getRulesOfConclusion() {
        Set<Rule> rules = new HashSet<>();
        neighbours(Direction.IN, Schema.EdgeLabel.CONCLUSION).forEach(concept -> rules.add(concept.asRule()));
        return Collections.unmodifiableCollection(rules);
    }

    @Override
    public String innerToString(){
        String message = super.innerToString();
        message = message + " - Label [" + getLabel() + "] - Abstract [" + isAbstract() + "] ";
        return message;
    }
}
