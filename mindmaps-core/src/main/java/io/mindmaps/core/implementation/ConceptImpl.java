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

package io.mindmaps.core.implementation;

import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.HashSet;
import java.util.Set;

/**
 * A concept which can represent anything in the graph
 * @param <T> The leaf interface of the object model. For example an EntityType, Entity, RelationType etc . . .
 * @param <V> The type of the concept.
 * @param <D> The data type of the value in the concept.
 */
abstract class ConceptImpl<T extends Concept, V extends Type, D> implements Concept {
    @SuppressWarnings("unchecked")
    T getThis(){
        return (T) this;
    }

    final AbstractMindmapsTransaction mindmapsTransaction;
    private Vertex vertex;

    ConceptImpl(Vertex v, AbstractMindmapsTransaction mindmapsTransaction){
        this.vertex = v;
        this.mindmapsTransaction = mindmapsTransaction;
        mindmapsTransaction.getConceptLog().putConcept(this);
    }

    /**
     *
     * @param key The key of the property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface itself
     */
    private T setProperty(String key, Object value){
        if(value == null)
            vertex.property(key).remove();
        else
            vertex.property(key, value);
        return getThis();
    }

    /**
     *
     * @param key The key of the property to retrieve
     * @return The value in the property
     */
    private Object getProperty(String key){
        VertexProperty property = vertex.property(key);
        if(property != null && property.isPresent())
            return property.value();
        else
            return null;
    }

    /**
     * Deletes the concept.
     * @throws ConceptException Throws an exception if the node has any edges attached to it.
     */
    @Override
    public void delete() throws ConceptException {
        ConceptImpl properType = getMindmapsTransaction().getElementFactory().buildUnknownConcept(this);
        properType.innerDelete(); //This will execute the proper deletion method.
    }

    /**
     * Helper method to call the appropriate deletion based on the type of the concept.
     */
    //TODO: Check if this is actually the right way of doing things. This is quite odd.
    void innerDelete(){
        deleteNode();
    }

    /**
     *
     * @param id The new unique id of the concept.
     * @return The concept itself casted to the correct interface itself
     */
    @Override
    public T setId(String id) {
        if(DataType.ConceptMeta.isMetaId(id)){
            throw new ConceptException(ErrorMessage.ID_RESERVED.getMessage(id));
        }

        return setUniqueProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, id);
    }

    /**
     *
     * @param subject The new unique subject of the concept.
     * @return The concept itself casted to the correct interface itself
     */
    @Override
    public T setSubject(String subject) {
        return setUniqueProperty(DataType.ConceptPropertyUnique.SUBJECT_IDENTIFIER, subject);
    }

    /**
     *
     * @param key The key of the unique property to mutate
     * @param id The new value of the unique property
     * @return The concept itself casted to the correct interface itself
     */
    T setUniqueProperty(DataType.ConceptPropertyUnique key, String id){
        if(mindmapsTransaction.isBatchLoadingEnabled() || updateAllowed(key, id))
            return setProperty(key, id);
        else
            throw new ConceptIdNotUniqueException(this, key, id);
    }

    /**
     *
     * @param key The key of the unique property to mutate
     * @param value The value to check
     * @return True if the concept can be updated. I.e. the value is unique for the property.
     */
    private boolean updateAllowed(DataType.ConceptPropertyUnique key, String value) {
        ConceptImpl fetchedConcept = mindmapsTransaction.getConcept(key, value);
        return fetchedConcept == null || this.equals(fetchedConcept);
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    void deleteNode(){
        // tracking
        vertex.edges(Direction.BOTH).
                forEachRemaining(
                        e -> {
                            mindmapsTransaction.getConceptLog().putConcept(getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.inVertex()));
                            mindmapsTransaction.getConceptLog().putConcept(getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.outVertex()));}
                );
        mindmapsTransaction.getConceptLog().removeConcept(this);
        // delete node
        vertex.remove();
        vertex = null;
    }

    /**
     *
     * @return The type of the concept casted to the correct interface
     */
    @SuppressWarnings("unchecked")
    @Override
    public V type() {
        HashSet<Concept> visitedConcepts = new HashSet<>();
        ConceptImpl currentConcept = this;
        visitedConcepts.add(currentConcept);
        Type type = null;
        boolean notFound = true;

        while(notFound){
            ConceptImpl concept = currentConcept.getParentIsa();
            if(concept != null){
                //Checks the following case c1 -ako-> c2 -ako-> c3 -isa-> c1 is invalid
                if(visitedConcepts.contains(concept) && !concept.equals(currentConcept)){
                    throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), DataType.EdgeLabel.AKO.getLabel() + " " + DataType.EdgeLabel.ISA.getLabel()));
                }
                notFound = false;
                type = getMindmapsTransaction().getElementFactory().buildSpecificConceptType(concept);
            } else {
                currentConcept = currentConcept.getParentAko();
                if(visitedConcepts.contains(currentConcept)){
                    throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), DataType.EdgeLabel.AKO.getLabel() + " " + DataType.EdgeLabel.ISA.getLabel()));
                }
                visitedConcepts.add(currentConcept);

            }
        }

        return (V) type;
    }

    /**
     * Helper method to cast a concept to it's correct type
     * @param type The type to cast to
     * @param <E> The type of the interface we are casting to.
     * @return The concept itself casted to the defined interface
     * @throws InvalidConceptTypeException when casting a concept incorrectly
     */
    private <E> E castConcept(Class<E> type){
        try {
            return type.cast(this);
        } catch(ClassCastException e){
            throw new InvalidConceptTypeException(this, type);
        }
    }

    /**
     *
     * @return A Type if the concept is a Type
     */
    @Override
    public Type asType() {
        return castConcept(Type.class);
    }

    /**
     *
     * @return An Instance if the concept is an Instance
     */
    @Override
    public Instance asInstance() {
        return castConcept(Instance.class);
    }

    /**
     *
     * @return A Entity Type if the concept is a Entity Type
     */
    @Override
    public EntityType asEntityType() {
        return castConcept(EntityType.class);
    }

    /**
     *
     * @return A Role Type if the concept is a Role Type
     */
    @Override
    public RoleType asRoleType() {
        return castConcept(RoleType.class);
    }

    /**
     *
     * @return A Relation Type if the concept is a Relation Type
     */
    @Override
    public RelationType asRelationType() {
        return castConcept(RelationType.class);
    }

    /**
     *
     * @return A Resource Type if the concept is a Resource Type
     */
    @SuppressWarnings("unchecked")
    @Override
    public <D> ResourceType<D> asResourceType() {
        return castConcept(ResourceType.class);
    }

    /**
     *
     * @return A Rule Type if the concept is a Rule Type
     */
    @Override
    public RuleType asRuleType() {
        return castConcept(RuleType.class);
    }

    /**
     *
     * @return An Entity if the concept is an Instance
     */
    @Override
    public Entity asEntity() {
        return castConcept(Entity.class);
    }

    /**
     *
     * @return A Relation if the concept is a Relation
     */
    @Override
    public Relation asRelation() {
        return castConcept(Relation.class);
    }

    /**
     *
     * @return A Resource if the concept is a Resource
     */
    @SuppressWarnings("unchecked")
    @Override
    public <D> Resource<D> asResource() {
        return castConcept(Resource.class);
    }

    /**
     *
     * @return A Rule if the concept is a Rule
     */@Override
    public Rule asRule() {
        return castConcept(Rule.class);
    }

    /**
     *
     * @return A casting if the concept is a casting
     */
    public CastingImpl asCasting(){
        return (CastingImpl) this;
    }

    /**
     *
     * @return true if the concept is a Type
     */
    @Override
    public boolean isType() {
        return this instanceof Type;
    }

    /**
     *
     * @return true if the concept is an Instance
     */
    @Override
    public boolean isInstance() {
        return this instanceof Instance;
    }

    /**
     *
     * @return true if the concept is a Entity Type
     */
    @Override
    public boolean isEntityType() {
        return this instanceof EntityType;
    }

    /**
     *
     * @return true if the concept is a Role Type
     */
    @Override
    public boolean isRoleType() {
        return this instanceof RoleType;
    }

    /**
     *
     * @return true if the concept is a Relation Type
     */
    @Override
    public boolean isRelationType() {
        return this instanceof RelationType;
    }

    /**
     *
     * @return true if the concept is a Resource Type
     */
    @Override
    public boolean isResourceType() {
        return this instanceof ResourceType;
    }

    /**
     *
     * @return true if the concept is a Rule Type
     */
    @Override
    public boolean isRuleType() {
        return this instanceof RuleType;
    }

    /**
     *
     * @return true if the concept is a Entity
     */
    @Override
    public boolean isEntity() {
        return this instanceof Entity;
    }

    /**
     *
     * @return true if the concept is a Relation
     */
    @Override
    public boolean isRelation() {
        return this instanceof Relation;
    }

    /**
     *
     * @return true if the concept is a Resource
     */
    @Override
    public boolean isResource() {
        return this instanceof Resource;
    }

    /**
     *
     * @return true if the concept is a Rule
     */
    @Override
    public boolean isRule() {
        return this instanceof Rule;
    }

    /**
     *
     * @return true if the concept is a casting
     */
    public boolean isCasting(){
        return this instanceof CastingImpl;
    }

    /**
     *
     * @param type The type of this concept
     * @return The concept itself casted to the correct interface
     */
    public T type(Type type) {
        deleteEdges(Direction.OUT, DataType.EdgeLabel.ISA);
        putEdge(getMindmapsTransaction().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.ISA);
        setType(String.valueOf(type.getId()));

        //Put any castings back into tracking to make sure the type is still valid
        getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER).forEach(casting -> {
            mindmapsTransaction.getConceptLog().putConcept(casting);
        });

        return getThis();
    }


    /**
     *
     * @return All of this concept's types going upwards. I.e. the result of calling {@link ConceptImpl#type()}
     */
    public Set<Type> getConceptTypeHierarchy() {
        HashSet<Type> types = new HashSet<>();
        Concept currentConcept = this;
        boolean hasMoreParents = true;
        while(hasMoreParents){
            Type type = currentConcept.type();
            if(type == null || types.contains(type)){
                hasMoreParents = false;
            } else {
                types.add(type);
                currentConcept = type;
            }
        }
        return types;
    }

    /**
     *
     * @return The result of following one outgoing isa edge to a Type.
     */
    public TypeImpl getParentIsa(){
        Concept isaParent = getOutgoingNeighbour(DataType.EdgeLabel.ISA);
        if(isaParent != null){
            return getMindmapsTransaction().getElementFactory().buildSpecificConceptType(isaParent);
        } else {
            return null;
        }
    }

    /**
     *
     * @return The result of following one outgoing ako edge to a Type.
     */
    public TypeImpl getParentAko(){
        Concept akoParent = getOutgoingNeighbour(DataType.EdgeLabel.AKO);
        if(akoParent != null){
            return getMindmapsTransaction().getElementFactory().buildSpecificConceptType(akoParent);
        } else {
            return null;
        }
    }

    /**
     *
     * @param edgeLabel The edge label to traverse
     * @return The neighbouring concept found by traversing one outgoing edge of a specific type
     */
    protected Concept getOutgoingNeighbour(DataType.EdgeLabel edgeLabel){
        Set<ConceptImpl> concepts = getOutgoingNeighbours(edgeLabel);
        if(concepts.size() == 1){
            return concepts.iterator().next();
        } else if(concepts.isEmpty()){
            return null;
        } else {
            throw new MoreThanOneEdgeException(this, edgeLabel);
        }
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing outgoing edges of a specific type
     */
    protected Set<ConceptImpl> getOutgoingNeighbours(DataType.EdgeLabel edgeType){
        Set<ConceptImpl> outgoingNeighbours = new HashSet<>();

        getEdgesOfType(Direction.OUT, edgeType).forEach(edge -> {
            outgoingNeighbours.add(edge.getTarget());
        });
        return outgoingNeighbours;
    }

    /**
     *
     * @param edgeLabel The edge label to traverse
     * @return The neighbouring concept found by traversing one incoming edge of a specific type
     */
    Concept getIncomingNeighbour(DataType.EdgeLabel edgeLabel){
        Set<ConceptImpl> concepts = getIncomingNeighbours(edgeLabel);
        if(concepts.size() == 1){
            return concepts.iterator().next();
        } else if(concepts.isEmpty()){
            return null;
        } else {
            throw new MoreThanOneEdgeException(this, edgeLabel);
        }
    }
    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing incoming edges of a specific type
     */
    protected Set<ConceptImpl> getIncomingNeighbours(DataType.EdgeLabel edgeType){
        Set<ConceptImpl> incomingNeighbours = new HashSet<>();
        getEdgesOfType(Direction.IN, edgeType).forEach(edge -> {
            incomingNeighbours.add(edge.getSource());
        });
        return incomingNeighbours;
    }

    /**
     *
     * @param key The key of the unique property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface
     */
    public T setProperty(DataType.ConceptPropertyUnique key, Object value) {
        return setProperty(key.name(), value);
    }

    /**
     *
     * @param key The key of the non-unique property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface
     */
    T setProperty(DataType.ConceptProperty key, Object value){
        return setProperty(key.name(), value);
    }

    /**
     *
     * @param key The key of the unique property to retrieve
     * @return The value stored in the property
     */
    public String getProperty(DataType.ConceptPropertyUnique key){
        Object property = getProperty(key.name());
        if(property == null)
            return null;
        else
            return property.toString();
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    public Object getProperty(DataType.ConceptProperty key){
        return getProperty(key.name());
    }

    /**
     *
     * @return The tinkerpop vertex
     */
    Vertex getVertex() {
        return vertex;
    }

    //------------ Setters ------------
    /**
     *
     * @param type The type of this concept
     * @return The concept itself casted to the correct interface
     */
    public T setType(String type){
        return setProperty(DataType.ConceptProperty.TYPE, type);
    }

    /**
     *
     * @param value The value of this concept
     * @return The concept itself casted to the correct interface
     */
    public T setValue(D value) {
        return setProperty(DataType.ConceptProperty.VALUE_STRING, value);
    }

    //------------ Getters ------------
    /**
     *
     * @return The unique base identifier of this concept.
     */
    public long getBaseIdentifier() {
        return (long) vertex.id();
    }

    /**
     *
     * @return The base ttpe of this concept which helps us identify the concept
     */
    public String getBaseType(){
        return vertex.label();
    }

    /**
     *
     * @return A string representing the concept's unique id.
     */
    @Override
    public String getId(){
        return getProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER);
    }

    /**
     *
     * @return A string representing the concept's unique subject.
     */
    @Override
    public String getSubject() {
        return getProperty(DataType.ConceptPropertyUnique.SUBJECT_IDENTIFIER);
    }

    /**
     *
     * @return The id of the type of this concept. This is a shortcut used to prevent traversals.
     */
    public String getType(){
        return String.valueOf(getProperty(DataType.ConceptProperty.TYPE));
    }

    /**
     *
     * @return The value of this concept
     */
    @SuppressWarnings("unchecked")
    public D getValue() {
        return (D) getProperty(DataType.ConceptProperty.VALUE_STRING);
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     * @return A collection of edges from this concept in a particular direction of a specific type
     */
    protected Set<EdgeImpl> getEdgesOfType(Direction direction, DataType.EdgeLabel type){
        Set<EdgeImpl> edges = new HashSet<>();
        vertex.edges(direction, type.getLabel()).
                forEachRemaining(e -> edges.add(new EdgeImpl(e, getMindmapsTransaction())));
        return edges;
    }

    /**
     *
     * @param type The type of the edge to retrieve
     * @return An edge from this concept in a particular direction of a specific type
     * @throws MoreThanOneEdgeException when more than one edge of s specific type
     */
    public EdgeImpl getEdgeOutgoingOfType(DataType.EdgeLabel type) {
        Set<EdgeImpl> edges = getEdgesOfType(Direction.OUT, type);
        if(edges.size() == 1)
            return edges.iterator().next();
        else if(edges.size() > 1)
            throw new MoreThanOneEdgeException(this, type);
        else
            return null;
    }

    /**
     *
     * @return The mindmaps transaction this concept is bound to.
     */
    AbstractMindmapsTransaction getMindmapsTransaction() {return mindmapsTransaction;}

    //--------- Create Links -------//
    /**
     *
     * @param toConcept the target concept
     * @param type the type of the edge to create
     */
    void putEdge(ConceptImpl toConcept, DataType.EdgeLabel type){
        GraphTraversal<Vertex, Edge> traversal = mindmapsTransaction.getTinkerPopGraph().traversal().V(getBaseIdentifier()).outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getBaseIdentifier()).select("edge");
        if(!traversal.hasNext())
            addEdge(toConcept, type);
    }

    /**
     *
     * @param toConcept the target concept
     * @param type the type of the edge to create
     * @return The edge created
     */
    public EdgeImpl addEdge(ConceptImpl toConcept, DataType.EdgeLabel type) {
        mindmapsTransaction.getConceptLog().putConcept(this);
        mindmapsTransaction.getConceptLog().putConcept(toConcept);

        return getMindmapsTransaction().getElementFactory().buildEdge(toConcept.addEdgeFrom(this.vertex, type.getLabel()), mindmapsTransaction);
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     */
    void deleteEdges(Direction direction, DataType.EdgeLabel type){
        // track changes
        vertex.edges(direction, type.getLabel()).
                forEachRemaining(
                        e -> {
                            mindmapsTransaction.getConceptLog().putConcept(
                                    getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.inVertex()));
                            mindmapsTransaction.getConceptLog().putConcept(
                                    getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.outVertex()));
                        }
                );

        // deletion
        vertex.edges(direction, type.getLabel()).forEachRemaining(Element::remove);
    }

    /**
     * Deletes an edge of a specific type going to a specific concept
     * @param type The type of the edge
     * @param toConcept The target concept
     */
    void deleteEdgeTo(DataType.EdgeLabel type, ConceptImpl toConcept){
        GraphTraversal<Vertex, Edge> traversal = mindmapsTransaction.getTinkerPopGraph().traversal().V(getBaseIdentifier()).
                outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getBaseIdentifier()).select("edge");
        if(traversal.hasNext())
            traversal.next().remove();
    }

    private org.apache.tinkerpop.gremlin.structure.Edge addEdgeFrom(Vertex fromVertex, String type) {
        return fromVertex.addEdge(type, vertex);
    }


    //------------ Base Equality ------------
    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return vertex.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof ConceptImpl && ((ConceptImpl) object).getVertex().equals(vertex);
    }

    @Override
    public String toString(){
        String message = "[" +  this.hashCode() + "] "+
                "- Base Type [" + getBaseType() + "] ";
        if(getSubject() != null)
            message = message +  "- Subject Identifier [" + getSubject() + "] ";
        if(getId() != null)
            message = message + "- Item Identifier [" + getId() + "] ";
        if(getValue() != null)
            message = message + "- Value [" + getValue() + "] ";

        return message;
    }

    //---------- Null Vertex Handler ---------
    /**
     * Checks if the underlaying vertex has not been removed and if it is not a ghost
     * @return true if the underlying vertex has not been removed.
     */
    public boolean isAlive () {
        if(vertex == null)
            return false;

        try {
            return vertex.edges(Direction.BOTH).hasNext();
        } catch (IllegalStateException e){
            return false;
        }
    }
    
    @Override
    public int compareTo(Concept o) {
        return this.getId().compareTo(o.getId());
    }
}
