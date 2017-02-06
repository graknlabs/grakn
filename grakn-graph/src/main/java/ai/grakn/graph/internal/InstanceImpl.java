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
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     A data instance in the graph belonging to a specific {@link Type}
 * </p>
 *
 * <p>
 *     Instances represent data in the graph.
 *     Every instance belongs to a {@link Type} which serves as a way of categorising them.
 *     Instances can relate to one another via {@link Relation}
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept which extends {@link Instance}.
 *           For example {@link ai.grakn.concept.Entity} or {@link Relation}.
 * @param <V> The type of the concept which extends {@link Type} of the concept.
 *           For example {@link ai.grakn.concept.EntityType} or {@link RelationType}
 */
abstract class InstanceImpl<T extends Instance, V extends Type> extends ConceptImpl<T> implements Instance {
    private Optional<V> cachedType = Optional.empty();

    InstanceImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    InstanceImpl(AbstractGraknGraph graknGraph, Vertex v, V type) {
        super(graknGraph, v);
        type(type);
    }

    /**
     * Deletes the concept as an Instance
     */
    @Override
    public void innerDelete() {
        InstanceImpl<?, ?> parent = this;
        Set<CastingImpl> castings = parent.castings();
        deleteNode();
        for(CastingImpl casting: castings){
            Set<RelationImpl> relations = casting.getRelations();
            getGraknGraph().getConceptLog().putConcept(casting);

            for(RelationImpl relation : relations) {
                getGraknGraph().getConceptLog().putConcept(relation);
                relation.cleanUp();
            }

            casting.deleteNode();
        }
    }

    /**
     * This index is used by concepts such as casting and relations to speed up internal lookups
     * @return The inner index value of some concepts.
     */
    public String getIndex(){
        return getProperty(Schema.ConceptProperty.INDEX);
    }

    /**
     *
     * @return All the {@link Resource} that this Instance is linked with
     */
    public Collection<Resource<?>> resources(ResourceType... resourceTypes) {
        Set<ConceptId> resourceTypesIds = Arrays.stream(resourceTypes).map(Concept::getId).collect(Collectors.toSet());

        Set<Resource<?>> resources = new HashSet<>();
        this.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).forEach(concept -> {
            if(concept.isResource()) {
                Resource<?> resource = concept.asResource();
                if(resourceTypesIds.isEmpty() || resourceTypesIds.contains(resource.type().getId())) {
                    resources.add(resource);
                }
            }
        });
        return resources;
    }

    /**
     *
     * @return All the {@link CastingImpl} that this Instance is linked with
     */
    public Set<CastingImpl> castings(){
        Set<CastingImpl> castings = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).forEach(casting -> castings.add((CastingImpl) casting));
        return castings;
    }

    /**
     *
     * @param roleTypes An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Collection<Relation> relations(RoleType... roleTypes) {
        Set<Relation> relations = new HashSet<>();
        Set<TypeName> roleTypeNames = Arrays.stream(roleTypes).map(RoleType::getName).collect(Collectors.toSet());

        InstanceImpl<?, ?> parent = this;

        parent.castings().forEach(c -> {
            CastingImpl casting = c.asCasting();
            if (roleTypeNames.size() != 0) {
                if (roleTypeNames.contains(casting.getType())) {
                    relations.addAll(casting.getRelations());
                }
            } else {
                relations.addAll(casting.getRelations());
            }
        });

        return relations;
    }

    /**
     *
     * @return A set of all the Role Types which this instance plays.
     */
    @Override
    public Collection<RoleType> playsRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        ConceptImpl<?> parent = this;
        parent.getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).forEach(c -> roleTypes.add(((CastingImpl)c).getRole()));
        return roleTypes;
    }


    /**
     * Creates a relation from this instance to the provided resource.
     * @param resource The resource to creating a relationship to
     * @return A relation which contains both the entity and the resource
     */
    @Override
    public Relation hasResource(Resource resource){
        TypeName name = resource.type().getName();
        RelationType hasResource = getGraknGraph().getType(Schema.Resource.HAS_RESOURCE.getName(name));
        RoleType hasResourceTarget = getGraknGraph().getType(Schema.Resource.HAS_RESOURCE_OWNER.getName(name));
        RoleType hasResourceValue = getGraknGraph().getType(Schema.Resource.HAS_RESOURCE_VALUE.getName(name));

        if(hasResource == null || hasResourceTarget == null || hasResourceValue == null){
            throw new ConceptException(ErrorMessage.HAS_RESOURCE_INVALID.getMessage(type().getName(), resource.type().getName()));
        }

        Relation relation = hasResource.addRelation();
        relation.putRolePlayer(hasResourceTarget, this);
        relation.putRolePlayer(hasResourceValue, resource);

        return relation;
    }

    /**
     *
     * @param type The type of this concept
     * @return The concept itself casted to the correct interface
     */
    protected T type(V type) {
        if(type != null){
            setType(String.valueOf(type.getName()));
            putEdge(type, Schema.EdgeLabel.ISA);
            cachedType = Optional.of(type);
        }
        return getThis();
    }

    /**
     *
     * @return The type of the concept casted to the correct interface
     */
    public V type() {
        if(!cachedType.isPresent()) {
            cachedType = Optional.of(getOutgoingNeighbour(Schema.EdgeLabel.ISA));
        }
        return cachedType.get();
    }

}
