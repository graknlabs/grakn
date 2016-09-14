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

package io.mindmaps.graph.internal;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This represents an instance of a Type. It represents data in the graph.
 * @param <T> The leaf interface of the object concept. For example an EntityType, Entity, RelationType etc . . .
 * @param <V> The type of the concept.
 */
abstract class InstanceImpl<T extends Instance, V extends Type> extends ConceptImpl<T, V> implements Instance {
    InstanceImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph) {
        super(v, mindmapsGraph);
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
            getMindmapsGraph().getConceptLog().putConcept(casting);

            for(RelationImpl relation : relations) {
                getMindmapsGraph().getConceptLog().putConcept(relation);
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
        return (String) getProperty(Schema.ConceptProperty.INDEX);
    }

    /**
     *
     * @return All the {@link Resource} that this Instance is linked with
     */
    public Collection<Resource<?>> resources(ResourceType... resourceTypes) {
        Set<String> resourceTypesIds = Arrays.stream(resourceTypes).map(Concept::getId).collect(Collectors.toSet());

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
        Set<String> roleTypeItemIdentifier = Arrays.stream(roleTypes).map(Concept::getId).collect(Collectors.toSet());

        InstanceImpl<?, ?> parent = this;

        parent.castings().forEach(c -> {
            CastingImpl casting = getMindmapsGraph().getElementFactory().buildCasting(c);
            if (roleTypeItemIdentifier.size() != 0) {
                if (roleTypeItemIdentifier.contains(casting.getType()))
                    relations.addAll(casting.getRelations());
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
        ConceptImpl<?, ?> parent = this;
        parent.getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).forEach(c -> roleTypes.add(getMindmapsGraph().getElementFactory().buildCasting(c).getRole()));
        return roleTypes;
    }
}
