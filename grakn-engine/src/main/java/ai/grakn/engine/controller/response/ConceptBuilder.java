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

package ai.grakn.engine.controller.response;

import ai.grakn.exception.GraknBackendException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     Factory used to build the wrapper {@link Concept}s from real {@link ai.grakn.concept.Concept}s
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class ConceptBuilder {

    /**
     * Takes a {@link ai.grakn.concept.Concept} and returns the equivalent response object
     *
     * @param concept The {@link ai.grakn.concept.Concept} to be converted into a response object
     * @return the response object wrapper {@link Concept}
     */
    public static <X extends Concept> Optional<X> build(ai.grakn.Keyspace keyspace, ai.grakn.concept.Concept concept){
        if(concept == null) return Optional.empty();

        Concept response = null;

        if(concept.isSchemaConcept()){
            response = buildSchemaConcept(concept.asSchemaConcept());
        } else if (concept.isThing()) {
            response = buildThing(concept.asThing());
        } else {
            throw GraknBackendException.convertingUnknownConcept(concept);
        }

        return Optional.of((X) response);
    }

    //TODO: This will scale poorly with super nodes. Need to introduce some sort of paging maybe?
    private static Thing buildThing(ai.grakn.concept.Thing thing) {
        Set<Attribute> attributes = thing.attributes().map(ConceptBuilder::<Attribute>buildThingLink).collect(Collectors.toSet());
        Set<Attribute> keys = thing.attributes().map(ConceptBuilder::<Attribute>buildThingLink).collect(Collectors.toSet());
        Set<Relationship> relationships = thing.relationships().map(ConceptBuilder::<Relationship>buildThingLink).collect(Collectors.toSet());

        if(thing.isAttribute()){
            return buildAttribute(thing.asAttribute(), attributes, keys, relationships);
        } else if (thing.isRelationship()){
            return buildRelationship(thing.asRelationship(), attributes, keys, relationships);
        } else if (thing.isEntity()){
            return buildEntity(thing.asEntity(), attributes, keys, relationships);
        } else {
            throw GraknBackendException.convertingUnknownConcept(thing);
        }
    }

    private static SchemaConcept buildSchemaConcept(ai.grakn.concept.SchemaConcept schemaConcept){
        SchemaConcept sup = buildSchemaLink(schemaConcept.sup());

        Set<SchemaConcept> subs = schemaConcept.subs().
                map(ConceptBuilder::<SchemaConcept>buildSchemaLink).
                collect(Collectors.toSet());

        if(schemaConcept.isRole()){
            return buildRole(schemaConcept.asRole(), sup, subs);
        } else if(schemaConcept.isRule()){
            return buildRule(schemaConcept.asRule(), sup, subs);
        } else {
            return buildType(schemaConcept.asType(), sup, subs);
        }
    }

    private static Entity buildEntity(ai.grakn.concept.Entity entity, Set<Attribute> attributes, Set<Attribute> keys, Set<Relationship> relationships){
        return Entity.createEmbedded(entity.keyspace(), entity.getId(), attributes, keys, relationships);
    }

    private static Attribute buildAttribute(ai.grakn.concept.Attribute attribute, Set<Attribute> attributes, Set<Attribute> keys, Set<Relationship> relationships){
        return Attribute.createEmbedded(attribute.keyspace(), attribute.getId(), attributes, keys, relationships, attribute.getValue().toString());
    }

    private static Relationship buildRelationship(ai.grakn.concept.Relationship relationship, Set<Attribute> attributes, Set<Attribute> keys, Set<Relationship> relationships){
        //Get all the role players and roles part of this relationship
        Set<RolePlayer> roleplayers = new HashSet<>();
        relationship.allRolePlayers().forEach((role, things) -> {
            Role roleLink = buildSchemaLink(role);
            things.forEach(thing -> roleplayers.add(RolePlayer.create(roleLink, buildThingLink(thing))));
        });
        return Relationship.createEmbedded(relationship.keyspace(), relationship.getId(), attributes, keys, relationships, roleplayers);
    }

    private static Type buildType(ai.grakn.concept.Type type, SchemaConcept sup, Set<SchemaConcept> subs){
        Set<Role> roles = type.plays().
                map(ConceptBuilder::<Role>buildSchemaLink).
                collect(Collectors.toSet());

        Set<AttributeType> attributes = type.attributes().
                map(ConceptBuilder::<AttributeType>buildTypeLink).
                collect(Collectors.toSet());

        Set<AttributeType> keys = type.keys().
                map(ConceptBuilder::<AttributeType>buildTypeLink).
                collect(Collectors.toSet());

        if(type.isAttributeType()){
            return buildAttributeType(type.asAttributeType(), sup, subs, roles, attributes, keys);
        } else if (type.isEntityType()){
            return buildEntityType(type.asEntityType(), sup, subs, roles, attributes, keys);
        } else if (type.isRelationshipType()){
            return buildRelationshipType(type.asRelationshipType(), sup, subs, roles, attributes, keys);
        } else {
            throw GraknBackendException.convertingUnknownConcept(type);
        }
    }

    private static Role buildRole(ai.grakn.concept.Role role, SchemaConcept sup, Set<SchemaConcept> subs){
        Set<RelationshipType> relationshipTypes = role.relationshipTypes().
                map(rel-> RelationshipType.createLinkOnly(rel.keyspace(), rel.getId(), rel.getLabel())).
                collect(Collectors.toSet());

        Set<Type> playedByTypes = role.playedByTypes().
                map(ConceptBuilder::<Type>buildTypeLink).collect(Collectors.toSet());

        return Role.createEmbedded(role.keyspace(), role.getId(), role.getLabel(),
                sup, subs, role.isImplicit(), relationshipTypes, playedByTypes);
    }

    private static Rule buildRule(ai.grakn.concept.Rule rule, SchemaConcept sup, Set<SchemaConcept> subs){
        return Rule.createEmbedded(rule.keyspace(), rule.getId(), rule.getLabel(),
                sup, subs, rule.isImplicit(), rule.getWhen().toString(), rule.getThen().toString());
    }

    private static AttributeType buildAttributeType(ai.grakn.concept.AttributeType attributeType, SchemaConcept sup,
                                                    Set<SchemaConcept> subs, Set<Role> plays, Set<AttributeType> attributes,
                                                    Set<AttributeType> keys){
        return AttributeType.createEmbedded(attributeType.keyspace(), attributeType.getId(), attributeType.getLabel(),
                sup, subs, attributeType.isImplicit(), attributeType.isAbstract(), plays, attributes, keys);
    }

    private static EntityType buildEntityType(ai.grakn.concept.EntityType entityType, SchemaConcept sup,
                                                    Set<SchemaConcept> subs, Set<Role> plays, Set<AttributeType> attributes,
                                                    Set<AttributeType> keys){
        return EntityType.createEmbedded(entityType.keyspace(), entityType.getId(), entityType.getLabel(),
                sup, subs, entityType.isImplicit(), entityType.isAbstract(), plays, attributes, keys);
    }

    private static RelationshipType buildRelationshipType(ai.grakn.concept.RelationshipType relationshipType, SchemaConcept sup,
                                              Set<SchemaConcept> subs, Set<Role> plays, Set<AttributeType> attributes,
                                              Set<AttributeType> keys){
        Set<Role> relates = relationshipType.relates().
                map(ConceptBuilder::<Role>buildSchemaLink).
                collect(Collectors.toSet());

        return RelationshipType.createEmbedded(relationshipType.keyspace(), relationshipType.getId(), relationshipType.getLabel(),
                sup, subs, relationshipType.isImplicit(), relationshipType.isAbstract(), plays, attributes, keys, relates);
    }

    /**
     * Builds  a {@link SchemaConcept} response which can only serialise into a link representation.
     */
    private static <X extends SchemaConcept> X buildSchemaLink(ai.grakn.concept.SchemaConcept schemaConcept){
        if(schemaConcept.isRole()){
            return (X) Role.createLinkOnly(schemaConcept.keyspace(), schemaConcept.getId(), schemaConcept.getLabel());
        } else if (schemaConcept.isRule()){
            return (X) Rule.createLinkOnly(schemaConcept.keyspace(), schemaConcept.getId(), schemaConcept.getLabel());
        } else {
            return (X) buildTypeLink(schemaConcept.asType());
        }
    }

    /**
     * Builds  a {@link Type} response which can only serialise into a link representation.
     */
    private static <X extends Type> X buildTypeLink(ai.grakn.concept.Type type){
        if (type.isAttributeType()){
            return (X) AttributeType.createLinkOnly(type.keyspace(), type.getId(), type.getLabel());
        } else if (type.isEntityType()){
            return (X) EntityType.createLinkOnly(type.keyspace(), type.getId(), type.getLabel());
        } else if (type.isRelationshipType()){
            return (X) RelationshipType.createLinkOnly(type.keyspace(), type.getId(), type.getLabel());
        } else {
            throw GraknBackendException.convertingUnknownConcept(type);
        }
    }

    /**
     * Builds  a {@link Thing} response which can only serialise into a link representation.
     */
    private static <X extends Thing> X buildThingLink(ai.grakn.concept.Thing thing){
        if (thing.isAttribute()){
            return (X) Attribute.createLinkOnly(thing.keyspace(), thing.getId());
        } else if (thing.isEntity()){
            return (X) Entity.createLinkOnly(thing.keyspace(), thing.getId());
        } else if (thing.isRelationship()){
            return (X) Relationship.createLinkOnly(thing.keyspace(), thing.getId());
        } else {
            throw GraknBackendException.convertingUnknownConcept(thing);
        }
    }
}
