/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */
package ai.grakn.migration.export;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarPattern;

import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;

/**
 * Map Grakn Core type to equivalent Graql representation
 * @author alexandraorth
 */
public class SchemaConceptMapper {

    /**
     * Map a Type to the Graql string representation
     * @param schemaConcept type to be mapped
     * @return Graql var equivalent to the given type
     */
    public static VarPattern map(SchemaConcept schemaConcept) {
        VarPattern mapped = formatBase(schemaConcept);
        if (schemaConcept.isRelationshipType()) {
            mapped = map(mapped, schemaConcept.asRelationshipType());
        } else if (schemaConcept.isAttributeType()) {
            mapped = map(mapped, schemaConcept.asAttributeType());
        }

        return mapped;
    }

    /**
     * Map a {@link RelationshipType} to a {@link VarPattern} with all of the relates edges
     * @param var holder var with basic information
     * @param relationshipType type to be mapped
     * @return var with {@link RelationshipType} specific metadata
     */
    private static VarPattern map(VarPattern var, RelationshipType relationshipType) {
        return relates(var, relationshipType);
    }

    /**
     * Map a {@link AttributeType} to a {@link VarPattern} with the datatype
     * @param var holder var with basic information
     * @param attributeType type to be mapped
     * @return var with {@link AttributeType} specific metadata
     */
    private static VarPattern map(VarPattern var, AttributeType attributeType) {
        return datatype(var, attributeType);
    }

    /**
     * Create a var with the information underlying all Types
     * @param schemaConcept type to be mapped
     * @return {@link VarPattern} containing basic information about the given type
     */
    private static VarPattern formatBase(SchemaConcept schemaConcept) {
        VarPattern var = var().label(schemaConcept.label());

        SchemaConcept superType = schemaConcept.sup();
        if (schemaConcept.sup() != null) {
            var = var.sub(Graql.label(superType.label()));
        }

        if(schemaConcept.isType()) {
            Type type = schemaConcept.asType();
            var = plays(var, type);
            var = isAbstract(var, type);
        }

        return var;
    }

    /**
     * Add is-abstract annotation to a var
     * @param var var to be marked
     * @param type type from which metadata extracted
     */
    private static VarPattern isAbstract(VarPattern var, Type type) {
       return type.isAbstract() ? var.isAbstract() : var;
    }

    /**
     * Add plays edges to a var, given a type
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate plays edges
     */
    private static VarPattern plays(VarPattern var, Type type) {
        for(Role role:type.playing().collect(Collectors.toSet())){
            var = var.plays(Graql.label(role.label()));
        }
        return var;
    }

    /**
     * Add relates edges to a var, given a type
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate relates edges
     */
    private static VarPattern relates(VarPattern var, RelationshipType type){
        for(Role role:type.roles().collect(Collectors.toSet())){
            var = var.relates(Graql.label(role.label()));
        }
        return var;
    }

    /**
     * Add a datatype to a resource type var
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate datatype
     */
    private static VarPattern datatype(VarPattern var, AttributeType type) {
        AttributeType.DataType dataType = type.dataType();
        if (dataType != null) {
            return var.datatype(dataType);
        } else {
            return var;
        }
    }
}
