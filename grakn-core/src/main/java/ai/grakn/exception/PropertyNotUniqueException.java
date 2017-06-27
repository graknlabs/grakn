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

package ai.grakn.exception;

import ai.grakn.concept.Concept;
import ai.grakn.concept.TypeLabel;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.ErrorMessage.INVALID_UNIQUE_PROPERTY_MUTATION;
import static ai.grakn.util.ErrorMessage.UNIQUE_PROPERTY_TAKEN;

/**
 * <p>
 *     Unique Concept Property Violation
 * </p>
 *
 * <p>
 *     This occurs when attempting to add a globally unique property to a concept.
 *     For example when creating a {@link ai.grakn.concept.EntityType} and {@link ai.grakn.concept.RelationType} using
 *     the same {@link ai.grakn.concept.TypeLabel}
 * </p>
 *
 * @author fppt
 */
public class PropertyNotUniqueException extends GraphOperationException{
    private PropertyNotUniqueException(String error) {
        super(error);
    }

    /**
     * Thrown when trying to set the property of concept {@code mutatingConcept} to a {@code value} which is already
     * taken by concept {@code conceptWithValue}
     */
    public static PropertyNotUniqueException cannotChangeProperty(Vertex mutatingConcept, Vertex conceptWithValue, Schema.VertexProperty property, Object value){
        return new PropertyNotUniqueException(INVALID_UNIQUE_PROPERTY_MUTATION.getMessage(property, mutatingConcept, value, conceptWithValue));
    }

    /**
     * Thrown when trying to create a concept using a unique property which is already taken.
     * For example this happens when using an already taken {@link TypeLabel}
     */
    public static PropertyNotUniqueException cannotCreateProperty(Concept conceptWithValue, Schema.VertexProperty property, Object value){
        return new PropertyNotUniqueException(UNIQUE_PROPERTY_TAKEN.getMessage(property.name(), value, conceptWithValue));
    }
}
