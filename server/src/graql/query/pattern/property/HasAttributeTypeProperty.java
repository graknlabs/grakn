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

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.Statement;

import java.util.stream.Stream;

import static grakn.core.graql.internal.Schema.ImplicitType.KEY;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_VALUE;
import static grakn.core.graql.query.Graql.var;

/**
 * Represents the {@code has} and {@code key} properties on a Type.
 * This property can be queried or inserted. Whether this is a key is indicated by the
 * HasAttributeTypeProperty#isKey field.
 * This property is defined as an implicit ontological structure between a Type and a AttributeType,
 * including one implicit RelationshipType and two implicit Roles. The labels of these types are derived
 * from the label of the AttributeType.
 * Like HasAttributeProperty, if this is not a key and is used in a match clause it will not use the implicit
 * structure - instead, it will match if there is any kind of relation type connecting the two types.
 */
public class HasAttributeTypeProperty extends VarProperty {

    private final Statement attributeType;
    private final Statement ownerRole;
    private final Statement valueRole;
    private final Statement relationOwner;
    private final Statement relationValue;
    private final boolean isKey;

    public HasAttributeTypeProperty(Statement attributeType, boolean isKey) {
        Label resourceLabel = attributeType.getTypeLabel().orElseThrow(
                () -> GraqlQueryException.noLabelSpecifiedForHas(attributeType)
        );

        Statement role = Graql.type(Schema.MetaSchema.ROLE.getLabel().getValue());

        Statement ownerRole = var().sub(role);
        Statement valueRole = var().sub(role);
        Statement relationType = var().sub(Graql.type(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()));

        // If a key, limit only to the implicit key type
        if (isKey) {
            ownerRole = ownerRole.type(KEY_OWNER.getLabel(resourceLabel).getValue());
            valueRole = valueRole.type(KEY_VALUE.getLabel(resourceLabel).getValue());
            relationType = relationType.type(KEY.getLabel(resourceLabel).getValue());
        }

        Statement relationOwner = relationType.relates(ownerRole);
        Statement relationValue = relationType.relates(valueRole);

        this.attributeType = attributeType;
        this.ownerRole = ownerRole;
        this.valueRole = valueRole;
        if (relationOwner == null) {
            throw new NullPointerException("Null relationOwner");
        }
        this.relationOwner = relationOwner;
        if (relationValue == null) {
            throw new NullPointerException("Null relationValue");
        }
        this.relationValue = relationValue;
        this.isKey = isKey;
    }

    public Statement attributeType() {
        return attributeType;
    }

    public Statement ownerRole() {
        return ownerRole;
    }

    public Statement valueRole() {
        return valueRole;
    }

    public boolean isKey() {
        return isKey;
    }

    @Override
    public String keyword() {
        return isKey ? Query.Property.KEY.toString() : Query.Property.HAS.toString();
    }

    @Override
    public String property() {
        return attributeType.getPrintableName();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Stream<Statement> types() {
        return Stream.of(attributeType);
    }

    @Override
    public Stream<Statement> innerStatements() {
        return Stream.of(attributeType);
    }

    @Override
    public Stream<Statement> implicitInnerStatements() {
        return Stream.of(attributeType, ownerRole, valueRole, relationOwner, relationValue);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof HasAttributeTypeProperty) {
            HasAttributeTypeProperty that = (HasAttributeTypeProperty) o;
            return (this.attributeType.equals(that.attributeType))
                    && (this.ownerRole.equals(that.ownerRole))
                    && (this.valueRole.equals(that.valueRole))
                    && (this.relationOwner.equals(that.relationOwner))
                    && (this.relationValue.equals(that.relationValue))
                    && (this.isKey == that.isKey);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.attributeType.hashCode();
        h *= 1000003;
        h ^= this.ownerRole.hashCode();
        h *= 1000003;
        h ^= this.valueRole.hashCode();
        h *= 1000003;
        h ^= this.relationOwner.hashCode();
        h *= 1000003;
        h ^= this.relationValue.hashCode();
        h *= 1000003;
        h ^= this.isKey ? 1231 : 1237;
        return h;
    }
}
