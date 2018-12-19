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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;

/**
 * Represents the {@code datatype} property on a AttributeType.
 * This property can be queried or inserted.
 */
public class DataTypeProperty extends VarProperty {

    public static final String NAME = "datatype";
    private final AttributeType.DataType<?> dataType;
    private static final ImmutableBiMap<String, AttributeType.DataType<?>> DATA_TYPES = ImmutableBiMap.of(
            "long", AttributeType.DataType.LONG,
            "double", AttributeType.DataType.DOUBLE,
            "string", AttributeType.DataType.STRING,
            "boolean", AttributeType.DataType.BOOLEAN,
            "date", AttributeType.DataType.DATE
    );

    public DataTypeProperty(
            AttributeType.DataType<?> dataType) {
        if (dataType == null) {
            throw new NullPointerException("Null dataType");
        }
        this.dataType = dataType;
    }

    public AttributeType.DataType<?> dataType() {
        return dataType;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return DATA_TYPES.inverse().get(dataType());
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.dataType(this, start, dataType()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof DataTypeProperty) {
            DataTypeProperty that = (DataTypeProperty) o;
            return (this.dataType.equals(that.dataType()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.dataType.hashCode();
        return h;
    }
}
