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

package ai.grakn.generator;

import ai.grakn.concept.AttributeType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Generator that generates random valid resource values.
 *
 * @author Felix Chapman
 */
public class ResourceValues extends AbstractGenerator<Object> {

    private AttributeType.DataType<?> dataType = null;

    public ResourceValues() {
        super(Object.class);
    }

    @Override
    public Object generate() {
        String className;
        if (dataType == null) {
            className = random.choose(AttributeType.DataType.SUPPORTED_TYPES.keySet());
        } else {
            className = dataType.getName();
        }

        Class<?> clazz;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unrecognised class " + className);
        }

        if(clazz.equals(LocalDateTime.class)){
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(random.nextLong()), ZoneId.systemDefault());
        }

        return gen(clazz);
    }

    ResourceValues dataType(AttributeType.DataType<?> dataType) {
        this.dataType = dataType;
        return this;
    }

}
