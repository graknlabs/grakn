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
 *
 */

package ai.grakn.generator;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;

/**
 * A generator that produces {@link AttributeType}s
 *
 * @author Felix Chapman
 */
public class ResourceTypes extends AbstractTypeGenerator<AttributeType> {

    public ResourceTypes() {
        super(AttributeType.class);
    }

    @Override
    protected AttributeType newOntologyConcept(Label label) {
        AttributeType.DataType<?> dataType = gen(AttributeType.DataType.class);

        return graph().putResourceType(label, dataType);
    }

    @Override
    protected AttributeType metaOntologyConcept() {
        return graph().admin().getMetaResourceType();
    }
}
