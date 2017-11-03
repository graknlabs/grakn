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

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;

/**
 * Generator that produces {@link EntityType}s
 *
 * @author Felix Chapman
 */
public class EntityTypes extends AbstractTypeGenerator<EntityType> {

    public EntityTypes() {
        super(EntityType.class);
    }

    @Override
    protected EntityType newSchemaConcept(Label label) {
        return tx().putEntityType(label);
    }

    @Override
    protected EntityType metaSchemaConcept() {
        return tx().admin().getMetaEntityType();
    }
}
