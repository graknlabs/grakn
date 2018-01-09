/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.generator;

import ai.grakn.concept.Type;
import ai.grakn.generator.AbstractSchemaConceptGenerator.Meta;

/**
 * This is a generator that just produces the top-level meta-type `thing`.
 *
 * Other meta types are still handled from their respective generators, e.g. `EntityTypes`
 *
 * @author Felix Chapman
 */
public class MetaTypes extends FromTxGenerator<Type> {

    public MetaTypes() {
        super(Type.class);
    }

    @Override
    protected Type generateFromTx() {
        return tx().admin().getMetaConcept();
    }

    public final void configure(Meta meta) {
    }
}
