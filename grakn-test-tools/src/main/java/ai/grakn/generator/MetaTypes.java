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

    @SuppressWarnings("unused") /** Used through {@link Meta} annotation*/
    public final void configure(@SuppressWarnings("unused") Meta meta) {
    }
}
