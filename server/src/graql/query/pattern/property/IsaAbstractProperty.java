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

import grakn.core.graql.query.pattern.Statement;

import java.util.stream.Stream;

public abstract class IsaAbstractProperty extends VarProperty {

    public abstract Statement type();

    @Override
    public final String getProperty() {
        return type().getPrintableName();
    }

    @Override
    public final boolean isUnique() {
        return true;
    }

    @Override
    public final Stream<Statement> getTypes() {
        return Stream.of(type());
    }

    @Override
    public final Stream<Statement> innerStatements() {
        return Stream.of(type());
    }
}
