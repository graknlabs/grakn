/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.graql.internal.pattern.property;

import io.grakn.GraknGraph;
import io.grakn.concept.Concept;
import io.grakn.graql.admin.VarAdmin;
import io.grakn.graql.admin.VarProperty;
import io.grakn.graql.internal.query.InsertQueryExecutor;
import io.grakn.graql.internal.util.CommonUtil;
import io.grakn.util.ErrorMessage;

import java.util.stream.Stream;

abstract class AbstractVarProperty implements VarPropertyInternal {

    @Override
    public final void checkValid(GraknGraph graph, VarAdmin var) throws IllegalStateException {
        checkValidProperty(graph, var);

        getTypes().map(VarAdmin::getId).flatMap(CommonUtil::optionalToStream).forEach(typeId -> {
            if (graph.getConcept(typeId) == null) {
                throw new IllegalStateException(ErrorMessage.ID_NOT_FOUND.getMessage(typeId));
            }
        });
    }

    void checkValidProperty(GraknGraph graph, VarAdmin var) {

    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        throw failDelete(this);
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.empty();
    }

    @Override
    public Stream<VarAdmin> getImplicitInnerVars() {
        return getInnerVars();
    }

    static IllegalStateException failDelete(VarProperty property) {
        StringBuilder builder = new StringBuilder();
        property.buildString(builder);
        return new IllegalStateException(ErrorMessage.DELETE_UNSUPPORTED_PROPERTY.getMessage(builder.toString()));
    }
}
