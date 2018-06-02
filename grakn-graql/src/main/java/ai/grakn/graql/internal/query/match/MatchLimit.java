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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.kb.internal.EmbeddedGraknTx;

import java.util.stream.Stream;

/**
 * "Limit" modifier for {@link Match} that limits the results of a query.
 *
 * @author Grakn Warriors
 */
class MatchLimit extends MatchModifier {

    private final long limit;

    MatchLimit(AbstractMatch inner, long limit) {
        super(inner);
        if (limit <= 0) {
            throw GraqlQueryException.nonPositiveLimit(limit);
        }
        this.limit = limit;
    }

    @Override
    public Stream<Answer> stream(EmbeddedGraknTx<?> tx) {
        return inner.stream(tx).limit(limit);
    }

    @Override
    protected String modifierString() {
        return " limit " + limit + ";";
    }
}
