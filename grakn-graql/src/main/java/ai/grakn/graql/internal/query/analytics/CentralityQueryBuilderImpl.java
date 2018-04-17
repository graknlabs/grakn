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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.graql.analytics.CentralityQueryBuilder;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.DegreeQuery;

import java.util.Optional;

/**
 * This class implements CentralityQueryBuilder.
 *
 * @author Jason Liu
 */

public class CentralityQueryBuilderImpl implements CentralityQueryBuilder {

    private Optional<GraknTx> tx;

    CentralityQueryBuilderImpl(Optional<GraknTx> tx) {
        this.tx = tx;
    }

    @Override
    public CentralityQueryBuilder withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return this;
    }

    @Override
    public CorenessQuery usingKCore() {
        return new CorenessQueryImpl(tx);
    }

    @Override
    public DegreeQuery usingDegree() {
        return new DegreeQueryImpl(tx);
    }
}
