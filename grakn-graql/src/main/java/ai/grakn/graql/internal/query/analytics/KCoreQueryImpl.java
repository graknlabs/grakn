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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.graql.analytics.KCoreQuery;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

class KCoreQueryImpl extends AbstractComputeQuery<Map<String, Set<String>>, KCoreQuery> implements KCoreQuery {

    private long k = -1L;

    private static final boolean INCLUDE_ATTRIBUTE = true; // TODO: REMOVE THIS LINE

    KCoreQueryImpl(Optional<GraknTx> tx) {
        super(tx, INCLUDE_ATTRIBUTE);
    }

    @Override
    public final ComputeJob<Map<String, Set<String>>> createJob() {
        return queryRunner().run(this);
    }

    @Override
    public final KCoreQuery kValue(long kValue) {
        k = kValue;
        return this;
    }

    @Override
    public final long kValue() {
        return k;
    }

    @Override
    String graqlString() {
        String string = "kcore ";
        string += k;
        string += subtypeString();
        return string;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        KCoreQueryImpl that = (KCoreQueryImpl) o;

        return k == that.k;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(k);
        return result;
    }
}
