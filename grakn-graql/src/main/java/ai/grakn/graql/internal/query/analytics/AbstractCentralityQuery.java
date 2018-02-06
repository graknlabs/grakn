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

import ai.grakn.API;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.Sets;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

abstract class AbstractCentralityQuery<V extends ComputeQuery<Map<Long, Set<String>>>>
        extends AbstractComputeQuery<Map<Long, Set<String>>, V> {

    /**
     * The centrality measures supported.
     */
    enum CentralityMeasure {

        DEGREE("degree"),
        K_CORE("k-core");
        private final String name;

        CentralityMeasure(String name) {
            this.name = name;
        }

        @CheckReturnValue
        public String getName() {
            return name;
        }
    }

    private boolean ofTypeLabelsSet = false;
    Set<Label> ofLabels = new HashSet<>();

    void initSubGraph() { //TODO: REMOVE THIS METHOD
        includeAttribute = true;
    }

    @API
    public V of(String... ofTypeLabels) {
        return of(Arrays.stream(ofTypeLabels).map(Label::of).collect(Collectors.toSet()));
    }

    public V of(Collection<Label> ofLabels) {
        if (!ofLabels.isEmpty()) {
            ofTypeLabelsSet = true;
            this.ofLabels = Sets.newHashSet(ofLabels);
        }
        return (V) this;
    }

    abstract CentralityMeasure getMethod();

    @Override
    String graqlString() {
        String string = "centrality";
        if (ofTypeLabelsSet) {
            string += " of " + ofLabels.stream()
                    .map(StringConverter::typeLabelToString)
                    .collect(joining(", "));
        }
        string += subtypeString() + " using " + getMethod().getName();

        return string;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractCentralityQuery that = (AbstractCentralityQuery) o;

        return ofTypeLabelsSet == that.ofTypeLabelsSet && ofLabels.equals(that.ofLabels) &&
                getMethod() == that.getMethod();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (ofTypeLabelsSet ? 1 : 0);
        result = 31 * result + ofLabels.hashCode();
        result = 31 * result + getMethod().hashCode();
        return result;
    }
}
