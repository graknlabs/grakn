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
 */

package ai.grakn.graql.internal.analytics;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class ClusterSizeMapReduce extends GraknMapReduce<Long> {

    private static final String CLUSTER_LABEL = "clusterMemberMapReduce.clusterLabel";

    public ClusterSizeMapReduce() {
    }

    public ClusterSizeMapReduce(Set<String> selectedTypes, String clusterLabel) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
            emitter.emit(vertex.value((String) persistentProperties.get(CLUSTER_LABEL)), 1L);
        }
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Long> values,
                       final ReduceEmitter<Serializable, Long> emitter) {
        emitter.emit(key, IteratorUtils.reduce(values, 0L,
                (a, b) -> a + b));
    }

    @Override
    public void combine(final Serializable key, final Iterator<Long> values,
                        final ReduceEmitter<Serializable, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Long> generateFinalResult(Iterator<KeyValue<Serializable, Long>> keyValues) {
        final Map<Serializable, Long> clusterPopulation = new HashMap<>();
        keyValues.forEachRemaining(pair -> clusterPopulation.put(pair.getKey(), pair.getValue()));
        return clusterPopulation;
    }
}
