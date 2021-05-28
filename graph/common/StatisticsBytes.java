/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.graph.common;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;


public class StatisticsBytes {
    public static ByteArray vertexCountKey(VertexIID.Type typeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                typeIID.bytes(),
                Encoding.Statistics.Infix.VERTEX_COUNT.bytes());
    }

    public static ByteArray vertexTransitiveCountKey(VertexIID.Type typeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                typeIID.bytes(),
                Encoding.Statistics.Infix.VERTEX_TRANSITIVE_COUNT.bytes());
    }

    public static ByteArray hasEdgeCountKey(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                thingTypeIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_COUNT.bytes(),
                attTypeIID.bytes());
    }

    public static ByteArray hasEdgeTotalCountKey(VertexIID.Type thingTypeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                thingTypeIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_TOTAL_COUNT.bytes());
    }

    public static ByteArray countJobKey() {
        return join(
                Encoding.Prefix.STATISTICS_COUNT_JOB.bytes());
    }

    public static ByteArray attributeCountJobKey(VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
                Encoding.Statistics.JobType.ATTRIBUTE_VERTEX.bytes(),
                attIID.bytes());
    }

    public static ByteArray attributeCountedKey(VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNTED.bytes(),
                attIID.bytes());
    }

    public static ByteArray hasEdgeCountJobKey(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
                Encoding.Statistics.JobType.HAS_EDGE.bytes(),
                thingIID.bytes(),
                attIID.bytes()
        );
    }

    public static ByteArray hasEdgeCountedKey(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNTED.bytes(),
                thingIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_COUNT.bytes(),
                attIID.bytes()
        );
    }

    public static ByteArray snapshotKey() {
        return Encoding.Prefix.STATISTICS_SNAPSHOT.bytes();
    }
}
