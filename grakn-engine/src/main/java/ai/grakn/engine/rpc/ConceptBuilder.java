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

package ai.grakn.engine.rpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.ValueProto;
import ai.grakn.util.CommonUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A utility class to build RPC Concepts by converting them from Grakn Concepts.
 *
 * @author Grakn Warriors
 */
public class ConceptBuilder {

    public static Concept concept(ValueProto.Concept ValueProto, EmbeddedGraknTx tx) {
        return tx.getConcept(ConceptId.of(ValueProto.getId()));
    }

    public static ValueProto.Concept concept(Concept concept) {
        return ValueProto.Concept.newBuilder()
                .setId(concept.getId().getValue())
                .setBaseType(getBaseType(concept))
                .build();
    }

    private static ValueProto.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return ValueProto.BaseType.ENTITY_TYPE;
        } else if (concept.isRelationshipType()) {
            return ValueProto.BaseType.RELATIONSHIP_TYPE;
        } else if (concept.isAttributeType()) {
            return ValueProto.BaseType.ATTRIBUTE_TYPE;
        } else if (concept.isEntity()) {
            return ValueProto.BaseType.ENTITY;
        } else if (concept.isRelationship()) {
            return ValueProto.BaseType.RELATIONSHIP;
        } else if (concept.isAttribute()) {
            return ValueProto.BaseType.ATTRIBUTE;
        } else if (concept.isRole()) {
            return ValueProto.BaseType.ROLE;
        } else if (concept.isRule()) {
            return ValueProto.BaseType.RULE;
        } else if (concept.isType()) {
            return ValueProto.BaseType.META_TYPE;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

    static ValueProto.DataType dataType(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return ValueProto.DataType.String;
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return ValueProto.DataType.Boolean;
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return ValueProto.DataType.Integer;
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return ValueProto.DataType.Long;
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return ValueProto.DataType.Float;
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return ValueProto.DataType.Double;
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return ValueProto.DataType.Date;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
        }
    }

    static ValueProto.AttributeValue attributeValue(Object value) {
        ValueProto.AttributeValue.Builder builder = ValueProto.AttributeValue.newBuilder();
        if (value instanceof String) {
            builder.setString((String) value);
        } else if (value instanceof Boolean) {
            builder.setBoolean((boolean) value);
        } else if (value instanceof Integer) {
            builder.setInteger((int) value);
        } else if (value instanceof Long) {
            builder.setLong((long) value);
        } else if (value instanceof Float) {
            builder.setFloat((float) value);
        } else if (value instanceof Double) {
            builder.setDouble((double) value);
        } else if (value instanceof LocalDateTime) {
            builder.setDate(((LocalDateTime) value).atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + value);
        }

        return builder.build();
    }

    static ValueProto.Answer answer(Object object) {
        ValueProto.Answer answer;

        if (object instanceof Answer) {
            answer = ValueProto.Answer.newBuilder().setQueryAnswer(ConceptBuilder.queryAnswer((Answer) object)).build();
        } else if (object instanceof ComputeQuery.Answer) {
            answer = ValueProto.Answer.newBuilder().setComputeAnswer(ConceptBuilder.computeAnswer((ComputeQuery.Answer) object)).build();
        } else {
            // If not an QueryAnswer or ComputeAnswer, convert to JSON
            answer = ValueProto.Answer.newBuilder().setOtherResult(Printer.jsonPrinter().toString(object)).build();
        }

        return answer;
    }

    static ValueProto.QueryAnswer queryAnswer(Answer answer) {
        ValueProto.QueryAnswer.Builder queryAnswerRPC = ValueProto.QueryAnswer.newBuilder();
        answer.forEach((var, concept) -> {
            ValueProto.Concept conceptRps = concept(concept);
            queryAnswerRPC.putQueryAnswer(var.getValue(), conceptRps);
        });

        return queryAnswerRPC.build();
    }

    static ValueProto.ComputeAnswer computeAnswer(ComputeQuery.Answer computeAnswer) {
        ValueProto.ComputeAnswer.Builder computeAnswerRPC = ValueProto.ComputeAnswer.newBuilder();

        if (computeAnswer.getNumber().isPresent()) {
            computeAnswerRPC.setNumber(computeAnswer.getNumber().get().toString());
        }
        else if (computeAnswer.getPaths().isPresent()) {
             computeAnswerRPC.setPaths(paths(computeAnswer.getPaths().get()));
        }
        else if (computeAnswer.getCentrality().isPresent()) {
            computeAnswerRPC.setCentrality(centralityCounts(computeAnswer.getCentrality().get()));
        }
        else if (computeAnswer.getClusters().isPresent()) {
            computeAnswerRPC.setClusters(clusters(computeAnswer.getClusters().get()));
        }
        else if (computeAnswer.getClusterSizes().isPresent()) {
            computeAnswerRPC.setClusterSizes(clusterSizes(computeAnswer.getClusterSizes().get()));
        }

        return computeAnswerRPC.build();
    }

    private static ValueProto.Paths paths(List<List<ConceptId>> paths) {
        ValueProto.Paths.Builder pathsRPC = ValueProto.Paths.newBuilder();
        for (List<ConceptId> path : paths) pathsRPC.addPaths(conceptIds(path));

        return pathsRPC.build();
    }

    private static ValueProto.Centrality centralityCounts(Map<Long, Set<ConceptId>> centralityCounts) {
        ValueProto.Centrality.Builder centralityCountsRPC = ValueProto.Centrality.newBuilder();

        for (Map.Entry<Long, Set<ConceptId>> centralityCount : centralityCounts.entrySet()) {
            centralityCountsRPC.putCentrality(centralityCount.getKey(), conceptIds(centralityCount.getValue()));
        }

        return centralityCountsRPC.build();
    }

    private static ValueProto.ClusterSizes clusterSizes(Collection<Long> clusterSizes) {
        ValueProto.ClusterSizes.Builder clusterSizesRPC = ValueProto.ClusterSizes.newBuilder();
        clusterSizesRPC.addAllClusterSizes(clusterSizes);

        return clusterSizesRPC.build();
    }

    private static ValueProto.Clusters clusters(Collection<? extends Collection<ConceptId>> clusters) {
        ValueProto.Clusters.Builder clustersRPC = ValueProto.Clusters.newBuilder();
        for(Collection<ConceptId> cluster : clusters) clustersRPC.addClusters(conceptIds(cluster));

        return clustersRPC.build();
    }

    private static ValueProto.ConceptIds conceptIds(Collection<ConceptId> conceptIds) {
        ValueProto.ConceptIds.Builder conceptIdsRPC = ValueProto.ConceptIds.newBuilder();
        conceptIdsRPC.addAllIds(conceptIds.stream()
                .map(id -> id.getValue())
                .collect(Collectors.toList()));

        return conceptIdsRPC.build();
    }
}
