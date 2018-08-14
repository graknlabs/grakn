/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ai.grakn.client.concept;

import ai.grakn.client.Grakn;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.rpc.proto.ConceptProto;
import com.google.auto.value.AutoValue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Attribute}
 *
 * @param <D> The data type of this attribute
 */
@AutoValue
public abstract class RemoteAttribute<D> extends RemoteThing<Attribute<D>, AttributeType<D>> implements Attribute<D> {

    static <D> RemoteAttribute<D> construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteAttribute<>(tx, id);
    }

    @Override
    public final D value() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeValueReq(ConceptProto.Attribute.Value.Req.getDefaultInstance()).build();

        ConceptProto.ValueObject value = runMethod(method).getAttributeValueRes().getValue();
        return castValue(value);
    }

    @SuppressWarnings("unchecked")
    private D castValue(ConceptProto.ValueObject value){
        switch (value.getValueCase()){
            case DATE: return (D) LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getDate()), ZoneId.of("Z"));
            case STRING: return (D) value.getString();
            case BOOLEAN: return (D) (Boolean) value.getBoolean();
            case INTEGER: return (D) (Integer) value.getInteger();
            case LONG: return (D) (Long) value.getLong();
            case FLOAT: return (D) (Float) value.getFloat();
            case DOUBLE: return (D) (Double) value.getDouble();
            case VALUE_NOT_SET: return null;
            default: throw new IllegalArgumentException("Unexpected value for attribute: " + value);
        }
    }

    @Override
    public final Stream<Thing> owners() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeOwnersReq(ConceptProto.Attribute.Owners.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getAttributeOwnersIter().getId();
        return conceptStream(iteratorId, res -> res.getAttributeOwnersIterRes().getThing()).map(Concept::asThing);
    }

    @Override
    public final AttributeType.DataType<D> dataType() {
        return type().dataType();
    }

    @Override
    final AttributeType<D> asCurrentType(Concept concept) {
        return concept.asAttributeType();
    }

    @Override
    final Attribute<D> asCurrentBaseType(Concept other) {
        return other.asAttribute();
    }
}
