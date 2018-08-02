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
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.util.CommonUtil;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * Client implementation of {@link ai.grakn.concept.AttributeType}
 *
 * @param <D> The data type of this attribute type
 */
@AutoValue
public abstract class RemoteAttributeType<D> extends RemoteType<AttributeType<D>, Attribute<D>> implements AttributeType<D> {

    static <D> RemoteAttributeType<D> construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteAttributeType<>(tx, id);
    }

    @Override
    public final Attribute<D> create(D value) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeCreateReq(ConceptProto.AttributeType.Create.Req.newBuilder()
                        .setValue(RequestBuilder.Concept.attributeValue(value))).build();

        Concept concept = RemoteConcept.of(runMethod(method).getAttributeTypeCreateRes().getAttribute(), tx());
        return asInstance(concept);
    }

    @Nullable
    @Override
    public final Attribute<D> attribute(D value) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeAttributeReq(ConceptProto.AttributeType.Attribute.Req.newBuilder()
                        .setValue(RequestBuilder.Concept.attributeValue(value))).build();

        ConceptProto.AttributeType.Attribute.Res response = runMethod(method).getAttributeTypeAttributeRes();
        switch (response.getResCase()) {
            case NULL:
                return null;
            case ATTRIBUTE:
                return RemoteConcept.of(response.getAttribute(), tx()).asAttribute();
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final AttributeType.DataType<D> dataType() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeDataTypeReq(ConceptProto.AttributeType.DataType.Req.getDefaultInstance()).build();

        ConceptProto.AttributeType.DataType.Res response = runMethod(method).getAttributeTypeDataTypeRes();
        switch (response.getResCase()) {
            case NULL:
                return null;
            case DATATYPE:
                return (AttributeType.DataType<D>) RequestBuilder.Concept.dataType(response.getDataType());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final String regex() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeGetRegexReq(ConceptProto.AttributeType.GetRegex.Req.getDefaultInstance()).build();

        String regex = runMethod(method).getAttributeTypeGetRegexRes().getRegex();
        return regex.isEmpty() ? null : regex;
    }

    @Override
    public final AttributeType<D> regex(String regex) {
        if (regex == null) regex = "";
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeSetRegexReq(ConceptProto.AttributeType.SetRegex.Req.newBuilder()
                        .setRegex(regex)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    final AttributeType<D> asCurrentBaseType(Concept other) {
        return other.asAttributeType();
    }

    @Override
    final boolean equalsCurrentBaseType(Concept other) {
        return other.isAttributeType();
    }

    @Override
    protected final Attribute<D> asInstance(Concept concept) {
        return concept.asAttribute();
    }
}
