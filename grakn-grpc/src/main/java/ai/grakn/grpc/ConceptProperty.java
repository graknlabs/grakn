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

package ai.grakn.grpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.ConceptPropertyValue;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.DataTypeProperty;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.IsAbstract;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.IsImplicit;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.IsInferred;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.LabelProperty;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.Regex;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.Then;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.ValueProperty;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.When;

/**
 * Wrapper for describing {@link Concept} properties that can be communicated over gRPC.
 *
 *
 * @author Felix Chapman
 *
 * @param <T> The type of the concept property value.
 */
public abstract class ConceptProperty<T> {

    public static final ConceptProperty<Object> VALUE = create(
            ValueProperty,
            val -> val.asAttribute().getValue(),
            val -> GrpcUtil.convert(val.getAttributeValue()),
            (builder, val) -> builder.setAttributeValue(GrpcUtil.convertValue(val))
    );

    public static final ConceptProperty<AttributeType.DataType<?>> DATA_TYPE = create(
            DataTypeProperty,
            val -> val.isAttribute() ? val.asAttribute().dataType() : val.asAttributeType().getDataType(),
            val -> GrpcUtil.convert(val.getDataType()),
            (builder, val) -> builder.setDataType(GrpcUtil.convert(val))
    );

    public static final ConceptProperty<Label> LABEL = create(
            LabelProperty,
            concept -> concept.asSchemaConcept().getLabel(),
            val -> GrpcUtil.convert(val.getLabel()),
            (builder, val) -> builder.setLabel(GrpcUtil.convert(val))
    );

    public static final ConceptProperty<Boolean> IS_IMPLICIT = create(
            IsImplicit,
            concept -> concept.asSchemaConcept().isImplicit(),
            ConceptPropertyValue::getIsImplicit,
            ConceptPropertyValue.Builder::setIsImplicit
    );

    public static final ConceptProperty<Boolean> IS_INFERRED = create(
            IsInferred,
            concept -> concept.asThing().isInferred(),
            ConceptPropertyValue::getIsInferred,
            ConceptPropertyValue.Builder::setIsInferred
    );

    public static final ConceptProperty<Boolean> IS_ABSTRACT = create(
            IsAbstract,
            concept -> concept.asType().isAbstract(),
            ConceptPropertyValue::getIsInferred,
            ConceptPropertyValue.Builder::setIsInferred
    );

    public static final ConceptProperty<Pattern> WHEN = create(
            When,
            concept -> concept.asRule().getWhen(),
            val -> GrpcUtil.convert(val.getWhen()),
            (builder, val) -> builder.setWhen(GrpcUtil.convert(val))
    );

    public static final ConceptProperty<Pattern> THEN = create(
            Then,
            concept -> concept.asRule().getThen(),
            val -> GrpcUtil.convert(val.getThen()),
            (builder, val) -> builder.setThen(GrpcUtil.convert(val))
    );

    public static final ConceptProperty<String> REGEX = create(
            Regex,
            concept -> concept.asAttributeType().getRegex(),
            ConceptPropertyValue::getRegex,
            ConceptPropertyValue.Builder::setRegex
    );

    public static ConceptProperty<?> fromGrpc(GraknOuterClass.ConceptProperty conceptProperty) {
        switch (conceptProperty) {
            case ValueProperty:
                return VALUE;
            case DataTypeProperty:
                return DATA_TYPE;
            case LabelProperty:
                return LABEL;
            case IsImplicit:
                return IS_IMPLICIT;
            case IsInferred:
                return IS_INFERRED;
            case IsAbstract:
                return IS_ABSTRACT;
            case When:
                return WHEN;
            case Then:
                return THEN;
            case Regex:
                return REGEX;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + conceptProperty);
        }
    }

    public final TxResponse createTxResponse(@Nullable T value) {
        ConceptPropertyValue.Builder conceptPropertyValue = ConceptPropertyValue.newBuilder();
        set(conceptPropertyValue, value);
        return TxResponse.newBuilder().setConceptPropertyValue(conceptPropertyValue.build()).build();
    }

    public abstract TxResponse createTxResponse(Concept concept);

    @Nullable
    public abstract T get(TxResponse value);

    abstract void set(ConceptPropertyValue.Builder builder, @Nullable T value);

    abstract GraknOuterClass.ConceptProperty toGrpc();

    /**
     * @param grpcProperty the gRPC {@link GraknOuterClass.ConceptProperty} equivalent of this with the same name
     * @param conceptGetter a method to retrieve a property value from a {@link Concept}
     * @param responseGetter a method to retrieve a property value from inside a gRPC response
     * @param setter a method to set the property value inside a gRPC response
     * @param <T> The type of values of the property
     */
    private static <T> ConceptProperty<T> create(
            GraknOuterClass.ConceptProperty grpcProperty,
            Function<Concept, T> conceptGetter,
            Function<ConceptPropertyValue, T> responseGetter,
            BiConsumer<ConceptPropertyValue.Builder, T> setter
    ) {
        return new ConceptProperty<T>() {
            @Override
            public TxResponse createTxResponse(Concept concept) {
                return createTxResponse(conceptGetter.apply(concept));
            }

            @Nullable
            @Override
            public T get(TxResponse txResponse) {
                ConceptPropertyValue conceptPropertyValue = txResponse.getConceptPropertyValue();
                if (conceptPropertyValue.getValueCase().equals(ConceptPropertyValue.ValueCase.VALUE_NOT_SET)) {
                    return null;
                } else {
                    return responseGetter.apply(conceptPropertyValue);
                }
            }

            @Override
            void set(ConceptPropertyValue.Builder builder, @Nullable T value) {
                if (value != null) {
                    setter.accept(builder, value);
                }
            }

            @Override
            GraknOuterClass.ConceptProperty toGrpc() {
                return grpcProperty;
            }
        };
    }
}
