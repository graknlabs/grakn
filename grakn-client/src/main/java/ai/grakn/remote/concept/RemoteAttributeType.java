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

package ai.grakn.remote.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <D> The data type of this attribute type
 */
@AutoValue
abstract class RemoteAttributeType<D> extends RemoteType<AttributeType<D>, Attribute<D>> implements AttributeType<D> {

    public static <D> RemoteAttributeType<D> create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteAttributeType<>(tx, id);
    }

    @Override
    public final AttributeType<D> setRegex(@Nullable String regex) {
        if (regex == null) {
            String oldRegex = getRegex();
            if (oldRegex != null) {
                undefine(ME.regex(oldRegex));
            }
        } else {
            define(ME.regex(regex));
        }
        return asSelf(this);
    }

    @Override
    public final Attribute<D> putAttribute(D value) {
        return asInstance(insert(TARGET.val(value).isa(ME)));
    }

    @Nullable
    @Override
    public final Attribute<D> getAttribute(D value) {
        Stream<Concept> concepts = query(TARGET.val(value).isa(ME));
        return concepts.findAny().map(this::asInstance).orElse(null);
    }

    @Nullable
    @Override
    public final AttributeType.DataType<D> getDataType() {
        return (AttributeType.DataType<D>) runNullableMethod(ConceptMethod.GET_DATA_TYPE);
    }

    @Nullable
    @Override
    public final String getRegex() {
        return runNullableMethod(ConceptMethod.GET_REGEX);
    }

    @Override
    final AttributeType<D> asSelf(Concept concept) {
        return concept.asAttributeType();
    }

    @Override
    protected final Attribute<D> asInstance(Concept concept) {
        return concept.asAttribute();
    }

    @Nonnull
    @Override
    public AttributeType<D> sup() {
        return Objects.requireNonNull(super.sup());
    }
}
