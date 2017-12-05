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

package ai.grakn.engine.controller.response;

import ai.grakn.concept.Label;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * <p>
 *     Wrapper class for a light representation of {@link ai.grakn.concept.Type} which is embedded in the
 *     {@link Thing} representation
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class EmbeddedType {

    @JsonProperty("@id")
    public abstract Link selfLink();

    @JsonProperty
    public abstract Label label();

    @JsonCreator
    public static EmbeddedType create(@JsonProperty("@id") Link selfLink, @JsonProperty("label") Label label){
        return new AutoValue_EmbeddedType(selfLink, label);
    }

    public static EmbeddedType create(ai.grakn.concept.Type type){
        return create(Link.create(type), type.getLabel());
    }
}
