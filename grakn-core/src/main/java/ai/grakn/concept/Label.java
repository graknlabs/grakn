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

package ai.grakn.concept;

import ai.grakn.GraknTx;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;
import java.util.function.Function;

/**
 * <p>
 *     A Label
 * </p>
 *
 * <p>
 *     A class which represents the unique label of any {@link SchemaConcept} in the {@link GraknTx}.
 *     Also contains a static method for producing {@link Label}s from Strings.
 * </p>
 *
 * @author fppt
 */
@AutoValue
public abstract class Label implements Comparable<Label>, Serializable {
    private static final long serialVersionUID = 2051578406740868932L;

    @JsonValue
    public abstract String getValue();

    /**
     * Rename a {@link Label} (does not modify the original {@link Label})
     * @param mapper a function to apply to the underlying type label
     * @return the new type label
     */
    @CheckReturnValue
    public Label map(Function<String, String> mapper) {
        return Label.of(mapper.apply(getValue()));
    }

    @Override
    public int compareTo(Label o) {
        return getValue().compareTo(o.getValue());
    }

    /**
     *
     * @param value The string which potentially represents a Type
     * @return The matching Type Label
     */
    @CheckReturnValue
    @JsonCreator
    public static Label of(String value){
        return new AutoValue_Label(value);
    }

    @Override
    public final String toString() {
        // TODO: Consider using @AutoValue toString
        return getValue();
    }
}
