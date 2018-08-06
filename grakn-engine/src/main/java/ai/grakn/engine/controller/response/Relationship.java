/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.engine.controller.response;

import ai.grakn.concept.ConceptId;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Relationship}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Relationship extends Thing {

    @JsonProperty
    public abstract Set<RolePlayer> roleplayers();

    @JsonCreator
    public static Relationship create(
            @JsonProperty("id") ConceptId id,
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("type") EmbeddedSchemaConcept type,
            @JsonProperty("attributes") Link attributes,
            @JsonProperty("keys") Link keys,
            @JsonProperty("relationships") Link relationships,
            @JsonProperty("inferred") boolean inferred,
            @Nullable @JsonProperty("explanation-query")  String explanation,
            @JsonProperty("roleplayers") Set<RolePlayer> roleplayers){
        return new AutoValue_Relationship(Schema.BaseType.RELATIONSHIP.name(), id, selfLink, type, attributes, keys, relationships, inferred, explanation, roleplayers);
    }
}
