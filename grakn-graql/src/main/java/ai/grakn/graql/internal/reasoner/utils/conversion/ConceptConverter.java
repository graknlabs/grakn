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

package ai.grakn.graql.internal.reasoner.utils.conversion;

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.CommonUtil;

/**
 * <p>
 * Interface for conversion from {@link Concept}s.
 * </p>
 * @param <T> {@link Concept} type to convert from
 *
 * @author Kasper Piskorski
 */
public interface ConceptConverter<T extends Concept> {

    static Pattern toPattern(Concept concept) {
        if (!concept.isThing()){
            throw GraqlQueryException.conceptNotAThing(concept);
        }
        if (concept.isEntity()) {
            return new EntityConverter().pattern(concept.asEntity());
        } else if (concept.isRelationship()) {
            return new RelationshipConverter().pattern(concept.asRelationship());
        } else if (concept.isAttribute()) {
            return new AttributeConverter().pattern(concept.asAttribute());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

}
