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

package ai.grakn.concept;

import java.io.Serializable;

/**
 * <p>
 *     A Concept Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link Concept} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing concept IDs from Strings.
 * </p>
 *
 * @author fppt
 */
public class ConceptId implements Comparable<ConceptId>, Serializable {
    private Object conceptId;

    private ConceptId(Object conceptId){
        this.conceptId = conceptId;
    }

    public Object getValue(){
        return conceptId;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof ConceptId && ((ConceptId) object).getValue().equals(conceptId);
    }

    @Override
    public int compareTo(ConceptId o) {
        if(o.getValue() instanceof Comparable){
            //Most vendor ids implement comparable so this should work most of the time.
            //noinspection unchecked
            return ((Comparable) o.getValue()).compareTo(getValue());
        } else {
            //When vendor Id's are not comparable we fallback to toString  comparison
            return getValue().toString().compareTo(o.getValue().toString());
        }
    }

    @Override
    public String toString(){
        return conceptId.toString();
    }

    @Override
    public int hashCode(){
        return conceptId.hashCode();
    }

    /**
     *
     * @param value The string which potentially represents a Concept
     * @return The matching concept ID
     */
    public static ConceptId of(Object value){
        return new ConceptId(value);
    }
}
