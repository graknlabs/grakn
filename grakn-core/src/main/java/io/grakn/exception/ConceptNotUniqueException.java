/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.exception;

import io.grakn.concept.Concept;
import io.grakn.concept.Instance;
import io.grakn.concept.Resource;
import io.grakn.util.ErrorMessage;
import io.grakn.util.Schema;

/**
 * This exception is thrown when two concepts attept to have the same unique id.
 */
public class ConceptNotUniqueException extends ConceptException {
    public ConceptNotUniqueException(Concept concept, Schema.ConceptProperty type, String id) {
        super(ErrorMessage.ID_NOT_UNIQUE.getMessage(concept.toString(), type.name(), id));
    }

    public ConceptNotUniqueException(Concept concept, String id){
        super(ErrorMessage.ID_ALREADY_TAKEN.getMessage(id, concept.toString()));
    }

    public ConceptNotUniqueException(Resource resource, Instance instance){
        super(ErrorMessage.RESOURCE_TYPE_UNIQUE.getMessage(resource.getId(), instance.getId()));
    }
}
