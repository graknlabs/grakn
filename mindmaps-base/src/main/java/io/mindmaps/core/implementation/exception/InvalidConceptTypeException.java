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

package io.mindmaps.core.implementation.exception;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.exception.ConceptException;
import io.mindmaps.core.model.Concept;

/**
 * This exception is thrown when attempting to incorrectly cast a concept to something it is not.
 * For example when
 */
public class InvalidConceptTypeException extends ConceptException {
    public InvalidConceptTypeException(Concept c, Class type) {
        super(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(c.toString(), type.getName()));
    }
}
