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

package ai.grakn.exception;

import static ai.grakn.util.ErrorMessage.LOCKING_EXCEPTION;

/**
 * <p>
 *     Graph Mutation Exception
 * </p>
 *
 * <p>
 *     This exception occurs when we are temporarily unable to write to the graph.
 *     This is typically caused by the persistence layer being overloaded.
 *     When this occurs the transaction should be retried
 * </p>
 *
 * @author fppt
 */
public class TemporaryWriteException extends GraknBackendException{
    private TemporaryWriteException(String error, Exception e) {
        super(error, e);
    }

    /**
     * Thrown when the persistence layer is locked temporarily.
     * Retrying the transaction is reccomended.
     */
    public static TemporaryWriteException temporaryLock(Exception e){
        return new TemporaryWriteException(LOCKING_EXCEPTION.getMessage(), e);
    }
}
