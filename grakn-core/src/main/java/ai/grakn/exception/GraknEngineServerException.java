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

import ai.grakn.util.ErrorMessage;

/**
 * This Exception is thrown by Grakn Engine web server when operations accessible through APIs go wrong.
 *
 * @author Marco Scoppetta, alexandraorth
 */
public class GraknEngineServerException extends RuntimeException {

    private final int status;

    public GraknEngineServerException(int status, ErrorMessage message, String... args) {
        super(message.getMessage(args));
        this.status = status;
    }

    //TODO remove this method
    public GraknEngineServerException(int status, String message) {
        super(message);
        this.status = status;
    }

    public GraknEngineServerException(int status, Exception e) {
        super(e.toString());
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }
}
