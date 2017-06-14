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

import static ai.grakn.util.ErrorMessage.AUTHENTICATION_FAILURE;
import static ai.grakn.util.ErrorMessage.ENGINE_ERROR;
import static ai.grakn.util.ErrorMessage.EXPLAIN_ONLY_MATCH;
import static ai.grakn.util.ErrorMessage.INVALID_CONTENT_TYPE;
import static ai.grakn.util.ErrorMessage.INVALID_QUERY_USAGE;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_BODY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.ErrorMessage.UNAVAILABLE_TASK_CLASS;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;

import ai.grakn.graql.Query;

/**
 * <p>
 *     Grakn Server Exception
 * </p>
 *
 * <p>
 *     Wraps backend exception which require a status code which needs to be returned to the client.
 * </p>
 *
 * @author fppt
 */
public class GraknServerException extends GraknBackendException {
    private final int status;

    private GraknServerException(String error, Exception e, int status) {
        super(error, e);
        this.status = status;
    }

    private GraknServerException(String error, int status){
        super(error);
        this.status = status;
    }

    /**
     * Gets the error status code if one is available
     */
    public int getStatus(){
        return status;
    }

    /**
     * Thrown when the Grakn server has an internal exception.
     * This is thrown upwards to be interpreted by clients
     */
    public static GraknServerException serverException(int status, Exception e){
        return new GraknServerException(ENGINE_ERROR.getMessage(), e, status);
    }

    /**
     * Thrown when attempting to create an invalid task
     */
    public static GraknServerException invalidTask(String className){
        return new GraknServerException(UNAVAILABLE_TASK_CLASS.getMessage(className), 400);
    }

    /**
     * Thrown when a request is missing mandatory parameters
     */
    public static GraknServerException requestMissingParameters(String parameter){
        return new GraknServerException(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(parameter), 400);
    }

    /**
     * Thrown when a request is missing mandatory parameters in the body
     */
    public static GraknServerException requestMissingBodyParameters(String parameter){
        return new GraknServerException(MISSING_MANDATORY_BODY_REQUEST_PARAMETERS.getMessage(parameter), 400);
    }

    /**
     * Thrown when a request is missing the body
     */
    public static GraknServerException requestMissingBody(){
        return new GraknServerException(MISSING_REQUEST_BODY.getMessage(), 400);
    }

    /**
     * Thrown the content type specified in a request is invalid
     */
    public static GraknServerException unsupportedContentType(String contentType){
        return new GraknServerException(UNSUPPORTED_CONTENT_TYPE.getMessage(contentType), 406);
    }

    /**
     * Thrown when there is a mismatch between the content type in the request and the query to be executed
     */
    public static GraknServerException contentTypeQueryMismatch(String contentType, Query query){
        return new GraknServerException(INVALID_CONTENT_TYPE.getMessage(query.getClass().getName(), contentType), 406);
    }

    /**
     * Thrown when an incorrect query is used with a REST endpoint
     */
    public static GraknServerException invalidQuery(String queryType){
        return new GraknServerException(INVALID_QUERY_USAGE.getMessage(queryType), 405);
    }

    /**
     * Thrown when asked to explain a non-match query
     */
    public static GraknServerException invalidQueryExplaination(String query){
        return new GraknServerException(EXPLAIN_ONLY_MATCH.getMessage(query), 405);
    }

    /**
     * Thrown when an incorrect query is used with a REST endpoint
     */
    public static GraknServerException authenticationFailure(){
        return new GraknServerException(AUTHENTICATION_FAILURE.getMessage(), 401);
    }

    /**
     * Thrown when an internal server error occurs. This is likely due to incorrect configs
     */
    public static GraknServerException internalError(String errorMessage){
        return new GraknServerException(errorMessage, 500);
    }
}
