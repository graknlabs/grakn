/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class GraqlErrorListener extends BaseErrorListener {

    private final String[] query;
    private final List<SyntaxError> errors = new ArrayList<>();

    GraqlErrorListener(String query) {
        this.query = query.split("\n");
    }

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg,
            RecognitionException e) {
            errors.add(new SyntaxError(query[line-1], line, charPositionInLine, msg));
    }

    boolean hasErrors() {
        return !errors.isEmpty();
    }

    @Override
    public String toString() {
        return errors.stream().map(SyntaxError::toString).collect(Collectors.joining("\n"));
    }
}

