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

package ai.grakn.graql.internal.shell;

import ai.grakn.graql.GraqlShell;
import ai.grakn.util.REST;
import ai.grakn.graql.GraqlShell;
import jline.console.completer.Completer;
import mjson.Json;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static ai.grakn.util.REST.RemoteShell.AUTOCOMPLETE_CANDIDATES;
import static ai.grakn.util.REST.RemoteShell.AUTOCOMPLETE_CURSOR;

/**
 * An autocompleter for Graql.
 * Provides a default 'complete' method that will filter results to only those that pass the Graql lexer
 */
public class GraQLCompleter implements Completer {

    private final GraqlShell shell;

    public GraQLCompleter(GraqlShell shell) {
        this.shell = shell;
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Json json = null;

        try {
            json = shell.getAutocompleteCandidates(buffer, cursor);
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }

        json.at(REST.RemoteShell.AUTOCOMPLETE_CANDIDATES).asJsonList().forEach(candidate -> candidates.add(candidate.asString()));
        return json.at(REST.RemoteShell.AUTOCOMPLETE_CURSOR).asInteger();
    }
}
