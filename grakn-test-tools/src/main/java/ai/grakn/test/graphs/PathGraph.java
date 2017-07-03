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

package ai.grakn.test.graphs;

import ai.grakn.GraknGraph;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class PathGraph extends AbstractPathGraph {
    private final static String gqlFile = "path-test.gql";

    public PathGraph(int n, int m){
        super(gqlFile, n, m);
    }

    public static Consumer<GraknGraph> get(int n, int children) {
        return new PathGraph(n, children).build();
    }
}
