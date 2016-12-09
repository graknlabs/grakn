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

package ai.grakn;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

/**
 * GraknComputer encapsulates a Tinkerpop graph computer and provides methods to execute OLAP tasks.
 */
public interface GraknComputer {

    /**
     * Execute the given vertex program using a graph computer.
     *
     * @param program   the vertex program
     * @param mapReduce a list of mapReduce job
     * @return          the result of the computation
     * @see ComputerResult
     */
    ComputerResult compute(VertexProgram program, MapReduce... mapReduce);

    /**
     * Execute the given map reduce job using a graph computer.
     *
     * @param mapReduce the map reduce job
     * @return          the result of the computation
     * @see ComputerResult
     */
    ComputerResult compute(MapReduce mapReduce);
}
