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

package io.grakn.test.graql.analytics;

import io.grakn.Grakn;
import io.grakn.GraknComputer;
import io.grakn.factory.GraknGraphFactoryMock;
import io.grakn.graql.internal.analytics.Analytics;

import java.util.Set;

/**
 *
 */
public class AnalyticsMock extends Analytics {
    int numberOfWorkers;
    private final String keyspace;

    public AnalyticsMock(String keySpace, Set<String> subTypeIds, Set<String> statisticsResourceTypeIds) {
        super(keySpace, subTypeIds, statisticsResourceTypeIds);
        this.keyspace = keySpace;
    }

    public AnalyticsMock(String keySpace, Set<String> subTypeIds, Set<String> statisticsResourceTypeIds, int numberOfWorkers) {
        super(keySpace, subTypeIds, statisticsResourceTypeIds);
        this.numberOfWorkers = numberOfWorkers;
        this.keyspace = keySpace;
    }

    @Override
    protected GraknComputer getGraphComputer() {
        GraknGraphFactoryMock factory = new GraknGraphFactoryMock(keyspace, Grakn.DEFAULT_URI);
        return factory.getGraphComputer(numberOfWorkers);
    }
}
