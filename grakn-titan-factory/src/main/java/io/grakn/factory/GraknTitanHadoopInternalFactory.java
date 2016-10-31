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

package io.grakn.factory;

import io.grakn.graph.internal.AbstractGraknGraph;
import io.grakn.util.ErrorMessage;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class GraknTitanHadoopInternalFactory extends AbstractGraknInternalFactory<AbstractGraknGraph<HadoopGraph>, HadoopGraph> {
    private static final String CLUSTER_KEYSPACE = "titanmr.ioformat.conf.storage.cassandra.keyspace";
    private static final String INPUT_KEYSPACE = "cassandra.input.keyspace";
    private final Logger LOG = LoggerFactory.getLogger(GraknTitanHadoopInternalFactory.class);

    GraknTitanHadoopInternalFactory(String keyspace, String engineUrl, String config) {
        super(keyspace, engineUrl, config);
    }

    @Override
    boolean isClosed(HadoopGraph innerGraph) {
        return false;
    }

    @Override
    AbstractGraknGraph<HadoopGraph> buildGraknGraphFromTinker(HadoopGraph graph, boolean batchLoading) {
        throw new UnsupportedOperationException(ErrorMessage.CANNOT_PRODUCE_GRAKN_GRAPH.getMessage(HadoopGraph.class.getName()));
    }

    @Override
    HadoopGraph buildTinkerPopGraph() {
        LOG.warn("Hadoop graph ignores parameter address [" + super.engineUrl + "]");
        return (HadoopGraph) GraphFactory.open(buildConfig(super.keyspace, super.config));
    }

    private static Configuration buildConfig(String name, String pathToConfig){
        try {
            PropertiesConfiguration properties = new PropertiesConfiguration(new File(pathToConfig));
            properties.setProperty(CLUSTER_KEYSPACE, name);
            properties.setProperty(INPUT_KEYSPACE, name);
            return properties;
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig), e);
        }
    }
}
