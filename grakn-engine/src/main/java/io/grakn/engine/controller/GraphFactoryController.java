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

package io.grakn.engine.controller;


import io.grakn.engine.util.ConfigProperties;
import io.grakn.exception.GraknEngineServerException;
import io.grakn.util.ErrorMessage;
import io.grakn.util.REST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static spark.Spark.get;


/**
 * REST controller used by GraknGraphFactoryImpl to retrieve graph configuration for a given graph name.
 */

public class GraphFactoryController {
    private final Logger LOG = LoggerFactory.getLogger(GraphFactoryController.class);

    public GraphFactoryController() {
        ConfigProperties prop = ConfigProperties.getInstance();

        get(REST.WebPath.GRAPH_FACTORY_URI, (req, res) -> {
            String graphConfig = req.queryParams(REST.Request.GRAPH_CONFIG_PARAM);

            try {
                if (graphConfig == null) {
                    graphConfig = ConfigProperties.GRAPH_CONFIG_PROPERTY;
                } else {
                    switch (graphConfig) {
                        case REST.GraphConfig.DEFAULT:
                            graphConfig = ConfigProperties.GRAPH_CONFIG_PROPERTY;
                            break;
                        case REST.GraphConfig.BATCH:
                            graphConfig = ConfigProperties.GRAPH_BATCH_CONFIG_PROPERTY;
                            break;
                        case REST.GraphConfig.COMPUTER:
                            graphConfig = ConfigProperties.GRAPH_COMPUTER_CONFIG_PROPERTY;
                            break;
                    }
                }
                return new String(Files.readAllBytes(Paths.get(prop.getPath(graphConfig))));
            } catch (IOException e) {
                throw new GraknEngineServerException(500, ErrorMessage.NO_CONFIG_FILE.getMessage(prop.getPath(graphConfig)));
            }
        });

    }
}
