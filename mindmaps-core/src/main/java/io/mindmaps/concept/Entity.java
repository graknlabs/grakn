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

package io.mindmaps.concept;

import java.util.Collection;

/**
 * An instance of Entity Type which represents some data in the graph.
 */
public interface Entity extends Instance{
    //------------------------------------- Accessors ----------------------------------

    /**
     *
     * @return The Entity Type of this Entity
     */
    EntityType type();

    /**
     *
     * @param resourceTypes Resource Types of the resources attached to this entity
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources(ResourceType ... resourceTypes);
}
