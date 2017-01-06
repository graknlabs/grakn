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

package ai.grakn.concept;



import java.util.Collection;

/**
 * A Resource is the Concept that represents a resource associated with one or more Instances. 
 */
public interface Resource<D> extends Instance{
    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the value of the Resource.
     *
     * @param D The data type of this resource. Supported Types include: String, Long, Double, and Boolean
     * @return The Resource itself
     */
    D getValue();

    /**
     * Retrieves the type of the Resource, that is, the ResourceType of which this resource is an Instance.
     * @param <D> The data type of this resource. Supported Types include: String, Long, Double, and Boolean
     * @return The ResourceType of which this resource is an Instance.
     */
    ResourceType<D> type();

    /**
     * Retrieves the data type of this Resource's ResourceType.
     *
     * @param <D> The data type of the Resource. Supported Types include: String, Long, Double, and Boolean
     * @return The data type of this Resource's type.
     */
    ResourceType.DataType<D> dataType();

    /**
     * Retrieves the set of all Instances that possess this Resource.
     *
     * @return The list of all Instances that possess this Resource.
     */
    Collection<Instance> ownerInstances();

    /**
     * If the Resource is unique, this method retrieves the Instance that possesses it.
     *
     * @return The Instance which is connected to a unique Resource.
     */
    Instance owner();

}
