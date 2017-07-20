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
 *
 */

package ai.grakn.property;

import ai.grakn.concept.Resource;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import static ai.grakn.property.PropertyUtil.choose;
import static ai.grakn.property.PropertyUtil.directInstances;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 * @author Felix Chapman
 */
@RunWith(JUnitQuickcheck.class)
public class ThingPropertyTest {

    @Property
    public void whenGettingTheDirectTypeOfAThing_TheThingIsADirectInstanceOfThatType(Thing thing) {
        Type type = thing.type();
        assertThat(directInstances(type), hasItem(thing));
    }

    @Property
    public void whenGettingTheResourceOfAThing_TheResourcesOwnerIsTheThing(Thing thing, long seed) {
        assumeThat(thing, not(instanceOf(Resource.class)));
        Resource<?> resource = choose(thing.resources(), seed);
        assertTrue("[" + thing + "] is connected to resource [" + resource + "] but is not in it's owner set", resource.ownerInstances().contains(thing));
    }
}
