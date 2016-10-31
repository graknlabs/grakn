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

import io.grakn.util.ErrorMessage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class GraknFactoryBuilderTest {
    private final static String TEST_CONFIG = "../conf/test/tinker/grakn-tinker.properties";
    private final static String KEYSPACE = "keyspace";
    private final static String ENGINE_URL = "rubbish";

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test(expected=InvocationTargetException.class)
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<GraknFactoryBuilder> c = GraknFactoryBuilder.class.getDeclaredConstructor();
        c.setAccessible(true);
        c.newInstance();
    }

    @Test
    public void testBuildGraknFactory(){
        GraknInternalFactory mgf = GraknFactoryBuilder.getFactory(KEYSPACE, ENGINE_URL, TEST_CONFIG);
        assertThat(mgf, instanceOf(GraknTinkerInternalFactory.class));
    }

    @Test
    public void testSingleton(){
        GraknInternalFactory mgf1 = GraknFactoryBuilder.getFactory(KEYSPACE, ENGINE_URL, TEST_CONFIG);
        GraknInternalFactory mgf2 = GraknFactoryBuilder.getFactory(KEYSPACE, ENGINE_URL, TEST_CONFIG);
        GraknInternalFactory mgf3 = GraknFactoryBuilder.getFactory("key", ENGINE_URL, TEST_CONFIG);
        GraknInternalFactory mgf4 = GraknFactoryBuilder.getFactory("key", ENGINE_URL, TEST_CONFIG);

        assertEquals(mgf1, mgf2);
        assertEquals(mgf3, mgf4);
        assertNotEquals(mgf1, mgf3);
        assertEquals(mgf1.getGraph(true), mgf2.getGraph(true));
        assertNotEquals(mgf1.getGraph(true), mgf3.getGraph(true));
    }

    @Test
    public void testBadConfigPath(){
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage("rubbish"))
        ));
        GraknFactoryBuilder.getFactory(KEYSPACE, ENGINE_URL, "rubbish");
    }

}