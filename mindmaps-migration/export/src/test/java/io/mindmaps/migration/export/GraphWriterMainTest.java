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

package io.mindmaps.migration.export;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.example.PokemonGraphFactory;
import io.mindmaps.factory.GraphFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class GraphWriterMainTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsGraph original = GraphFactory.getInstance().getGraph("original");
        PokemonGraphFactory.loadGraph(original);
        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }


    @Test
    public void exportOntologyToSystemOutTest(){
        runAndAssertDataCorrect(new String[]{"export", "-ontology", "-keyspace", "original"});
    }

    @Test
    public void exportDataToSystemOutTest(){
        runAndAssertDataCorrect(new String[]{"export", "-data", "-keyspace", "original"});
    }

    @Test
    public void exportToFileTest(){
        runAndAssertDataCorrect(new String[]{"export", "-data", "-file", "/tmp/pokemon.gql", "-keyspace", "original"});
        File pokemonFile = new File("/tmp/pokemon.gql");
        assertTrue(pokemonFile.exists());
    }

    @Test
    public void exportToFileNotFoundTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Problem writing to file grah/?*");
        runAndAssertDataCorrect(new String[]{"export", "-data", "-file", "grah/?*", "-keyspace", "original"});
    }

    @Test
    public void exportNoGraphNameTest(){
        runAndAssertDataCorrect(new String[]{"export", "ontology"});
    }

    @Test
    public void exportEngineURLProvidedTest(){
        runAndAssertDataCorrect(new String[]{"export", "-data", "-uri", "localhost:4567", "-keyspace", "original"});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);
    }
}
