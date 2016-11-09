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

package io.mindmaps.test.migration.csv;

import io.mindmaps.migration.csv.Main;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.Before;
import org.junit.Test;

public class CSVMigratorMainTest extends AbstractMindmapsMigratorTest {

    private final String dataFile = getFile("csv", "pets/data/pets.csv").getAbsolutePath();
    private final String templateFile = getFile("csv", "pets/template.gql").getAbsolutePath();

    @Before
    public void setup(){
        load(getFile("csv", "pets/schema.gql"));
    }

    @Test
    public void csvMainTest(){
        exit.expectSystemExitWithStatus(0);
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace());
    }

    @Test
    public void tsvMainTest(){
        exit.expectSystemExitWithStatus(0);
        String tsvFile = getFile("csv", "pets/data/pets.tsv").getAbsolutePath();
        runAndAssertDataCorrect("-input", tsvFile, "-template", templateFile, "-separator", "\t", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void spacesMainTest(){
        exit.expectSystemExitWithStatus(0);
        String tsvFile = getFile("csv", "pets/data/pets.spaces").getAbsolutePath();
        runAndAssertDataCorrect("-input", tsvFile, "-template", templateFile, "-separator", " ", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void csvMainTestDistributedLoader(){
        exit.expectSystemExitWithStatus(0);
        runAndAssertDataCorrect("csv", "-input", dataFile, "-template", templateFile, "-uri", "localhost:4567", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void csvMainDifferentBatchSizeTest(){
        exit.expectSystemExitWithStatus(0);
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void csvMainNoArgsTest(){
        exit.expectSystemExitWithStatus(1);
        run();
    }

    @Test
    public void csvMainNoTemplateNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        run("-input", dataFile);
    }

    @Test
    public void csvMainInvalidTemplateFileTest(){
        exception.expect(RuntimeException.class);
        run("-input", dataFile + "wrong", "-template", templateFile + "wrong");
    }

    @Test
    public void csvMainThrowableTest(){
        exception.expect(NumberFormatException.class);
        run("-input", dataFile, "-template", templateFile, "-batch", "hello");
    }

    @Test
    public void unknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        run("-whale", "");
    }

    private void run(String... args){
        Main.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        exit.checkAssertionAfterwards(this::assertPetGraphCorrect);
        run(args);
    }
}
