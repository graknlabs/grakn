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

package ai.grakn.test.graql.reasoner.graphs;

public class GenealogyGraph extends TestGraph{

    final private static String dataDir = "genealogy/";
    final private static String ontologyFile = dataDir + "ontology.gql";

    final private static String peopleTemplatePath = filePath + dataDir + "people-migrator.gql";
    final private static String peoplePath = filePath + dataDir + "people.csv";

    final private static String parentTemplatePath = filePath + dataDir + "parentage-migrator.gql";
    final private static String parentageFilePath = filePath + dataDir + "parentage.csv";

    final private static String marriageTemplatePath = filePath + dataDir + "weddings-migrator.gql";
    final private static String marriageFilePath = filePath + dataDir + "/weddings.csv";

    final private static String rules = dataDir + "rules.gql";

    public GenealogyGraph(){
        super(null, ontologyFile);
        migrateCSV(peopleTemplatePath, peoplePath);
        migrateCSV(parentTemplatePath, parentageFilePath);
        migrateCSV(marriageTemplatePath, marriageFilePath);
        loadFiles(rules);
    }
}
