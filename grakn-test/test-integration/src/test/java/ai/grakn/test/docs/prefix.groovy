/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.docs

import ai.grakn.*
import ai.grakn.batch.BatchExecutorClient
import ai.grakn.concept.*
import ai.grakn.engine.TaskId
import ai.grakn.graql.*
import ai.grakn.graql.answer.ConceptMap
import ai.grakn.migration.csv.CSVMigrator
import mjson.Json

import java.time.LocalDateTime

import static ai.grakn.concept.AttributeType.DataType.STRING
import static ai.grakn.graql.Graql.*

// This is some dumb stuff so IntelliJ doesn't get rid of the imports
//noinspection GroovyConstantIfStatement
if (false) {
    label("hello")
    ConceptMap answer = null
    Concept concept = null
    Var var = var()
    GraknSession session = null
    LocalDateTime time = null
    CSVMigrator migrator = null
    BatchExecutorClient client = null
    Json json = null
    TaskId id = null
    str = STRING
    Label label = null
    Order order = null
    GraknTx tx = null
    Attribute attribute = null
}

// Initialise graphs and fields that the code samples will use

uri = JavaDocsTest.engine.uri()
host = uri.host
port = uri.port

tx = DocTestUtil.getTestGraph(uri, JavaDocsTest.knowledgeBaseName).transaction(GraknTxType.WRITE)

_otherTx = DocTestUtil.getTestGraph(uri, JavaDocsTest.knowledgeBaseName).transaction(GraknTxType.WRITE)
keyspace = _otherTx.keyspace()
_otherTx.close()

callback = {x -> x}

qb = tx.graql()

def body() {
$putTheBodyHere
}

try {
    body()
} finally {
    tx.close()
}



