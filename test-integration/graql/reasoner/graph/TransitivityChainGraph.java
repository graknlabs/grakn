/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.graph;

import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;

import static grakn.core.util.GraqlTestUtil.loadFromFile;
import static grakn.core.util.GraqlTestUtil.putEntityWithResource;

@SuppressWarnings("CheckReturnValue")
public class TransitivityChainGraph {

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/";
    private final static String gqlFile = "quadraticTransitivity.gql";
    private final static Label key = Label.of("index");

    public TransitivityChainGraph(Session session){
        this.session = session;
    }

    public final void load(int n) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, Transaction tx){
        Role qfrom = tx.getRole("Q-from");
        Role qto = tx.getRole("Q-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        RelationType q = tx.getRelationType("Q");
        Thing aInst = putEntityWithResource(tx, "a", tx.getEntityType("entity2"), key);
        ConceptId[] aInstanceIds = new ConceptId[n];
        for(int i = 0 ; i < n ;i++) {
            aInstanceIds[i] = putEntityWithResource(tx, "a" + i, aEntity, key).id();
        }

        q.create()
                .assign(qfrom, aInst)
                .assign(qto, tx.getConcept(aInstanceIds[0]));

        for(int i = 0 ; i < n - 1 ; i++) {
            q.create()
                    .assign(qfrom, tx.getConcept(aInstanceIds[i]))
                    .assign(qto, tx.getConcept(aInstanceIds[i+1]));
        }
    }

}
