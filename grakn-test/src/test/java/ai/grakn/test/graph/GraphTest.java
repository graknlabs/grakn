package ai.grakn.test.graph;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GraphTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @Test
    public void isClosedTest() throws Exception {
        GraknGraph graph = engine.factoryWithNewKeyspace().getGraph();
        String keyspace = graph.getKeyspace();
        graph.putEntityType("thing");
        graph.commitOnClose();
        assertFalse(graph.isClosed());
        graph.close();
        assertTrue(graph.isClosed());

        HashSet<Future> futures = new HashSet<>();
        futures.add(Executors.newCachedThreadPool().submit(() -> addThingToBatch(keyspace)));

        for (Future future : futures) {
            future.get();
        }

        assertTrue(graph.isClosed());
    }

    private void addThingToBatch(String keyspace){
        try(GraknGraph graphBatchLoading = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()) {
            graphBatchLoading.getEntityType("thing").addEntity();
            graphBatchLoading.commitOnClose();
            graphBatchLoading.close();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSameGraphs() throws GraknValidationException {
        String key = "mykeyspace";
        GraknGraph graph1 = Grakn.factory(Grakn.DEFAULT_URI, key).getGraph();
        GraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(key);
        assertEquals(graph1, graph2);
        graph1.close();
        graph2.close();
    }

    @Test
    public void checkNumberOfOpenTransactionsChangesAsExpected() throws ExecutionException, InterruptedException {
        GraknGraphFactory factory = Grakn.factory(Grakn.DEFAULT_URI, "MyWonderFullGraph");
        assertEquals(0, factory.openGraphTxs());
        assertEquals(0, factory.openGraphBatchTxs());

        factory.getGraph();
        assertEquals(1, factory.openGraphTxs());
        assertEquals(0, factory.openGraphBatchTxs());

        factory.getGraph();
        factory.getGraphBatchLoading();
        assertEquals(1, factory.openGraphTxs());
        assertEquals(1, factory.openGraphBatchTxs());

        int expectedValue = 1;

        for(int i = 0; i < 5; i ++){
            Executors.newSingleThreadExecutor().submit(factory::getGraph).get();
            Executors.newSingleThreadExecutor().submit(factory::getGraphBatchLoading).get();

            if(!usingTinker()) expectedValue++;

            assertEquals(expectedValue, factory.openGraphTxs());
            assertEquals(expectedValue, factory.openGraphBatchTxs());
        }
    }
}
