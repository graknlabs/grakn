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

package ai.grakn.test.engine;

import ai.grakn.Keyspace;
import ai.grakn.test.EngineContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public class GraknEngineStartTest {
    private static final Set<EngineContext> engines = new HashSet<>();

    @ClassRule
    public static final EngineContext engine1 = EngineContext.createWithEmbeddedRedis();

    @ClassRule
    public static final EngineContext engine2 = EngineContext.createWithEmbeddedRedis();

    @ClassRule
    public static final EngineContext engine3 = EngineContext.createWithEmbeddedRedis();

    @BeforeClass
    public static void groupEngines(){
        engines.add(engine1);
        engines.add(engine2);
        engines.add(engine3);
    }

    @Test
    public void whenStartingMultipleEngines_InitializationSucceeds() throws InterruptedException {
        HashSet<CompletableFuture<Void>> futures = new HashSet<>();

        //Check That They Running
        engines.forEach(engine -> futures.add(
                CompletableFuture.supplyAsync(engine::server).handle((result, exception) -> handleException(exception))
        ));

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    }

    @Test
    public void whenStartingAndCreatingKeyspace_InitializationSucceeds() throws InterruptedException {
        /*HashSet<CompletableFuture<Void>> cfs = new HashSet<>();
        final GraknEngineServer engine = makeEngine(PORTS[0]);
        cfs
                .add(CompletableFuture.runAsync(engine::start));
        cfs
                .add(CompletableFuture.runAsync(() -> {
                    while(!engine.getGraknEngineStatus().isReady()) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            fail();
                        }
                    }
                    engine.factory().systemKeyspace().openKeyspace(Keyspace.of("grakn"));
                }));
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).join();
        engine.close();*/

        HashSet<CompletableFuture<Void>> futures = new HashSet<>();

        engines.forEach(engine ->{
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail();
                }
                engine.server().factory().systemKeyspace().openKeyspace(Keyspace.of("grakn"));
            }));
        });

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    }

    private Void handleException(Throwable exception) {
        if (exception != null) {
            exception.printStackTrace();
            fail("Could not initialize engine successfully");
        }
        return null;
    }
}
