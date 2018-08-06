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


package ai.grakn.factory;

import ai.grakn.GraknSession;
import ai.grakn.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;


/**
 * <p>
 * Builds a {@link TxFactory}
 * </p>
 * <p>
 * <p>
 * Builds a {@link TxFactory} which is locked to a specific keyspace and engine URL.
 * This uses refection in order to dynamically build any vendor specific factory which implements the
 * {@link TxFactory} API.
 * <p>
 * The factories in this class are treated as singletons.
 * </p>
 *
 * @author fppt
 */


public abstract class TxFactoryBuilder {
    public static final String IN_MEMORY = "in-memory";
    private static final Logger LOG = LoggerFactory.getLogger(GraknTxFactoryBuilder.class);

    public abstract TxFactory<?> getFactory(EmbeddedGraknSession session, boolean isComputerFactory);

    /**
     * @param factoryType The type of the factory to initialise. Any factory which implements {@link TxFactory}
     * @param session     The {@link GraknSession} creating this factory
     * @return A new factory bound to a specific keyspace
     */
    final protected static synchronized TxFactory<?> newFactory(String factoryType, EmbeddedGraknSession session) {
        TxFactory<?> txFactory;
        try {
            txFactory = (TxFactory<?>) Class.forName(factoryType)
                    .getDeclaredConstructor(EmbeddedGraknSession.class)
                    .newInstance(session);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType), e);
        }
        LOG.trace("New factory created " + txFactory);
        return txFactory;
    }
}
