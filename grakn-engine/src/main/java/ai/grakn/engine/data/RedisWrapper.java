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
package ai.grakn.engine.data;

import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.util.SimpleURI;
import com.google.common.base.Preconditions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ai.grakn.GraknConfigKey.REDIS_HOST;
import static ai.grakn.GraknConfigKey.REDIS_POOL_SIZE;
import static ai.grakn.GraknConfigKey.REDIS_SENTINEL_HOST;
import static ai.grakn.GraknConfigKey.REDIS_SENTINEL_MASTER;

/**
 * This class just wraps a Jedis  pool so it's transparent whether
 * we use Sentinel or not (and TODO partitioning)
 *
 * @author pluraliseseverythings
 */
public class RedisWrapper {

    private Pool<Jedis> jedisPool;
    private Set<String> uriSet;

    private RedisWrapper(Pool<Jedis> jedisPool, Set<String> uriSet) {
        this.jedisPool = jedisPool;
        this.uriSet = uriSet;
    }

    public static RedisWrapper create(GraknConfig config) {
        List<String> redisUrl = config.getProperty(REDIS_HOST);
        List<String> sentinelUrl = config.getProperty(REDIS_SENTINEL_HOST);
        int poolSize = config.getProperty(REDIS_POOL_SIZE);
        boolean useSentinel = !sentinelUrl.isEmpty();
        Builder builder = builder()
                .setUseSentinel(useSentinel)
                .setPoolSize(poolSize)
                .setURI((useSentinel ? sentinelUrl : redisUrl));
        if (useSentinel) {
            builder.setMasterName(config.getProperty(REDIS_SENTINEL_MASTER));
        }
        return builder.build();
    }

    public Pool<Jedis> getJedisPool() {
        return jedisPool;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void close() {
        jedisPool.close();
    }

    public void testConnection() {
        try {
            getJedisPool().getResource();
        } catch (JedisConnectionException e) {
            throw GraknBackendException.serverStartupException(
                "Redis is not available. Make sure it's running on "
                        + String.join(", ", uriSet)
                        + ". It's possible the destination"
                        + "directory for the rdb and aof files is not writable. Restarting "
                        + "Redis could fix it.", e);
        }
    }

    /**
     * Builder for the wrapper
     */
    public static class Builder {

        static final int DEFAULT_PORT = 6379;
        static final int TIMEOUT = 5000;

        private boolean useSentinel = false;
        private Set<String> uriSet = new HashSet<>();
        private String masterName = null;

        // This is the number of simultaneous connections to Jedis
        private int poolSize = 32;

        public Builder setUseSentinel(boolean useSentinel) {
            this.useSentinel = useSentinel;
            return this;
        }

        public Builder addURI(String uri) {
            this.uriSet.add(uri);
            return this;
        }

        public Builder setURI(Collection<String> uri) {
            this.uriSet = new HashSet<>(uri);
            return this;
        }

        public Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        public Builder setPoolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public RedisWrapper build() {
            // TODO make connection pool sizes configurable
            Preconditions
                    .checkState(!uriSet.isEmpty(), "Trying to build RedisWrapper without uriSet");
            Preconditions.checkState(!(!useSentinel && uriSet.size() > 1),
                    "More than one URL provided but Sentinel not used");
            Preconditions.checkState(!(useSentinel && masterName == null),
                    "Using Sentinel but master name not provided");
            Pool<Jedis> jedisPool;
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setMaxTotal(poolSize);
            if (useSentinel) {
                jedisPool = new JedisSentinelPool(masterName, uriSet, poolConfig, TIMEOUT);
            } else {
                String uri = uriSet.iterator().next();
                SimpleURI simpleURI = SimpleURI.withDefaultPort(uri, DEFAULT_PORT);
                jedisPool = new JedisPool(poolConfig, simpleURI.getHost(), simpleURI.getPort(),
                        TIMEOUT);
            }
            return new RedisWrapper(jedisPool, uriSet);
        }
    }
}
