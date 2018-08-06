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

package ai.grakn;

import ai.grakn.util.ErrorMessage;
import com.google.auto.value.AutoValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class for keys of properties in the file {@code grakn.properties}.
 *
 * @param <T> the type of the values of the key
 * @author Grakn Warriors
 */
@AutoValue
public abstract class GraknConfigKey<T> {

    /**
     * Parser for a {@link GraknConfigKey}.
     * Describes how to {@link #read(String)} and {@link #write(Object)} properties.
     *
     * @param <T> The type of the property value
     */
    public interface KeyParser<T> {

        T read(String string);

        default String write(T value) {
            return value.toString();
        }
    }

    // These are helpful parser to describe how to parse parameters of certain types.
    public static final KeyParser<String> STRING = string -> string;
    public static final KeyParser<Integer> INT = Integer::parseInt;
    public static final KeyParser<Boolean> BOOL = Boolean::parseBoolean;
    public static final KeyParser<Long> LONG = Long::parseLong;
    public static final KeyParser<Path> PATH = Paths::get;
    public static final KeyParser<List<String>> CSV = new KeyParser<List<String>>() {
        @Override
        public List<String> read(String string) {
            Stream<String> split = Arrays.stream(string.split(","));
            return split.map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
        }

        @Override
        public String write(List<String> value) {
            return value.stream().collect(Collectors.joining(","));
        }
    };

    public static final GraknConfigKey<Integer> WEBSERVER_THREADS = key("webserver.threads", INT);
    public static final GraknConfigKey<Integer> NUM_BACKGROUND_THREADS = key("background-tasks.threads", INT);

    public static final GraknConfigKey<String> SERVER_HOST_NAME = key("server.host");
    public static final GraknConfigKey<Integer> SERVER_PORT = key("server.port", INT);
    public static final GraknConfigKey<Integer> GRPC_PORT = key("grpc.port", INT);

    public static final GraknConfigKey<String> STORAGE_HOSTNAME = key("storage.hostname", STRING);
    public static final GraknConfigKey<String> STORAGE_BATCH_LOADING = key("storage.batch-loading", STRING);
    public static final GraknConfigKey<String> STORAGE_KEYSPACE = key("storage.cassandra.keyspace", STRING);
    public static final GraknConfigKey<Integer> STORAGE_REPLICATION_FACTOR = key("storage.cassandra.replication-factor", INT);

    public static final GraknConfigKey<List<String>> REDIS_HOST = key("queue.host", CSV);
    public static final GraknConfigKey<List<String>> REDIS_SENTINEL_HOST = key("queue.sentinel.host", CSV);
    public static final GraknConfigKey<String> REDIS_BIND = key("bind");
    public static final GraknConfigKey<String> REDIS_SENTINEL_MASTER = key("queue.sentinel.master");
    public static final GraknConfigKey<Integer> REDIS_POOL_SIZE = key("queue.pool-size", INT);
    public static final GraknConfigKey<Integer> POST_PROCESSOR_POOL_SIZE = key("post-processor.pool-size", INT);
    public static final GraknConfigKey<Integer> POST_PROCESSOR_DELAY = key("post-processor.delay", INT);

    public static final GraknConfigKey<Path> STATIC_FILES_PATH = key("server.static-file-dir", PATH);

    public static final GraknConfigKey<Integer> SESSION_CACHE_TIMEOUT_MS = key("knowledge-base.schema-cache-timeout-ms", INT);

    public static final GraknConfigKey<Integer> TASKS_RETRY_DELAY = key("tasks.retry.delay", INT);

    public static final GraknConfigKey<Long> SHARDING_THRESHOLD = key("knowledge-base.sharding-threshold", LONG);
    public static final GraknConfigKey<String> KB_MODE = key("knowledge-base.mode");
    public static final GraknConfigKey<String> KB_ANALYTICS = key("knowledge-base.analytics");
    public static final GraknConfigKey<String> DATA_DIR = key("data-dir");
    public static final GraknConfigKey<String> LOG_DIR = key("log.dirs");

    public static final GraknConfigKey<Boolean> TEST_START_EMBEDDED_COMPONENTS =
            key("test.start.embedded.components", BOOL);

    /**
     * The name of the key, how it looks in the properties file
     */
    public abstract String name();

    /**
     * The parser used to read and write the property.
     */
    abstract KeyParser<T> parser();

    /**
     * Parse the value of a property.
     *
     * This function should return an empty optional if the key was not present and there is no default value.
     *
     * @param value the value of the property. Empty if the property isn't in the property file.
     * @param configFilePath path to the config file
     * @return the parsed value
     *
     * @throws RuntimeException if the value is not present and there is no default value
     */
    public final T parse(String value, Path configFilePath) {
        if (value == null) {
            throw new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(name(), configFilePath));
        }

        return parser().read(value);
    }

    /**
     * Convert the value of the property into a string to store in a properties file
     */
    public final String valueToString(T value) {
        return parser().write(value);
    }

    /**
     * Create a key for a string property
     */
    public static GraknConfigKey<String> key(String value) {
        return key(value, STRING);
    }

    /**
     * Create a key with the given parser
     */
    public static <T> GraknConfigKey<T> key(String value, KeyParser<T> parser) {
        return new AutoValue_GraknConfigKey<>(value, parser);
    }

}
