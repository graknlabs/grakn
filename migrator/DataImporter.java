/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.migrator;

import com.google.protobuf.Parser;
import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.migrator.proto.DataProto;
import com.vaticle.typedb.core.migrator.proto.MigratorProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.INVALID_DATA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.PLAYER_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.migrator.proto.DataProto.Item.ItemCase.HEADER;

public class DataImporter implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);
    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 1000;
    private final Path filename;
    private final TypeDB.Session session;
    private final Executor importExecutor;
    private final int parallelism;

    private final Map<String, String> remapLabels;
    private final ConcurrentMap<String, ByteArray> idMap;
    private final ConcurrentSet<DataProto.Item.Relation> skippedNestedRelations;
    private final String version;
    private final Status status;
    private Checksum checksum;

    DataImporter(TypeDB typedb, String database, Path filename, Map<String, String> remapLabels, String version) {
        if (!Files.exists(filename)) throw TypeDBException.of(FILE_NOT_FOUND, filename);
        this.session = typedb.session(database, Arguments.Session.Type.DATA);
        this.filename = filename;
        this.remapLabels = remapLabels;
        this.version = version;
        assert com.vaticle.typedb.core.concurrent.executor.Executors.isInitialised();
        this.parallelism = com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
        this.importExecutor = Executors.newFixedThreadPool(parallelism);
        this.idMap = new ConcurrentHashMap<>();
        this.skippedNestedRelations = new ConcurrentSet<>();
        this.status = new Status();
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        if (checksum != null) {
            return MigratorProto.Job.Progress.newBuilder()
                    .setImportProgress(
                            MigratorProto.Job.ImportProgress.newBuilder()
                                    .setAttributesCurrent(status.attributeCount.get())
                                    .setEntitiesCurrent(status.entityCount.get())
                                    .setRelationsCurrent(status.relationCount.get())
                                    .setOwnershipsCurrent(status.ownershipCount.get())
                                    .setRolesCurrent(status.roleCount.get())
                                    .setAttributes(checksum.attributes)
                                    .setEntities(checksum.entities)
                                    .setRelations(checksum.relations)
                                    .setOwnerships(checksum.ownerships)
                                    .setRoles(checksum.roles)
                                    .build()
                    ).build();
        } else {
            return MigratorProto.Job.Progress.newBuilder()
                    .setImportProgress(
                            MigratorProto.Job.ImportProgress.newBuilder()
                                    .setAttributesCurrent(status.attributeCount.get())
                                    .build()
                    ).build();
        }
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public void run() {
        try {
            initAndAttributes();
            entitiesOwnerships();
            relations();
            insertSkippedRelations();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            LOG.info("Imported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                    status.entityCount.get(),
                    status.attributeCount.get(),
                    status.relationCount.get(),
                    status.roleCount.get(),
                    status.ownershipCount.get());
            close();
        }
    }

    private void initAndAttributes() throws InterruptedException, ExecutionException {
        BlockingQueue<DataProto.Item> items = readFile();
        DataProto.Item item = items.take();
        assert item.getItemCase().equals(HEADER);
        DataProto.Item.Header header = item.getHeader();
        LOG.info("Importing {} from TypeDB {} to {} in TypeDB {}",
                header.getOriginalDatabase(),
                header.getTypedbVersion(),
                session.database().name(),
                version);
        CompletableFuture<Void>[] workers = new CompletableFuture[parallelism];
        for (int i = 0; i < parallelism; i++) {
            workers[i] = asyncAttrAndChecksum(items);
        }
        CompletableFuture.allOf(workers).get();
    }

    private CompletableFuture<Void> asyncAttrAndChecksum(BlockingQueue<DataProto.Item> items) {
        return batchWorker(items, (tx, item) -> {
            switch (item.getItemCase()) {
                case ATTRIBUTE:
                    insertAttribute(tx, item.getAttribute());
                    return 1;
                case CHECKSUMS:
                    DataProto.Item.Checksums checksums = item.getChecksums();
                    this.checksum = new Checksum(checksums.getAttributeCount(), checksums.getEntityCount(),
                            checksums.getRelationCount(), checksums.getOwnershipCount(), checksums.getRoleCount());
                    return 0;
                default:
                    return 0;
            }
        });
    }

    private void entitiesOwnerships() throws ExecutionException, InterruptedException {
        BlockingQueue<DataProto.Item> items = readFile();
        CompletableFuture<Void>[] workers = new CompletableFuture[parallelism];
        for (int i = 0; i < parallelism; i++) {
            workers[i] = asyncEntityAndOwnerships(items);
        }
        CompletableFuture.allOf(workers).get();
    }

    private CompletableFuture<Void> asyncEntityAndOwnerships(BlockingQueue<DataProto.Item> items) {
        AtomicLong n = new AtomicLong(0);
        return batchWorker(items, (tx, item) -> {
            System.out.println(n.incrementAndGet());
            switch (item.getItemCase()) {
                case ENTITY:
                    insertEntity(tx, item.getEntity());
                    return 1 + insertOwnerships(tx, item.getEntity().getId(), item.getEntity().getAttributeList());
                case ATTRIBUTE:
                    return insertOwnerships(tx, item.getAttribute().getId(), item.getAttribute().getAttributeList());
                default:
                    return 0;
            }
        });
    }

    private void relations() throws ExecutionException, InterruptedException {
        BlockingQueue<DataProto.Item> items = readFile();
        CompletableFuture<Void>[] workers = new CompletableFuture[parallelism];
        for (int i = 0; i < parallelism; i++) {
            workers[i] = asyncRelations(items);
        }
        CompletableFuture.allOf(workers).get();
    }

    private CompletableFuture<Void> asyncRelations(BlockingQueue<DataProto.Item> items) {
        return batchWorker(items, (tx, item) -> {
            if (item.getItemCase() == DataProto.Item.ItemCase.RELATION) {
                Optional<Integer> inserted = tryInsertCompleteRelation(tx, item.getRelation());
                if (inserted.isPresent()) {
                    return inserted.get() + insertOwnerships(tx, item.getRelation().getId(), item.getRelation().getAttributeList());
                } else {
                    skippedNestedRelations.add(item.getRelation());
                    return 0;
                }
            } else return 0;
        });
    }

    private CompletableFuture<Void> batchWorker(BlockingQueue<DataProto.Item> items, BiFunction<TypeDB.Transaction, DataProto.Item, Integer> fn) {
        return CompletableFuture.runAsync(() -> {
            DataProto.Item item;
            TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE);
            int count = 0;
            try {
                while ((item = items.poll(1, TimeUnit.SECONDS)) != null) {
                    if (count >= BATCH_SIZE) {
                        transaction.commit();
                        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
                    }
                    count += fn.apply(transaction, item);
                }
                transaction.commit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
        }, importExecutor);
    }

    private BlockingQueue<DataProto.Item> readFile() {
        BlockingQueue<DataProto.Item> queue = new ArrayBlockingQueue<>(1000);
        CompletableFuture.runAsync(() -> {
            try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
                DataProto.Item item;
                while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                    queue.put(item);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        return queue;
    }


    private void insertAttribute(TypeDB.Transaction transaction, DataProto.Item.Attribute attributeMsg) {
        AttributeType attributeType = transaction.concepts().getAttributeType(relabel(attributeMsg.getLabel()));
        if (attributeType != null) {
            DataProto.ValueObject valueMsg = attributeMsg.getValue();
            Attribute attribute;
            switch (valueMsg.getValueCase()) {
                case STRING:
                    attribute = attributeType.asString().put(valueMsg.getString());
                    break;
                case BOOLEAN:
                    attribute = attributeType.asBoolean().put(valueMsg.getBoolean());
                    break;
                case LONG:
                    attribute = attributeType.asLong().put(valueMsg.getLong());
                    break;
                case DOUBLE:
                    attribute = attributeType.asDouble().put(valueMsg.getDouble());
                    break;
                case DATETIME:
                    attribute = attributeType.asDateTime().put(
                            Instant.ofEpochMilli(valueMsg.getDatetime()).atZone(ZoneId.of("Z")).toLocalDateTime());
                    break;
                default:
                    throw TypeDBException.of(INVALID_DATA);
            }
            idMap.put(attributeMsg.getId(), attribute.getIID());
            status.attributeCount.incrementAndGet();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(attributeMsg.getLabel()), attributeMsg.getLabel());
        }
    }

    private void insertEntity(TypeDB.Transaction transaction, DataProto.Item.Entity entityMsg) {
        EntityType entityType = transaction.concepts().getEntityType(relabel(entityMsg.getLabel()));
        if (entityType != null) {
            Entity entity = entityType.create();
            idMap.put(entityMsg.getId(), entity.getIID());
            status.entityCount.incrementAndGet();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(entityMsg.getLabel()), entityMsg.getLabel());
        }
    }

    private int insertOwnerships(TypeDB.Transaction transaction, String oldId, List<DataProto.Item.OwnedAttribute> ownedMsgs) {
        Thing owner = getThing(transaction, oldId);
        int ownerships = 0;
        for (DataProto.Item.OwnedAttribute ownedMsg : ownedMsgs) {
            Thing attrThing = getThing(transaction, ownedMsg.getId());
            assert attrThing != null;
            owner.setHas(attrThing.asAttribute());
            ownerships++;
        }
        status.ownershipCount.addAndGet(ownerships);
        return ownerships;
    }

    private Optional<Integer> tryInsertCompleteRelation(TypeDB.Transaction transaction, DataProto.Item.Relation relationMsg) {
        RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
        Optional<List<Pair<RoleType, Thing>>> players;
        if (relationType == null) {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
        } else if ((players = getAllPlayers(transaction, relationType, relationMsg)).isPresent()) {
            assert players.get().size() > 0;
            Relation relation = relationType.create();
            idMap.put(relationMsg.getId(), relation.getIID());
            players.get().forEach(rp -> {
                relation.addPlayer(rp.first(), rp.second());
            });
            status.relationCount.incrementAndGet();
            status.roleCount.addAndGet(players.get().size());
            return Optional.of(1 + players.get().size());
        } else {
            return Optional.empty();
        }
    }

    private Optional<List<Pair<RoleType, Thing>>> getAllPlayers(TypeDB.Transaction transaction, RelationType relationType,
                                                                DataProto.Item.Relation relationMsg) {
        assert relationMsg.getRoleList().size() > 0;
        List<Pair<RoleType, Thing>> players = new ArrayList<>();
        for (DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
            RoleType roleType = getRoleType(relationType, roleMsg);
            for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                Thing player = getThing(transaction, playerMessage.getId());
                if (player == null) return Optional.empty();
                else players.add(new Pair<>(roleType, player));
            }
            return Optional.of(players);
        }
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private RoleType getRoleType(RelationType relationType, DataProto.Item.Relation.Role roleMsg) {
        String unscopedRoleLabel;
        String roleLabel = relabel(roleMsg.getLabel());
        if (roleLabel.contains(":")) unscopedRoleLabel = roleLabel.split(":")[1];
        else unscopedRoleLabel = roleLabel;
        RoleType roleType = relationType.getRelates(unscopedRoleLabel);
        if (roleType == null) {
            throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, roleLabel, roleMsg.getLabel(), relationType.getLabel().name());
        } else return roleType;
    }

    private void insertSkippedRelations() {
        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            skippedNestedRelations.forEach(relationMsg -> {
                RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
                if (relationType == null) {
                    throw TypeDBException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
                }
                Relation relation = relationType.create();
                idMap.put(relationMsg.getId(), relation.getIID());
            });

            skippedNestedRelations.forEach(relationMsg -> {
                RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
                Relation relation = transaction.concepts().getThing(idMap.get(relationMsg.getId())).asRelation();
                relationMsg.getRoleList().forEach(roleMsg -> {
                    RoleType roleType = getRoleType(relationType, roleMsg);
                    for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        Thing player = getThing(transaction, playerMessage.getId());
                        if (player == null) throw TypeDBException.of(PLAYER_NOT_FOUND, relationType.getLabel());
                        else relation.addPlayer(roleType, player);
                    }
                });
            });
            transaction.commit();
        }
    }

    private Thing getThing(TypeDB.Transaction transaction, String originalId) {
        ByteArray newId = idMap.get(originalId);
        Thing thing = transaction.concepts().getThing(newId);
        assert thing != null;
        return thing;
    }

    private String relabel(String label) {
        return remapLabels.getOrDefault(label, label);
    }

    private static class Status {
        private final AtomicLong entityCount = new AtomicLong(0);
        private final AtomicLong relationCount = new AtomicLong(0);
        private final AtomicLong attributeCount = new AtomicLong(0);
        private final AtomicLong ownershipCount = new AtomicLong(0);
        private final AtomicLong roleCount = new AtomicLong(0);
    }

    private static class Checksum {
        private final long attributes;
        private final long entities;
        private final long relations;
        private final long ownerships;
        private final long roles;

        Checksum(long attributes, long entities, long relations, long ownerships, long roles) {
            this.attributes = attributes;
            this.entities = entities;
            this.relations = relations;
            this.ownerships = ownerships;
            this.roles = roles;
        }
    }
}
