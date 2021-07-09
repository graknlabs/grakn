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
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.concurrent.executor.ParallelThreadPoolExecutor;
import com.vaticle.typedb.core.migrator.proto.DataProto;
import com.vaticle.typedb.core.migrator.proto.MigratorProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_READABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.INVALID_DATA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.migrator.proto.DataProto.Item.ItemCase.HEADER;

public class DataImporter implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);
    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 1_000;
    private final Path filename;
    private final TypeDB.Session session;
    private final ParallelThreadPoolExecutor executor;
    private final ParallelThreadPoolExecutor readerExecutor;
    private final int parallelism;

    private final Map<String, String> remapLabels;
    private final ConcurrentMap<String, ByteArray> idMap = new ConcurrentHashMap<>();
    private final List<Pair<ByteArray, List<String>>> missingOwnerships = new ArrayList<>();
    private final List<Pair<ByteArray, List<Pair<String, List<String>>>>> missingRolePlayers = new ArrayList<>();
    private final String version;
    private Checksum checksum;
    private AtomicLong totalThingCount = new AtomicLong(0);
    private AtomicLong entityCount = new AtomicLong(0);
    private AtomicLong relationCount = new AtomicLong(0);
    private AtomicLong attributeCount = new AtomicLong(0);
    private AtomicLong ownershipCount = new AtomicLong(0);
    private AtomicLong roleCount = new AtomicLong(0);
    private int txWriteCount = 0;

    DataImporter(TypeDB typedb, String database, Path filename, Map<String, String> remapLabels, String version) {
        if (!Files.exists(filename)) throw TypeDBException.of(FILE_NOT_FOUND, filename);
        this.session = typedb.session(database, Arguments.Session.Type.DATA);
        this.filename = filename;
        this.remapLabels = remapLabels;
        this.version = version;
        assert Executors.isInitialised();
        this.executor = Executors.async1();
        this.readerExecutor = Executors.async2();
        this.parallelism = Executors.PARALLELISATION_FACTOR;
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        if (checksum != null) {
            return MigratorProto.Job.Progress.newBuilder()
                    .setImportProgress(
                            MigratorProto.Job.ImportProgress.newBuilder()
                                    .setAttributesCurrent(attributeCount.get())
                                    .setEntitiesCurrent(entityCount.get())
                                    .setRelationsCurrent(relationCount.get())
                                    .setOwnershipsCurrent(ownershipCount.get())
                                    .setRolesCurrent(roleCount.get())
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
                                    .setAttributesCurrent(attributeCount.get())
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
        // We scan the file to find the checksum. This is probably not a good idea for files larger than several
        // gigabytes but that case is rare and the actual import would take so long that even if this took a few
        // seconds it would still be cheap.
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                if (item.getItemCase() == DataProto.Item.ItemCase.ENTITY ||
                        item.getItemCase() == DataProto.Item.ItemCase.RELATION ||
                        item.getItemCase() == DataProto.Item.ItemCase.ATTRIBUTE) {
                    totalThingCount.incrementAndGet();
                }
            }
        } catch (IOException e) {
            throw TypeDBException.of(FILE_NOT_READABLE, filename.toString());
        }

        try {
            initAndAttributes();
            entitiesOwnerships();
            relations();
//            incompleteRelations();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
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
        return CompletableFuture.runAsync(() -> {
            DataProto.Item item;
            TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE);
            int count = 0;
            try {
                while ((item = items.poll(1, TimeUnit.SECONDS)) != null) {
                    if (count == BATCH_SIZE) {
                        transaction.commit();
                        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
                    }
                    switch (item.getItemCase()) {
                        case ATTRIBUTE:
                            insertAttribute(transaction, item.getAttribute());
                            count++;
                            break;
                        case CHECKSUMS:
                            DataProto.Item.Checksums checksums = item.getChecksums();
                            this.checksum = new Checksum(
                                    checksums.getAttributeCount(),
                                    checksums.getEntityCount(),
                                    checksums.getRelationCount(),
                                    checksums.getOwnershipCount(),
                                    checksums.getRoleCount()
                            );
                        default:
                    }
                }
                transaction.commit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
        }, executor);
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
                    switch (item.getItemCase()) {
                        case ENTITY:
                            insertEntity(transaction, item.getEntity());
                            count++;
                            count += insertOwnerships(transaction, item.getEntity().getId(), item.getEntity().getAttributeList());
                            break;
                        case ATTRIBUTE:
                            count += insertOwnerships(transaction, item.getAttribute().getId(), item.getAttribute().getAttributeList());
                            break;
                    }
                }
                transaction.commit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
        }, executor);
    }

    private void relations() {

    }

    private CompletableFuture<Void> asyncRelations(BlockingQueue<DataProto.Item> items) {
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
                    if (item.getItemCase() == DataProto.Item.ItemCase.RELATION) {
                        count += insertRelation(transaction, item.getRelation());
                        count += insertOwnerships(transaction, item.getRelation().getId(), item.getRelation().getAttributeList());
                    }
                }
                transaction.commit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
        }, executor);
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
        }, readerExecutor);
        return queue;
    }


    void test() {
//
//        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
//            DataProto.Item item;
//            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
//                switch (item.getItemCase()) {
//                    case HEADER:
//                        DataProto.Item.Header header = item.getHeader();
//                        LOG.info("Importing {} from TypeDB {} to {} in TypeDB {}",
//                                header.getOriginalDatabase(),
//                                header.getTypedbVersion(),
//                                session.database().name(),
//                                version);
//                        break;
//                    case ENTITY:
//                        insertEntity(item.getEntity());
//                        break;
//                    case RELATION:
//                        insertRelation(item.getRelation());
//                        break;
//                    case ATTRIBUTE:
//                        insertAttribute(transaction, item.getAttribute());
//                        break;
//                }
//            }
//        } catch (IOException e) {
//            throw TypeDBException.of(FILE_NOT_READABLE, filename.toString());
//        }

//        insertMissingOwnerships();
        insertMissingRolePlayers();
//        commit();
        close();

        LOG.info("Imported {} entities, {} attributes, {} relations ({} players), {} ownerships",
                entityCount,
                attributeCount,
                relationCount,
                roleCount,
                ownershipCount);
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
            attributeCount.incrementAndGet();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(attributeMsg.getLabel()), attributeMsg.getLabel());
        }
    }

    private void insertEntity(TypeDB.Transaction transaction, DataProto.Item.Entity entityMsg) {
        EntityType entityType = transaction.concepts().getEntityType(relabel(entityMsg.getLabel()));
        if (entityType != null) {
            Entity entity = entityType.create();
            idMap.put(entityMsg.getId(), entity.getIID());
            entityCount.incrementAndGet();
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
        ownershipCount.addAndGet(ownerships);
        return ownerships;
    }

    // TODO
    private int insertRelation(TypeDB.Transaction transaction, DataProto.Item.Relation relationMsg) {
        RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
        int relationsAndPlayers = 0;
        if (relationType != null) {
            Map<String, RoleType> roles = getScopedRoleTypes(relationType);
            Relation relation = relationType.create();
            relationsAndPlayers++;
            idMap.put(relationMsg.getId(), relation.getIID());

            List<Pair<String, List<String>>> relationRPs = new ArrayList<>();
            for (DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
                RoleType role = roles.get(relabel(roleMsg.getLabel()));
                if (role != null) {
                    List<String> missingPlayers = new ArrayList<>();
                    for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        Thing player = getThing(transaction, playerMessage.getId());
                        if (player != null) {
                            relation.addPlayer(role, player);
                            roleCount.incrementAndGet();
                            relationsAndPlayers++;
                        } else {
                            missingPlayers.add(playerMessage.getId());
                        }
                    }
                    relationRPs.add(new Pair<>(relabel(roleMsg.getLabel()), missingPlayers));
                } else {
                    throw TypeDBException.of(TYPE_NOT_FOUND, relabel(roleMsg.getLabel()), roleMsg.getLabel());
                }
            }
            if (!relationRPs.isEmpty()) this.missingRolePlayers.add(new Pair<>(relation.getIID(), relationRPs));

            relationCount.incrementAndGet();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
        }
        return relationsAndPlayers;
    }

//
//    private void insertMissingOwnerships() {
//        for (Pair<ByteArray, List<String>> ownership : missingOwnerships) {
//            Thing thing = tx.concepts().getThing(ownership.first());
//            for (String originalAttributeId : ownership.second()) {
//                Thing attrThing = getThing(originalAttributeId);
//                assert thing != null && attrThing != null;
//                thing.setHas(attrThing.asAttribute());
//                ownershipCount++;
//            }
//            mayCommit();
//        }
//        missingOwnerships.clear();
//    }

    private void insertMissingRolePlayers() {
        // TODO
//        for (Pair<ByteArray, List<Pair<String, List<String>>>> rolePlayers : missingRolePlayers) {
//            Thing thing = tx.concepts().getThing(rolePlayers.first());
//            assert thing != null;
//            Relation relation = thing.asRelation();
//            for (Pair<String, List<String>> pair : rolePlayers.second()) {
//                Map<String, RoleType> roles = getScopedRoleTypes(relation.getType());
//                RoleType role = roles.get(pair.first());
//                assert role != null;
//                for (String originalPlayerId : pair.second()) {
//                    Thing player = getThing(originalPlayerId);
//                    relation.addPlayer(role, player);
//                    roleCount++;
//                }
//            }
//            mayCommit();
//        }
//        missingRolePlayers.clear();
    }

    private Thing getThing(TypeDB.Transaction transaction, String originalId) {
        ByteArray newId = idMap.get(originalId);
        Thing thing = transaction.concepts().getThing(newId);
        assert thing != null;
        return thing;
    }

    private Map<String, RoleType> getScopedRoleTypes(RelationType relationType) {
        return relationType.getRelates().stream().collect(
                Collectors.toMap(x -> x.getLabel().scopedName(), x -> x));
    }

    private String relabel(String label) {
        return remapLabels.getOrDefault(label, label);
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
