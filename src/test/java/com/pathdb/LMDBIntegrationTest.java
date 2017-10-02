/**
 * Copyright (C) 2015-2017 - All rights reserved. This file is part of the pathdb project which is
 * released under the GPLv3 license. See file LICENSE.txt or go to
 * http://www.gnu.org/licenses/gpl.txt for full license details. You may use, distribute and modify
 * this code under the terms of the GPLv3 license.
 */
package com.pathdb;

import com.pathdb.pathIndex.PathSerializer;
import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.pathIndex.persisted.LMDBIndexFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static com.pathdb.pathIndex.PathSerializer.serialize;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

public class LMDBIntegrationTest {
    private static final String DB_NAME = LMDBIntegrationTest.class.getName();
    File path;
    Env<ByteBuffer> env;
    Dbi<ByteBuffer> db = null;

    @Rule public final TemporaryFolder dir = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        path = dir.newFolder();
        env = create().setMapSize(MEBIBYTES.toBytes(42)).setMaxDbs(1).open(path);
        db = env.openDbi(DB_NAME, MDB_CREATE);
    }

    @After
    public void after() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void twoKeyTest() throws IOException {
        List<Node> nodesA = new ArrayList<>();
        nodesA.add(new Node(1));
        nodesA.add(new Node(2));
        Path pathA = ImmutablePath.builder().pathId(42).addAllNodes(nodesA).build();
        ByteBuffer keyA = serialize(pathA);

        List<Node> nodesB = new ArrayList<>();
        nodesB.add(new Node(1));
        nodesB.add(new Node(3));
        Path pathB = ImmutablePath.builder().pathId(42).addAllNodes(nodesB).build();
        ByteBuffer keyB = serialize(pathB);

        db.put(keyA, serialize(pathA));
        db.put(keyB, serialize(pathB));

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            Cursor<ByteBuffer> byteBufferCursor = db.openCursor(txn);
            boolean b = byteBufferCursor.get(keyA, GetOp.MDB_SET_RANGE);

            //            final ByteBuffer found = db.get( txn, keyA );
            //            assertNotNull( found );
            //
            //            final ByteBuffer fetchedVal = txn.val();
            //            assertEquals( pathA, PathSerializer.deserialize( fetchedVal ) );
        }

        //        db.delete( key );
        //
        //        try ( Txn<ByteBuffer> txn = env.txnRead() )
        //        {
        //            assertNull( db.get( txn, key ) );
        //        }
    }

    @Test
    public void basicKeyTest() throws IOException {
        List<Node> nodesA = new ArrayList<>();
        nodesA.add(new Node(1));
        nodesA.add(new Node(2));
        Path pathA = ImmutablePath.builder().pathId(42).addAllNodes(nodesA).build();
        ByteBuffer key = serialize(pathA);

        db.put(serialize(pathA), serialize(pathA));

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key);
            assertNotNull(found);

            final ByteBuffer fetchedVal = txn.val();
            assertEquals(pathA, PathSerializer.deserialize(fetchedVal));
        }

        db.delete(key);

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            assertNull(db.get(txn, key));
        }
    }

    @Test
    public void basicTest() throws IOException {
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        final ByteBuffer val = allocateDirect(700);
        key.put("Hello".getBytes(UTF_8)).flip();
        val.put("World".getBytes(UTF_8)).flip();
        final int valSize = val.remaining();

        db.put(key, val);

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key);
            assertNotNull(found);

            final ByteBuffer fetchedVal = txn.val();
            assertThat(fetchedVal.remaining(), is(valSize));

            assertThat(UTF_8.decode(fetchedVal).toString(), is("World"));
        }

        db.delete(key);

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            assertNull(db.get(txn, key));
        }
    }
}
