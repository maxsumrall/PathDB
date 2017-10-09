/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.persisted;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import com.pathdb.pathIndex.PathIndex;
import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.ImmutablePathPrefix;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.pathIndex.models.PathPrefix;
import java.io.IOException;
import java.util.Iterator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LMDBIndexFactoryTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void createLMDBTest() throws Exception {
        PathIndex build =
                new LMDBIndexFactory(tmp.newFolder("testDir")).withMaxDBSize(42, MEBIBYTES).build();
    }

    @Test
    public void testInsertionOrderAndRetrievalOrder() throws IOException {
        PathIndex db =
                new LMDBIndexFactory(tmp.newFolder("testDir")).withMaxDBSize(40, MEBIBYTES).build();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++)
                for (int k = 0; k < 10; k++) {
                    {
                        Path path =
                                ImmutablePath.builder()
                                        .pathId(k)
                                        .addNodes(new Node(i), new Node(k), new Node(j))
                                        .build();
                        db.insert(path);
                    }
                }
        }

        for( int i = 0; i < 10; i++){
            int count = 1;
            Iterable<Path> paths = db.getPaths(ImmutablePathPrefix.builder().pathId(i).build());
            Iterator<Path> iterator = paths.iterator();
            Path next;
            Path prev = iterator.next();
            while(iterator.hasNext())
            {
                count++;
                next = iterator.next();
                assertThat(next, greaterThan(prev));
                prev = next;
            }
            assertEquals(db.getStatisticsStore().getCardinality(i), count);
        }
    }

    @Test
    public void openInsertCloseOpenRead() throws Exception {
        PathIndex db =
                new LMDBIndexFactory(tmp.newFolder("testDir")).withMaxDBSize(42, MEBIBYTES).build();

        Path pathA =
                ImmutablePath.builder()
                        .pathId(42L)
                        .addNodes(new Node(42), new Node(45), new Node(9999))
                        .build();
        db.insert(pathA);

        PathPrefix pathPrefix =
                ImmutablePathPrefix.builder().pathId(42L).addNodes(new Node(42)).build();
        Iterable<Path> paths = db.getPaths(pathPrefix);
        for (Path p : paths) {
            System.out.println(p);
        }
    }
}
