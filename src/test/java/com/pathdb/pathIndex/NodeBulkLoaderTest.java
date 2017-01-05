/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import org.junit.Ignore;
import com.pathdb.pathIndex.tree.IndexTree;
import com.pathdb.storage.DiskCache;
import com.pathdb.util.IndexBulkLoader;

import java.io.IOException;

public class NodeBulkLoaderTest {
    IndexTree proxy;

    @Ignore
    public void loadTest() throws IOException {

        DiskCache disk = DiskCache.temporaryDiskCache(false);
        //tree = Tree.initializeNewTree("tmp_tree_yo.dat", disk); //used for debugging
        int numberOfPages = 40000; //100000 pages should roughly equal 20mil keys;
        SimpleDataGenerator dataGenerator = new SimpleDataGenerator(numberOfPages);
        IndexBulkLoader bulkLoader = new IndexBulkLoader(dataGenerator.disk, numberOfPages, 4);
        IndexTree tree = bulkLoader.run();
        System.out.println("Done: " + tree.rootNodeId);
        /*
        for(long i = 0; i < dataGenerator.currentKey; i++){
            long[] key = new long[4];
            for (int k = 0; k < key.length; k++) {
                key[k] = i;
            }
            SearchCursor searchCursor = proxy.find(key);
            try(PageProxyCursor cursor = disk.getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)){
                assert(searchCursor.hasNext(cursor));
            }
        }
        */
    }
}
