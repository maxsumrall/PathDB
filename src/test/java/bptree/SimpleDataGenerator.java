/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree;

import bptree.impl.DiskCache;
import bptree.impl.NodeHeader;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;


public class SimpleDataGenerator {

    public int numberOfPages;
    public int keyLength = 4;
    public int keysPerPage = (((DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH) / Long.BYTES) / keyLength) ;
    public DiskCache disk = DiskCache.temporaryDiskCache(false);

    public SimpleDataGenerator(int numberOfPages) throws IOException {
        this.numberOfPages = numberOfPages;
        for(int i = 0; i < numberOfPages; i++){
            try(PageProxyCursor cursor = disk.getCursor(i, PagedFile.PF_EXCLUSIVE_LOCK)){
                NodeHeader.setNodeTypeLeaf(cursor);
                NodeHeader.setFollowingID(cursor, cursor.getCurrentPageId() + 1);
                NodeHeader.setPrecedingId(cursor, cursor.getCurrentPageId() - 1);
                NodeHeader.setKeyLength(cursor, keyLength);
                NodeHeader.setNumberOfKeys(cursor, keysPerPage);
                cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                for(int j = 0; j < keysPerPage; j++){
                    for(int k = 0; k < keyLength; k++){
                        cursor.putLong((i * keysPerPage) + j + 1);
                    }
                }
            }
        }
    }
}