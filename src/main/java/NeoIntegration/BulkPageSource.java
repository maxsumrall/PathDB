/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;

import bptree.BulkLoadDataSource;
import bptree.PageProxyCursor;
import bptree.impl.DiskCache;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

public class BulkPageSource implements BulkLoadDataSource {
    DiskCache disk;
    long finalPage;
    long currentPage = 0;
    PageProxyCursor cursor;

    public BulkPageSource(DiskCache disk, long finalPage) throws IOException {
        this.disk = disk;
        this.finalPage = finalPage;
        cursor = disk.getCursor(0, PagedFile.PF_SHARED_LOCK);
    }

    @Override
    public byte[] nextPage() throws IOException {
        cursor.next(currentPage++);
        byte[] bytes = new byte[cursor.getInt()];
        cursor.getBytes(bytes);
        return bytes;
    }

    @Override
    public boolean hasNext() throws IOException {
        if(currentPage > finalPage){
            this.cursor.close();
        }
        return currentPage <= finalPage;
    }
}

