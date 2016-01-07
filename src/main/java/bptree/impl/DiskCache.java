/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import bptree.PageProxyCursor;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;


public class DiskCache {
    public static int PAGE_SIZE = 8192; //size of a single page, in bytes.
    protected final static String DEFAULT_CACHE_FILE_NAME = "cache.bin";
    protected int max_size_in_mb = 2048;
    protected int maxPages = max_size_in_mb * (1000000 / PAGE_SIZE);
    protected DefaultFileSystemAbstraction fs;
    protected MuninnPageCache pageCache;
    public transient PagedFile pagedFile;
    public File pageCacheFile;
    public boolean COMPRESSION = true;

    private DiskCache(File pageCacheFile, boolean compression) {
        this.COMPRESSION = compression;
        this.pageCacheFile = pageCacheFile;
        try {
            initializePageCache();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initializePageCache() throws IOException {
        fs = new DefaultFileSystemAbstraction();
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        pageCache = new MuninnPageCache(factory, maxPages, PAGE_SIZE, PageCacheTracer.NULL);
        pagedFile = pageCache.map(this.pageCacheFile, PAGE_SIZE);
    }

    public static DiskCache temporaryDiskCache(boolean compression){
        return temporaryDiskCache(DEFAULT_CACHE_FILE_NAME, compression);
    }

    public static DiskCache temporaryDiskCache(String filename, boolean compression){
        File cache_file = new File(filename);
        cache_file.deleteOnExit();
        return new DiskCache(cache_file, compression);
    }

    public static DiskCache persistentDiskCache(boolean compression){
        return  persistentDiskCache(DEFAULT_CACHE_FILE_NAME, compression);
    }

    public static DiskCache persistentDiskCache(String filename, boolean compression){
        return new DiskCache(new File(filename), compression);
    }

    public PageProxyCursor getCursor(long id, int lockType) throws IOException {
            return new CompressedPageCursor(this, id, lockType);
    }

    public void shutdown() throws IOException {
        pagedFile.close();
        pageCache.close();
    }
}
