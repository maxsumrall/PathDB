/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import bptree.PageProxyCursor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

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
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory(fs);
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
        if(COMPRESSION){
            //return new LZ4PageCursor(this, id, lockType);
            //return new CompressedPageCursor(this, id, lockType);
            return new SuperCompressedPageCursor(this, id, lockType);
        }
        else{
            return new BasicPageCursor(this, id, lockType);
        }
    }

    public ByteBuffer readPage(IndexTree tree, long id) {
        byte[] byteArray = new byte[0];
        try (PageProxyCursor cursor = getCursor(tree.rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    byteArray = new byte[cursor.getSize()];
                    cursor.getBytes(byteArray);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(byteArray);
    }
    public ByteBuffer readPage(PageProxyCursor cursor) {
        cursor.setOffset(0);
        byte[] byteArray = new byte[0];
        byteArray = new byte[cursor.getSize()];
        cursor.getBytes(byteArray);
        return ByteBuffer.wrap(byteArray);
    }

    public void writePage(long id, byte[] bytes) {
        try (PageProxyCursor cursor = getCursor(id, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    // perform read or write operations on the page
                    cursor.putBytes(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getMaxNumberOfPages(){
        return pageCache.maxCachedPages();
    }

    public int cache_size(){
        return (int)(pageCacheFile.length() / 1000000l);
    }

    public PagedFile getPagedFile(){
        return pagedFile;
    }

    public void shutdown() throws IOException {
        //System.out.println(this.pageCacheFile);
        pagedFile.close();
        pageCache.close();
    }
}
