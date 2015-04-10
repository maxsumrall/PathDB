package bptree.impl;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DiskCache {
    //public static int PAGE_SIZE = bptree.Utils.getIdealBlockSize();
    public static int PAGE_SIZE = 1024;
    protected final static String DEFAULT_CACHE_FILE_NAME = "cache.bin";
    protected int recordSize = 9; //TODO What is this?
    protected int maxPages = 200; //TODO How big should this be?
    protected int pageCachePageSize = PAGE_SIZE;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;
    protected transient DefaultFileSystemAbstraction fs;
    protected transient MuninnPageCache pageCache;
    protected transient PagedFile pagedFile;


    private DiskCache(File cache_file) {
        try {
            initializePageCache(cache_file);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public static DiskCache temporaryDiskCache(){
        return temporaryDiskCache(DEFAULT_CACHE_FILE_NAME);
    }

    public static DiskCache temporaryDiskCache(String filename){
        File cache_file = new File(filename);
        cache_file.deleteOnExit();
        return new DiskCache(cache_file);
    }

    public static DiskCache persistentDiskCache(){
        return persistentDiskCache(DEFAULT_CACHE_FILE_NAME);
    }

    public static DiskCache persistentDiskCache(String filename){
        return new DiskCache(new File(filename));
    }

    private void initializePageCache(File page_cache_file) throws IOException {
        fs = new DefaultFileSystemAbstraction();
        pageCache = new MuninnPageCache(fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL);
        pagedFile = pageCache.map(page_cache_file, filePageSize);
    }

    public ByteBuffer readPage(long id) {
        byte[] byteArray = new byte[0];
        try (PageCursor cursor = pagedFile.io(id, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    byteArray = new byte[PAGE_SIZE];
                    cursor.getBytes(byteArray);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(byteArray);
    }

    public void writePage(long id, byte[] bytes) {
        try (PageCursor cursor = pagedFile.io(id, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    cursor.putBytes(bytes);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
