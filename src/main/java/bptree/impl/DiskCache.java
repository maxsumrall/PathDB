package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DiskCache {
    public static int PAGE_SIZE = 8192;
    protected final static String DEFAULT_CACHE_FILE_NAME = "cache.bin";
    protected int recordSize = 9; //TODO What is this?
    protected int max_size_in_mb = 16384;
    protected int maxPages = max_size_in_mb * (1000000 / PAGE_SIZE);
    //protected int maxPages = 8000; //TODO How big should this be?
    protected int pageCachePageSize = PAGE_SIZE;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;
    protected transient DefaultFileSystemAbstraction fs;
    protected transient MuninnPageCache pageCache;
    public transient PagedFile pagedFile;
    public File cache_file;

    private DiskCache(File cache_file) {
        try {
            initializePageCache(cache_file);
            this.cache_file = cache_file;
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
    public PageProxyCursor getCursor(long id, int lockType) throws IOException {
        return new BasicPageCursor(this, id, lockType);
        //return new ZLIBPageCursor(singleInstance, id, lockType);
    }

    public static DiskCache temporaryDiskCache(){
        return temporaryDiskCache(DEFAULT_CACHE_FILE_NAME);
    }

    public static DiskCache temporaryDiskCache(String filename){
        File cache_file = new File(filename);
        cache_file.deleteOnExit();
        return new DiskCache(cache_file);
    }

    public static DiskCache getDiskCacheWithFilename(String filename){
        File cache_file = new File(filename);
        cache_file.deleteOnExit();
        return new DiskCache(cache_file);
    }

    public static DiskCache persistentDiskCache(){
        return  persistentDiskCache(DEFAULT_CACHE_FILE_NAME);
    }

    public static DiskCache persistentDiskCache(String filename){
        return new DiskCache(new File(filename));
    }

    private void initializePageCache(File page_cache_file) throws IOException {
        fs = new DefaultFileSystemAbstraction();
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory(fs);
        //factory.setFileSystemAbstraction( fs );
        pageCache = new MuninnPageCache(factory, maxPages, pageCachePageSize, PageCacheMonitor.NULL);
        pagedFile = pageCache.map(page_cache_file, filePageSize);
    }

    public ByteBuffer readPage(NodeTree tree, long id) {
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
        return (int)(cache_file.length() / 1000000l);
    }

    public PagedFile getPagedFile(){
        return pagedFile;
    }

    public void shutdown() throws IOException {
        //System.out.println(this.cache_file);
        pagedFile.close();
        pageCache.close();
    }
}
