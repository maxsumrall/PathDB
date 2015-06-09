package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DiskCache {
    public static int PAGE_SIZE = 8192; //size of a single page, in bytes.
    protected final static String DEFAULT_CACHE_FILE_NAME = "cache.bin";
    protected int max_size_in_mb = 5120;
    protected int maxPages = max_size_in_mb * (1000000 / PAGE_SIZE);
    protected DefaultFileSystemAbstraction fs;
    protected MuninnPageCache pageCache;
    public transient PagedFile pagedFile;
    public File pageCacheFile;
    private final boolean COMPRESSION = true;

    private DiskCache(File pageCacheFile) {
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

    public static DiskCache temporaryDiskCache(){
        return temporaryDiskCache(DEFAULT_CACHE_FILE_NAME);
    }

    public static DiskCache temporaryDiskCache(String filename){
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

    public PageProxyCursor getCursor(long id, int lockType) throws IOException {
        if(COMPRESSION){
            //return new LZ4PageCursor(this, id, lockType);
            return new CompressedPageCursor(this, id, lockType);
        }
        else{
            return new BasicPageCursor(this, id, lockType);
        }
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
