package NeoIntegration;

import bptree.BulkLoadDataSource;
import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

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

