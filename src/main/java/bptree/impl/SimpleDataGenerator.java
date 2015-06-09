package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;


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