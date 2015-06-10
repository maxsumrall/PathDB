package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeHeader;
import bptree.impl.NodeTree;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * Created by max on 6/10/15.
 */
public class DebugCounterPathIDs {

    public static void main(String[] args) throws IOException {
        new DebugCounterPathIDs();
    }

    public DebugCounterPathIDs() throws IOException {
/*
        DiskCache k3Bigdisk = DiskCache.persistentDiskCache("LUBM50Index/K4Cleverlubm50Index.db", false);
        DiskCache k3Smalldisk = DiskCache.persistentDiskCache("LUBM50IndexCompressed/K4Cleverlubm50Index.db", true);

        System.out.println("Big");
        LinkedHashMap<Long, Long> statsA = run(k3Bigdisk);
        System.out.println("Small");
        LinkedHashMap<Long, Long> statsB =run(k3Smalldisk);

        for(Long key : statsA.keySet()){
            System.out.println("Key: " + key + " Big: " + statsA.get(key) + " small: " + statsB.get(key));
        }
        */
        brutesearch(new long[]{57, 36983, 0 , 558097});

    }

    public void brutesearch(long[] key) throws IOException {
        DiskCache disk = DiskCache.persistentDiskCache("K4Cleverlubm50Index.db", true);
        NodeTree index = new NodeTree(338335, disk);
        long currentPage = 0;
        try(PageProxyCursor cursor = disk.getCursor(currentPage, PagedFile.PF_SHARED_LOCK)){
            while(currentPage != -1){
                cursor.next(currentPage);
                if(!NodeHeader.isLeafNode(cursor))
                    break;
                currentPage = NodeHeader.getSiblingID(cursor);
                cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                for(int i = 0; i < NodeHeader.getNumberOfKeys(cursor); i++){
                    long pathID = cursor.getLong();
                    long nodeA = cursor.getLong();
                    long nodeB = cursor.getLong();
                    long nodeC = cursor.getLong();
                    if(pathID == key[0] && nodeA == key[1] && nodeB == key[2] && nodeC == key[3]){
                        System.out.println("Found: " + Arrays.toString(key));
                        System.exit(0);
                    }
                }
            }
        }
        System.out.println("FUCK!");
    }

    public LinkedHashMap<Long, Long> run(DiskCache disk) throws IOException {
        LinkedHashMap<Long, Long> count = new LinkedHashMap<>();
        long nextPage = 0;
        try(PageProxyCursor cursor = disk.getCursor(nextPage, PagedFile.PF_SHARED_LOCK)){
            int keyLength = NodeHeader.getKeyLength(cursor);
            while(nextPage != -1){
                cursor.next(nextPage);
                if(!NodeHeader.isLeafNode(cursor))
                    break;
                nextPage = NodeHeader.getSiblingID(cursor);
                cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                for(int i = 0; i < NodeHeader.getNumberOfKeys(cursor); i++){
                    Long pathID = cursor.getLong();
                    Long nodeA = cursor.getLong();
                    Long nodeB = cursor.getLong();
                    Long nodeC = cursor.getLong();
                    if(count.containsKey(pathID))
                        count.put(pathID, count.get(pathID) + 1);
                    else
                        count.put(pathID, 1l);
                }
            }
        }


    return count;
    }
}

