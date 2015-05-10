package bptree;

import bptree.impl.DiskCache;
import bptree.impl.NodeBulkLoader;
import bptree.impl.Tree;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Created by max on 5/8/15.
 */
public class NodeBulkLoaderTest {
    static Tree tree;

    @Test
    public void loadTest() throws IOException {

        DiskCache disk = DiskCache.temporaryDiskCache();//TODO its bad that I make an instance of this but it has static methods. Need to refactor DiskCache.
        tree = Tree.initializeNewTree("tmp_tree_yo.dat", disk);
        int numberOfPages = 1000; //100000 pages should roughly equal 20mil keys;
        SimpleDataGenerator dataGenerator = new SimpleDataGenerator(numberOfPages);
        NodeBulkLoader bulkLoader = new NodeBulkLoader(dataGenerator, DiskCache.pagedFile);
        long root = bulkLoader.run();
        System.out.println("Done: " + root);
    }

    class SimpleDataGenerator implements BulkLoadDataSource{

        public int numberOfPages;
        public int currentPage = 0;
        long currentKey = 0;
        int keyLength = 4;
        byte[] keysInPage = new byte[NodeBulkLoader.MAX_PAIRS * NodeBulkLoader.KEY_LENGTH * 8];
        ByteBuffer bb = ByteBuffer.wrap(keysInPage);
        LongBuffer lb = bb.asLongBuffer();
        public SimpleDataGenerator(int numberOfPages){
            this.numberOfPages = numberOfPages;
        }

        @Override
        public byte[] nextPage() {
            lb.position(0);
            long[] key = new long[keyLength];
            for (long j = 0; j < NodeBulkLoader.MAX_PAIRS; j++) {
                for (int k = 0; k < key.length; k++) {
                    key[k] = currentKey;
                }
                lb.put(key);
                currentKey++;
            }
            currentPage++;
            return keysInPage;
        }


        @Override
        public boolean hasNext() {
            return currentPage < numberOfPages;
        }
    }
}