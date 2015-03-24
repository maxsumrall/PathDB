package bptree;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class InternalNodeTest {
    private PathIndex index;
    private File indexFile;
    private ArrayList<Long[]> labelPaths;

    public ArrayList<Long[]> exampleLabelPaths(int number_of_paths, int k){
        Random random = new Random();
        ArrayList<Long[]> labelPaths = new ArrayList<>();
        for(int i = 0; i < number_of_paths; i++){
            labelPaths.add(new Long[k]);
            for(int j = 0; j < k; j++){
                labelPaths.get(i)[j] = Math.abs(random.nextLong());
            }
        }
        return labelPaths;
    }

    @Before
    public void initializeIndex() throws IOException {
        labelPaths = exampleLabelPaths(20,2);
        index = PathIndex.temporaryPathIndex()
                .setKValues(2, 2)
                .buildLabelPathMapping(labelPaths)
                .setSignatures(PathIndex.defaultSignatures(2,2));
        Assert.assertTrue(index.ready());
    }

    @Test
    public void insertSplitTest() throws IOException {
        Tree tree = index.tree;
        InternalNode node = tree.createInternalNode();
        for (int i = 220; i > 0; i--){
            node = (InternalNode)tree.getNode(node.id);
            Long[] key = new Long[]{(long) i,(long) i, (long) i, (long) i};
            SplitResult result = node.insertFromResult(new SplitResult(key, (long) i, (long) i+1));
            if (result != null){
                //PathIndexTest.printNode(tree.getNode(result.left));
                //PathIndexTest.printNode(tree.getNode(result.right));
            }
        }

    }
    @Test
    public void insertSplitVariousLengthTest() throws IOException {
        Tree tree = index.tree;
        Random random = new Random();
        InternalNode node = tree.createInternalNode();
        for (int i = 220; i > 0; i--){
            node = (InternalNode)tree.getNode(node.id);
            Long[] key = new Long[random.nextInt(3)+2];
            for(int j = 0; j < key.length; j ++){
                key[j] = (long)i;
            }
            System.out.println("Children size: " + node.children.size());
            int total_longs_key = 0;
            for(Long[] k : node.keys){
                total_longs_key += k.length;
            }
            System.out.println("Keys length: " + node.keys.size());
            System.out.println("Keys sum: " + total_longs_key);
            System.out.println("Byte rep size: " + node.byte_representation_size(key));

            SplitResult result = node.insertFromResult(new SplitResult(key, (long) i, (long) i+1));
            if (result != null){
                //printNode(tree.getNode(result.left));
                //printNode(tree.getNode(result.right));
            }
        }

    }

}