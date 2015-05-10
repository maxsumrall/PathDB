package bptree;

import bptree.impl.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static bptree.LabelsAndPathsGenerator.exampleLabelPaths;
import static bptree.LabelsAndPathsGenerator.exampleSequentialKeys;

/**
 * Created by max on 4/28/15.
 */
public class NodeProxyTest {

    Index index;
    ArrayList labelPaths;
    PathIndexImpl pindex;
    NodeTree proxy;
    @Before
    public void initializeIndex() throws IOException {
        labelPaths = exampleLabelPaths(20,2);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();

        int number_of_keys_to_insert = 2000;
        ArrayList<Key> keys = exampleSequentialKeys(labelPaths, number_of_keys_to_insert);
        for(Key key: keys){
            index.insert(key);
        }
        pindex = ((PathIndexImpl)index);
        proxy = new NodeTree(pindex.tree.rootNodePageID, pindex.tree.nodeKeeper.diskCache.getPagedFile());
    }

    public long[] toPrimitive(Long[] key){
        long[] keyprim = new long[key.length];
        for(int i = 0; i < key.length; i++){keyprim[i] = key[i];}
        return keyprim;
    }

    @Test
    public void searchKeyInternalNodeSameKeyLength() throws IOException {
        InternalNode node = (InternalNode) pindex.tree.getNode(pindex.tree.rootNodePageID);
        for(Long[] key : node.keys){
            long[] keyprim = new long[key.length];
            for(int i = 0; i < key.length; i++){keyprim[i] = key[i];}
            int nodeSearch = node.search(key);
            int proxySearch = NodeSearch.search(pindex.tree.rootNodePageID, keyprim)[0];
            assert(nodeSearch == proxySearch);
        }
    }

    @Test
    public void searchKeyInternalNodeDifferentKeyLength() throws IOException {
        InternalNode node = (InternalNode) pindex.tree.getNode(pindex.tree.rootNodePageID);
        Long[] newKey = pindex.buildComposedKey(new KeyImpl(new Long[]{9l, 9l, 9l}, new Long[]{6l, 6l, 6l, 6l}));
        assert(!node.hasSameKeyLength(newKey)); //New key is not the same length
        node.keys.add(newKey);
        node.children.add(999l);
        node.determineIfKeysAreSameLength();
        pindex.tree.writeNodeToPage(node);
        for(Long[] key : node.keys){
            int nodeSearch = node.search(key);
            int proxySearch = NodeSearch.search(pindex.tree.rootNodePageID, toPrimitive(key))[0];
            assert(nodeSearch == proxySearch);
        }
    }

    @Test
    public void searchKeyLeafNodeSameKeyLength() throws IOException {
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
        for(Long[] key : node.keys){
            int nodeSearch = node.search(key);
            int proxySearch = NodeSearch.search(node.id, toPrimitive(key))[0];
            assert(nodeSearch == proxySearch);
        }
    }

    @Test
    public void searchKeyLeafNodeDifferentKeyLength() throws IOException{
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
        node.keys = new ArrayList<>(node.keys.subList(0, 20));
        Long[] newKey = pindex.buildComposedKey(new KeyImpl(new Long[]{9l, 9l, 9l}, new Long[]{6l, 6l, 6l, 6l}));
        assert(!node.hasSameKeyLength(newKey)); //New key is not the same length
        node.insert(newKey); //insert a new longer than normal key
        for(Long[] key : node.keys){
            int nodeSearch = node.search(key);
            int proxySearch = NodeSearch.search(node.id, toPrimitive(key))[0];
            assert(nodeSearch == proxySearch);
        }
    }


    @Test
    public void getChildIdTest() throws IOException{
        InternalNode node = (InternalNode) pindex.tree.getNode(pindex.tree.rootNodePageID);
        for(Long[] key : node.keys){
            long[] keyprimitive = new long[key.length];
            for(int i = 0; i < key.length; i++){keyprimitive[i] = key[i];}
            int nodeSearch = node.search(key);
            int proxySearch = NodeSearch.search(pindex.tree.rootNodePageID, keyprimitive)[0];
            assert(node.children.get(nodeSearch) == proxy.getChildIdAtIndex(pindex.tree.rootNodePageID, proxySearch));
        }
    }

    @Test
    public void leafNodeSameLengthKeysByteSize() throws IOException{
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
        int sizeProxy = NodeSize.leafNodeByteSize(node.id, toPrimitive(node.keys.get(0)));
        int sizeNode = node.byteRepresentationSize(node.keys.get(0));
        assert(sizeProxy == sizeNode);
    }

    @Test
    public void leafNodeDifferentLengthKeysByteSize() throws IOException{
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
        node.keys = new ArrayList<>(node.keys.subList(0, 20));
        Long[] newKey = pindex.buildComposedKey(new KeyImpl(new Long[]{9l, 9l, 9l}, new Long[]{6l, 6l, 6l, 6l}));
        assert(!node.hasSameKeyLength(newKey)); //New key is not the same length
        node.insert(newKey); //insert a new longer than normal key
        int sizeProxy = NodeSize.leafNodeByteSize(node.id, toPrimitive(newKey));
        int sizeNode = node.byteRepresentationSize(newKey);
        assert(sizeProxy == sizeNode);
    }

    @Test
    public void internalNodeSameLengthKeysByteSize() throws IOException{
        InternalNode node = (InternalNode) pindex.tree.getNode(pindex.tree.rootNodePageID);
        int sizeProxy = NodeSize.internalNodeByteSize(node.id, toPrimitive(node.keys.get(0)));
        int sizeNode = node.byteRepresentationSize(node.keys.get(0));
        assert(sizeProxy == sizeNode);
    }

    @Test
    public void internalNodeDifferentLengthKeysByteSize() throws IOException{
        InternalNode node = (InternalNode) pindex.tree.getNode(pindex.tree.rootNodePageID);
        Long[] newKey = pindex.buildComposedKey(new KeyImpl(new Long[]{9l, 9l, 9l}, new Long[]{6l, 6l, 6l, 6l}));
        assert(!node.hasSameKeyLength(newKey)); //New key is not the same length
        node.keys.add(newKey);
        node.children.add(999l);
        node.determineIfKeysAreSameLength();
        pindex.tree.writeNodeToPage(node);
        int sizeProxy = NodeSize.internalNodeByteSize(node.id, toPrimitive(newKey));
        int sizeNode = node.byteRepresentationSize(newKey);
        assert(sizeProxy == sizeNode);
    }

    @Test
    public void internalNodeSameLengthKeyInsertKeyAndChildNoSplit() throws IOException {
        InternalNode nodeA = (InternalNode) pindex.tree.createInternalNode();
        InternalNode nodeB = (InternalNode) pindex.tree.createInternalNode();

        LinkedList<Long[]> keys = new LinkedList<>();
        for(long i = 0; i < 204l; i++){
            keys.add(new Long[]{i,i,i,i});
        }
        SplitResult resultA = null;
        SplitResult resultB = null;
        SplitResult inserter = new SplitResult();
        for(Long[] key : keys){
            inserter.key = key;
            inserter.left = key[0];
            inserter.right = key[0];
            resultA = nodeA.insertFromResult(inserter);
            resultB = NodeInsertion.addKeyAndChildToInternalNode(nodeB.id, toPrimitive(key), key[0]);
            assert(resultA == resultB);
        }
        keys.add(new Long[]{255l,255l,255l,255l});
        inserter.key = keys.getLast();
        inserter.left = keys.getLast()[0];
        inserter.right = keys.getLast()[0];
        resultA = nodeA.insertFromResult(inserter);
        resultB = NodeInsertion.addKeyAndChildToInternalNode(nodeB.id, toPrimitive(keys.getLast()), keys.getLast()[0]);
       // assert(Arrays.equals(toPrimitive(resultA.key), resultB.primkey));
    }
    @Test
    public void internalNodeSameLengthKeyInsertKeyAndChildSplit(){

    }

    @Test
    public void internalNodeDifferentLengthKeyInsertKeyAndChildNoSplit(){

    }
    @Test
    public void internalNodeDifferentLengthKeyInsertKeyAndChildSplit(){

    }

    @Test
    public void leafNodeSameLengthKeyInsertKeyNoSplit() throws IOException {
        LeafNode nodeA = (LeafNode) pindex.tree.createLeafNode();
        LeafNode nodeB = (LeafNode) pindex.tree.createLeafNode();

        LinkedList<Long[]> keys = new LinkedList<>();
        for(long i = 0; i < 10l; i++){
            keys.add(new Long[]{i,i,i,i});
        }
        Collections.shuffle(keys, new Random());
        for(Long[] key : keys){
            nodeA.insert(key);
            NodeInsertion.addKeyToLeafNode(nodeB.id, toPrimitive(key));
        }

        for(Long[] key : keys){
            assert(nodeA.search(key) == NodeSearch.search(nodeB.id, toPrimitive(key))[0]);
        }
    }
    @Test
    public void leafNodeSameLengthKeyInsertKeySplit() throws IOException {
        LeafNode nodeA = (LeafNode) pindex.tree.createLeafNode();
        LeafNode nodeB = (LeafNode) pindex.tree.createLeafNode();

        LinkedList<Long[]> keys = new LinkedList<>();
        for(long i = 0; i < 255l; i++){
            keys.add(new Long[]{i,i,i,i});
        }
        SplitResult resultA = null;
        SplitResult resultB = null;
        for(Long[] key : keys){
            resultA = nodeA.insert(key);
            resultB = NodeInsertion.addKeyToLeafNode(nodeB.id, toPrimitive(key));
            assert(resultA == resultB);
        }
        keys.add(new Long[]{255l,255l,255l,255l});
        resultA = nodeA.insert(keys.getLast());
        resultB = NodeInsertion.addKeyToLeafNode(nodeB.id, toPrimitive(keys.getLast()));
        assert(Arrays.equals(toPrimitive(resultA.key), resultB.primkey));
    }

    @Test
    public void leafNodeDifferentLengthKeyInsertKeyNoSplit() throws IOException {
        LeafNode nodeA = (LeafNode) pindex.tree.createLeafNode();
        LeafNode nodeB = (LeafNode) pindex.tree.createLeafNode();

        LinkedList<Long[]> keys = new LinkedList<>();
        for(long i = 0; i < 10l; i++){
            keys.add(new Long[]{i*100,i,i,i,i});
        }
        for(long i = 0; i < 192l; i++){
            keys.add(new Long[]{i,i,i,i});
        }
        SplitResult resultA = null;
        SplitResult resultB = null;
        for(Long[] key : keys){
            resultA = nodeA.insert(key);
            resultB = NodeInsertion.addKeyToLeafNode(nodeB.id, toPrimitive(key));
            assert(resultA == resultB);
        }
        keys.add(new Long[]{255l,255l,255l,255l});
        resultA = nodeA.insert(keys.getLast());
        resultB = NodeInsertion.addKeyToLeafNode(nodeB.id, toPrimitive(keys.getLast()));
        assert(Arrays.equals(toPrimitive(resultA.key), resultB.primkey));
    }
    @Test
    public void leafNodeDifferentLengthKeyInsertKeySplit() throws IOException {
        //Todo this is not inducing a split
        LeafNode nodeA = (LeafNode) pindex.tree.createLeafNode();
        LeafNode nodeB = (LeafNode) pindex.tree.createLeafNode();

        LinkedList<Long[]> keys = new LinkedList<>();
        for(long i = 0; i < 10l; i++){
            keys.add(new Long[]{i,i,i,i});
        }
        for(long i = 0; i < 10l; i++){
            keys.add(new Long[]{i*100,i,i,i,i});
        }
        Collections.shuffle(keys, new Random());
        for(Long[] key : keys){
            nodeA.insert(key);
            NodeInsertion.addKeyToLeafNode(nodeB.id, toPrimitive(key));
        }

        for(Long[] key : keys){
            int resultA = nodeA.search(key);
            int resultB = NodeSearch.search(nodeB.id, toPrimitive(key))[0];
            assert(resultA == resultB);
        }
    }

    @Test
    public void leafNodeSameLengthKeyDeleteNoCollapse() throws IOException {

    }

    @Test
    public void leafNodeSameLengthKeyDeleteCollapse() throws IOException {

    }

    @Test
    public void leafNodeDifferentLengthKeyDeleteNoCollapse() throws IOException {

    }

    @Test
    public void leafNodeDifferentLengthKeyDeleteCollapse() throws IOException {

    }

    @Test
    public void internalNodeSameLengthKeyDeleteNoCollapse() throws IOException {

    }

    @Test
    public void internalNodeSameLengthKeyDeleteCollapse() throws IOException {

    }

    @Test
    public void internalNodeDifferentLengthKeyDeleteNoCollapse() throws IOException {

    }

    @Test
    public void internalNodeDifferentLengthKeyDeleteCollapse() throws IOException {

    }

}