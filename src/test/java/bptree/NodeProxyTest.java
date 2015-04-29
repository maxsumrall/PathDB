package bptree;

import bptree.impl.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

import static bptree.LabelsAndPathsGenerator.exampleLabelPaths;
import static bptree.LabelsAndPathsGenerator.exampleSequentialKeys;

/**
 * Created by max on 4/28/15.
 */
public class NodeProxyTest {

    Index index;
    ArrayList labelPaths;
    PathIndexImpl pindex;
    NodeProxy proxy;
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
        proxy = new NodeProxy();
        proxy.setPagedFile(pindex.tree.nodeKeeper.diskCache.getPagedFile());
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
            int proxySearch = proxy.search(pindex.tree.rootNodePageID, keyprim)[0];
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
            int proxySearch = proxy.search(pindex.tree.rootNodePageID, toPrimitive(key))[0];
            assert(nodeSearch == proxySearch);
        }
    }

    @Test
    public void searchKeyLeafNodeSameKeyLength() throws IOException {
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
        for(Long[] key : node.keys){
            int nodeSearch = node.search(key);
            int proxySearch = proxy.search(node.id, toPrimitive(key))[0];
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
            int proxySearch = proxy.search(node.id, toPrimitive(key))[0];
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
            int proxySearch = proxy.search(pindex.tree.rootNodePageID, keyprimitive)[0];
            assert(node.children.get(nodeSearch) == proxy.getChildIdAtIndex(pindex.tree.rootNodePageID, proxySearch));
        }
    }

    @Test
    public void leafNodeSameLengthKeysByteSize() throws IOException{
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
        int sizeProxy = proxy.leafNodeByteSize(node.id, toPrimitive(node.keys.get(0)));
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
        int sizeProxy = proxy.leafNodeByteSize(node.id, toPrimitive(newKey));
        int sizeNode = node.byteRepresentationSize(newKey);
        assert(sizeProxy == sizeNode);
    }

    @Test
    public void internalNodeSameLengthKeysByteSize() throws IOException{
        InternalNode node = (InternalNode) pindex.tree.getNode(pindex.tree.rootNodePageID);
        int sizeProxy = proxy.internalNodeByteSize(node.id, toPrimitive(node.keys.get(0)));
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
        int sizeProxy = proxy.internalNodeByteSize(node.id, toPrimitive(newKey));
        int sizeNode = node.byteRepresentationSize(newKey);
        assert(sizeProxy == sizeNode);
    }

    @Test
    public void internalNodeSameLengthKeyInsertKeyAndChildNoSplit(){

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
            proxy.addKeyToLeafNode(nodeB.id, toPrimitive(key));
        }

        for(Long[] key : keys){
            assert(nodeA.search(key) == proxy.search(nodeB.id, toPrimitive(key))[0]);
        }
    }
    @Test
    public void leafNodeSameLengthKeyInsertKeySplit() throws IOException {
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
    }

    @Test
    public void leafNodeDifferentLengthKeyInsertKeyNoSplit() throws IOException {
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
    }
    @Test
    public void leafNodeDifferentLengthKeyInsertKeySplit() throws IOException {
        LeafNode node = (LeafNode) pindex.tree.getFirstLeaf();
    }
}