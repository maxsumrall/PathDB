package bptree.impl;

import bptree.Cursor;
import bptree.RemoveResult;
import org.neo4j.io.pagecache.PageCursor;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by max on 2/10/15.
 */
public class Tree implements Closeable, Serializable, ObjectInputValidation {

    protected static String DEFAULT_TREE_FILE_NAME = "tree.bin";
    protected String tree_filename;
    private AvailablePageIdPool idPool;
    public long rootNodePageID;
    public NodeKeeper nodeKeeper;
    private LinkedList<Long> lastTrace = new LinkedList<>();
    private int keySetSize = 0;
    public NodeTree proxy;

    /**
     * Constructs a new Tree object
     * @param file The file where the tree should be based.
     * @throws IOException
     */
    private Tree(String tree_filename, DiskCache diskCache) throws IOException {
        this.nodeKeeper = new NodeKeeper(this, diskCache);
        this.tree_filename = tree_filename;
        idPool = new AvailablePageIdPool(nodeKeeper.diskCache.getMaxNumberOfPages());
        Node rootNode = createLeafNode();
        rootNodePageID = rootNode.id;
        proxy = new NodeTree(rootNodePageID, diskCache.pagedFile);
    }
    public static Tree initializeTemporaryNewTree() throws IOException {
        return initializeNewTree(DEFAULT_TREE_FILE_NAME, DiskCache.temporaryDiskCache()); //Delete on exit
    }
    public static Tree initializePersistentNewTree() throws IOException {
        return initializeNewTree(DEFAULT_TREE_FILE_NAME, DiskCache.persistentDiskCache()); //Delete on exit
    }

    public static Tree initializeNewTree(String tree_filename, DiskCache diskCache) throws IOException {
        return new Tree(tree_filename, diskCache);
    }

    public static Tree loadTreeFromFile(String tree_location) throws IOException {
        FileInputStream fis = new FileInputStream(tree_location);
        byte[] bytes = new byte[fis.available()];
        fis.read(bytes);
        Tree tree;
        try {
            tree = deserialize(bytes);
        }
        catch (InvalidClassException e){
            throw new InvalidClassException("Invalid object found at file: " + tree_location);
        }
        return tree;
    }


    /**
     * Gets a node.
     * @param id of the node, also representing it's page id.
     * @return a reference to this node.
     */
    public Node getNode(long id) throws IOException {
        //updateLogger(id);
        if(id < 0){throw new IOException("Invalid Node ID");}
        //if(idPool.isNodeIdInFreePool(id)){ //this is slow.
        //    throw new IOException("Invalid Node ID: Attempting to read page ID of free'd page/node");
        //}
        return nodeKeeper.getNode(id);
    }

    public Node getNode(PageCursor cursor) throws IOException {
        return nodeKeeper.getNode(cursor);
    }

    private void updateLogger(Long id){
        if(id.equals(rootNodePageID))
            lastTrace.clear();
        lastTrace.add(id);
    }

    public long getNewID(){
        return idPool.acquireId();
    }

    public void releaseNodeId(Long id){
        idPool.releaseId(id);
    }

    public LeafNode createLeafNode() throws IOException {
        return new LeafNode(this, getNewID());
    }

    public LeafNode createLeafNode(ArrayList<Long[]> keys, Long followingNodeId, Long precedingNodeId) throws IOException {
        return new LeafNode(this, getNewID(), keys, followingNodeId, precedingNodeId);
    }


    public InternalNode createInternalNode() throws IOException {
        try {
            return new InternalNode(this, getNewID());
        }
        catch(IOException e){
            //TODO log error here
            throw new IOException("Error creating new Internal Node");
        }
    }
    public InternalNode createInternalNode(ArrayList<Long[]> keys, ArrayList<Long> children) throws IOException {
        try {
            return new InternalNode(this, getNewID(), keys, children);
        }
        catch(IOException e){
            //TODO log error here
            throw new IOException("Error creating new Internal Node");
        }

    }

    /**
     * When a node is changed, it will call this function to have itself written to the disk.
     * This better controls the instantiation of PageCursors,
     * limiting it to only being instantiated in the Tree.
     * @param node the Node which would like to be serialized.
     */
    public void writeNodeToPage(Node node){
        nodeKeeper.writeNodeToPage(node);
        //this.diskCache.writePage(node.id, node.serialize().array());
    }

    /**
     * Returns a Cursor at the first relevant result given a key.
     * @param key
     * @return
     * @throws IOException
     */
    public Cursor find(Long[] key) throws IOException {
        try {
            return getNode(rootNodePageID).find(key);
        }
        catch(IOException e){
            return new NullCursorImpl(); //TODO consider what to do when a find() calls bad nodes. This means tree is broken.
        }
    }

    public RemoveResult remove(Long[] key) {
        RemoveResult result;
        try {
            result = attemptRemoval(key);
            keySetSize = keySetSize - result.getN();
        }
        catch(IOException e){
            result = new RemoveResultImpl();
        }
        return result;
    }
    private RemoveResult attemptRemoval(Long[] key) throws IOException{
        RemoveResult result = getNode(rootNodePageID).remove(key);

        if(result.containsNodesWhichRequireAttention()){ //Root node collapsed.
            assert(result.getMergedNodes().size() == 1);
            Node root = getNode(rootNodePageID);
            assert(root.keys.size() == 0);
            if(root instanceof InternalNode){
                assert(((InternalNode) root).children.size() == 0);
            }
            releaseNodeId(root.id);
            Node newRoot = createLeafNode();
            rootNodePageID = newRoot.id;
        }
        return result;
    }

    /**
     * TODO return a cursor where this was inserted to do bulk insertion later, maybe.
     * Get the root block and call insert on it.
     * If the root returns a split result, make a new block and set it as the root.
     * @param key
     */
    public void insert(Long[] key){
        try {
            attemptInsertion(key);
            keySetSize++;
        }
        catch (IOException e){
            //TODO log error here
        }
    }
    private void attemptInsertion(Long[] key) throws IOException{
        SplitResult result = getNode(rootNodePageID).insert(key);

        if (result != null){ //Root block split.
            ArrayList<Long[]> keys = new ArrayList<>();
            ArrayList<Long> children = new ArrayList<>();
            keys.add(result.key);
            children.add(result.left);
            children.add(result.right);
            InternalNode newRoot = createInternalNode(keys, children);
            rootNodePageID = newRoot.id;
        }
    }

    public void proxyInsertion(long[] key) throws IOException{
        SplitResult result = NodeInsertion.insert(key);

        if(result != null){
            InternalNode newRoot = createInternalNode();
            NodeTree.rootNodeId = newRoot.id;
            NodeTree.newRoot(result.left, result.right, result.primkey);
        }
    }

    public ProxyCursor proxyFind(long[] key) throws IOException{
        return NodeSearch.find(key);
    }

    public void proxyRemove(long[] key) throws IOException{
        RemoveResultProxy result = NodeDeletion.remove(key);
        if(result != null){
            LeafNode newRoot = createLeafNode();
            NodeTree.rootNodeId = newRoot.id;
        }
    }

    public int getCountOfMatchingKeys(Long[] search_key) throws IOException {
        int count = 0;
        LeafNode leaf = getFirstLeaf();
        for(Long[] key : leaf.keys){
            if (Node.keyComparator.validPrefix(search_key, key)){
                count++;
            }
        }
        while(!leaf.followingNodeId.equals(-1l)){
            leaf = (LeafNode)getNode(leaf.followingNodeId);
            for(Long[] key : leaf.keys){
                if (Node.keyComparator.validPrefix(search_key, key)){
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Returns the number of keys stored in this tree.
     * @return number of keys in this tree.
     */
    public int getKeySetSize(){
        return keySetSize;
    }

    /**
     *Forces the tree to read each leaf node and count the number of keys in them.
     * For Debugging Purposes.
     * @return number of keys in this tree.
     */
    public int getKeySetSizeForceCheck(){
        int count = 0;
        try {
            LeafNode leaf = getFirstLeaf();
            count += leaf.keys.size();
            while(!leaf.followingNodeId.equals(-1l)){
                leaf = (LeafNode)getNode(leaf.followingNodeId);
                count += leaf.keys.size();
            }
        }
        catch(IOException e){
            //TODO Log this
        }
        return count;
    }

    /**
     * Finds the first leaf in the tree.
     * Can be used for doing a complete walk along leaf node linked lists.
     * @return
     */
    public LeafNode getFirstLeaf() throws IOException {
        Node currentNode = getNode(rootNodePageID);
        while(currentNode instanceof InternalNode){
            currentNode = getNode(((InternalNode) currentNode).children.get(0));
        }
        assert(currentNode.precedingNodeId.equals(-1l));
        return (LeafNode) currentNode;
    }
    public int getDepthOfTree() throws IOException{
        Node currentNode = getNode(proxy.rootNodeId);
        int  depth = 1;
        while(currentNode instanceof InternalNode){
            depth++;
            currentNode = getNode(((InternalNode) currentNode).children.get(0));
        }
        depth++;
        assert(currentNode.precedingNodeId.equals(-1l));
        return depth;
    }

    public int diskCacheSize(){
        return nodeKeeper.disk_cache_size();
    }

    public void shutdown() throws IOException {
        nodeKeeper.shutdown();
    }

    /**
     * Save the state of this tree to the file to load later.
     */
    public void close() throws IOException {
        FileOutputStream fos = new FileOutputStream(tree_filename);
        fos.write(serialize(this));
        fos.close();
    }

    /**
     * Serialize this Tree object
     * @param o
     * @return
     * @throws IOException
     */
    private static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.flush();
        oos.close();
        return baos.toByteArray();
    }

    /**
     * Deserialize this Tree object
     * @param bytes
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private static Tree deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Tree tree = null;
        try{
            Object o = ois.readObject();
            ois.close();
            if (o instanceof Tree){
                tree = (Tree) o;
            }
        }
        catch(ClassNotFoundException e){
            throw new InvalidClassException("Attempted to read invalid Tree object from byte array");
        }
        return tree;
    }
    /**
     * Methods implementing serialization of Tree
     * @throws InvalidObjectException
     */
    @Override
    public void validateObject() throws InvalidObjectException {

    }

    /**
     * Methods implementing serialization of Tree
     * @param out
     * @throws IOException
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Method implementing serialization of Tree
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.registerValidation(this, 0);
        in.defaultReadObject();
    }
}
