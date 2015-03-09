package bptree;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

import java.io.*;

/**
 * Created by max on 2/10/15.
 */
public class Tree implements Closeable, Serializable, ObjectInputValidation {

    protected static String DEFAULT_TREE_FILE_NAME = "tree.bin";
    protected static String DEFAULT_CACHE_FILE_NAME = "cache.bin";
    protected static int PAGE_SIZE = bptree.Utils.getIdealBlockSize();
    protected String tree_filename;
    protected String cache_filename;
    private long nextAvailablePageID = 1l;
    protected long rootNodePageID;
    protected int recordSize = 9; //TODO What is this?
    protected int maxPages = 20; //TODO How big should this be?
    protected int pageCachePageSize = 32;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;
    protected transient DefaultFileSystemAbstraction fs;
    protected transient MuninnPageCache pageCache;
    protected transient PagedFile pagedFile;

    /**
     * Constructs a new Tree object
     * @param file The file where the tree should be based.
     * @throws IOException
     */
    private Tree(String cache_filename, String tree_filename, boolean delete_on_exit) throws IOException {
        File page_cache_file = new File(cache_filename);
        File tree_file = new File(tree_filename);
        if(delete_on_exit){
            page_cache_file.deleteOnExit();
            tree_file.deleteOnExit();
        }
        initializePageCache(page_cache_file);
        Node rootNode = createLeafNode(); // Initialize the tree with only one block for now, a leaf block.
        rootNodePageID = rootNode.id;
    }

    public static Tree initializeNewTree() throws IOException {
        return initializeNewTree(DEFAULT_CACHE_FILE_NAME, DEFAULT_TREE_FILE_NAME, true); //Delete on exit
    }

    public static Tree initializeNewTree(String cache_file_location, String tree_file_location, boolean deleteOnExit) throws IOException {
        return new Tree(cache_file_location, tree_file_location, deleteOnExit);
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
        tree.initializePageCache(new File(tree.cache_filename));
        return tree;
    }

    private void initializePageCache(File page_cache_file) throws IOException {
        fs = new DefaultFileSystemAbstraction();
        pageCache = new MuninnPageCache(fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL);
        pagedFile = pageCache.map(page_cache_file, filePageSize);
    }

    /**
     * Gets a node.
     * @param id of the node, also representing it's page id.
     * @return a reference to this node.
     */
    public Node getNode(long id) throws IOException {
        Node node = null;
        try (PageCursor cursor = pagedFile.io(id, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    if(Node.parseHeaderForNodeTypeFlag(cursor) == Node.LEAF_FLAG){
                        node = new LeafNode(cursor, this, id);
                    }
                    else{
                        node = new InternalNode(cursor, this, id);
                    }
                }
                while (cursor.shouldRetry());
            }
        }
        return node;
    }

    public long getNewID(){
        return nextAvailablePageID++;
    }

    public LeafNode createLeafNode(){
        return new LeafNode(this, getNewID());
    }
    public InternalNode createInternalNode(){
        return new InternalNode(this, getNewID());
    }

    /**
     * When a node is changed, it will call this function to have itself written to the disk.
     * This better controls the instantiation of PageCursors,
     * limiting it to only being instantiated in the Tree.
     * @param node the Node which would like to be serialized.
     */
    public void writeNodeToPage(Node node) throws IOException {
        try (PageCursor cursor = pagedFile.io(node.id, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    node.serialize(cursor);
                }
                while (cursor.shouldRetry());
            }
        }
    }

    /**
     * Returns a single result given a key.
     * @param key
     * @return
     * @throws IOException
     */
    public Long[] find(Long[] key) throws IOException {
        return getNode(rootNodePageID).find(key);

    }

    /**
     * TODO return a cursor where this was inserted to do bulk insertion later, maybe.
     * Get the root block and call insert on it.
     * If the root returns a split result, make a new block and set it as the root.
     * @param key
     */
    public void insert(Long[] key) throws IOException {
        Node.SplitResult result = getNode(rootNodePageID).insert(key);
        if (result != null){ //Root block split.
            InternalNode newRoot = createInternalNode();
            newRoot.keys.add(result.key);
            newRoot.children.add(result.left);
            newRoot.children.add(result.right);
            rootNodePageID = newRoot.id;
        }
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

    /*private class IndexCursor extends Cursor {

    }*/
}
