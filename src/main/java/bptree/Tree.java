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
        rootNodePageID = rootNode.nodeID;
    }

    protected Tree initalizeNewTree() throws IOException {
        return initializeNewTree(DEFAULT_CACHE_FILE_NAME, DEFAULT_TREE_FILE_NAME, true); //Delete on exit
    }

    protected Tree initializeNewTree(String cache_file_location, String tree_file_location, boolean deleteOnExit) throws IOException {
        return new Tree(cache_file_location, tree_file_location, deleteOnExit);
    }

    protected Tree loadTreeFromFile(String tree_location) throws IOException {
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
     * Gets a block. If the block is already loaded into memory, then we can imemdately return it.
     * If the block is not found, then we need to load it from disk.
     * @param id
     * @return
     */
    public Node getNode(long id) throws IOException {
        return loadNodeFromDisk(id);
    }

    /**
     * Returns a cursor on the page specified by id
     * @param id specific page to set the cursor to initially.
     * @return a cursor on that page.
     */


    /**
     * Finds the block on the disk, loads it and stores it into the memory.
     * TODO should remove a block from memory or overwrite a block in memory.
     * @param id of the block to load and return
     * @return The block reterived from disk
     */
    private Node loadNodeFromDisk(Long id) throws IOException {
        try (PageCursor cursor = pagedFile.io(id, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    Node node;
                    if(Node.isLeafBlock(cursor)){
                        node = new LeafNode(cursor, this, id);
                    }
                    else{
                        //block = new IBlock(cursor);
                        node = null;
                    }
                    return node;
                }
                while (cursor.shouldRetry());
            }
        }
        return null;
    }

    public long getNewID(){
        return nextAvailablePageID++;
    }

    public LeafNode createLeafNode(){
        long newBlockID = getNewID();
        //blocks.put(newBlockID, new LeafNode(this, newBlockID));
        return (LeafNode)blocks.get(newBlockID);
    }
    public InternalNode createInternalNode(){
        long newBlockID = getNewID();
        //blocks.put(newBlockID, new InternalNode(this, newBlockID));
        return (InternalNode)blocks.get(newBlockID);
    }

    public IndexCursor find(Long[] key){

    }

    /**
     * Get the root block and call insert on it.
     * If the root returns a split result, make a new block and set it as the root.
     * @param key
     */
    public void insert(Long[] key){
        Node.SplitResult result = bm.rootNode.insert(key);
        if (result != null){ //Root block split.
            InternalNode newRoot = bm.createIBlock();
            newRoot.num = 1;
            newRoot.keys[0] = result.key;
            newRoot.children[0] = result.left;
            newRoot.children[1] = result.right;
            bm.rootNode = newRoot;
        }
    }

    /**
     * Save the state of the index to the file to load later.
     */
    public void close() throws IOException {
        FileOutputStream fos = new FileOutputStream(tree_filename);
        fos.write(serialize(this));
        fos.close();
    }

    /**
     * Serialize this object
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
     * Deserialize this object
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
     * Methods implementing serialization
     * @throws InvalidObjectException
     */
    @Override
    public void validateObject() throws InvalidObjectException {

    }

    /**
     * Methods implementing serialization
     * @param out
     * @throws IOException
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Method implementing serialization
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.registerValidation(this, 0);
        in.defaultReadObject();
    }

    private class IndexCursor extends Cursor {

    }
}
