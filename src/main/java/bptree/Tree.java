package bptree;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

import java.io.File;
import java.io.IOException;

/**
 * Created by max on 2/10/15.
 */
public class Tree {

    public static String DEFAULT_TREE_FILE_NAME = "tree.bin";
    public static int PAGE_SIZE = bptree.Utils.getIdealBlockSize();
    protected long nextID = 0l;

    public long rootNodePageID;
    public Node rootNode;


    protected final File file = new File( "a" );
    protected int recordSize = 9; //TODO What is this?
    protected int maxPages = 20; //TODO How big should this be?
    protected int pageCachePageSize = 32;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;
    protected DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    MuninnPageCache pageCache;
    PagedFile pagedFile;

    public Tree() throws IOException {
        rootNode = createLBlock(); // Initialize the tree with only one block for now, a leaf block.
        pageCache = new MuninnPageCache(fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL);
        pagedFile = pageCache.map(file, filePageSize);

    }

    /**
     * Gets a block. If the block is already loaded into memory, then we can imemdately return it.
     * If the block is not found, then we need to load it from disk.
     * @param id
     * @return
     */
    public Node getBlock(long id){
        try {
            return loadBlockFromPageCache(id);
        } catch (IOException e) {
            return null; //This is bad TODO think of something better but for now it should not fail.
        }
    }

    /**
     * Finds the block on the disk, loads it and stores it into the memory.
     * TODO should remove a block from memory or overwrite a block in memory.
     * @param id of the block to load and return
     * @return The block reterived from disk
     */
    private Node loadBlockFromPageCache(long id) throws IOException {

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
        return ++nextID;
    }

    public LeafNode createLBlock(){
        long newBlockID = getNewID();
        blocks.put(newBlockID, new LeafNode(this, newBlockID));
        return (LeafNode)blocks.get(newBlockID);
    }
    public InternalNode createIBlock(){
        long newBlockID = getNewID();
        blocks.put(newBlockID, new InternalNode(this, newBlockID));
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

    private class IndexCursor extends Cursor {

    }
}
