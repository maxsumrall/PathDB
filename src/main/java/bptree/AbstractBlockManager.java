package bptree;

import java.nio.channels.FileChannel;
import java.util.HashMap;

/**
 * Created by max on 2/12/15.
 */
public abstract class AbstractBlockManager {

    protected long nextID = 0l;

    protected int blockSize;
    protected FileChannel blockFileChannel;
    protected long tuplesPerBlock;
    /**
     * This autoboxing from long to Long is really bad!
     */
    //TODO do not use autoboxing.
    public HashMap<Long, Block> blocks = new HashMap<Long, Block>();

    public HashMap<Long, Long> blockFileOffsets = new HashMap<Long, Long>(); // Mapping from block ID to that blocks offset in the file on disk.

    public Block rootBlock;

    public abstract Block getBlock(long id);

    private static int calculate_keys_in_IBlock(){
        if (BLOCK_SIZE % ((KEY_LENGTH * BYTES_IN_LONG) + (CHILD_POINTER_LENGTH * BYTES_IN_LONG)) >= (CHILD_POINTER_LENGTH * BYTES_IN_LONG)){
            return BLOCK_SIZE / ((KEY_LENGTH * BYTES_IN_LONG) + (CHILD_POINTER_LENGTH * BYTES_IN_LONG));
        }
        return BLOCK_SIZE / ((KEY_LENGTH * BYTES_IN_LONG) + (CHILD_POINTER_LENGTH * BYTES_IN_LONG)) -1;
    }

    public static int CHILD_POINTER_LENGTH = 1;

    public static int BYTES_IN_LONG = 8;

    public static int KEY_LENGTH = 3; //bptree.Key length is the number of longs in the key, which will be the number of relationships we index for.

    //public static int BLOCK_SIZE = bptree.Utils.getIdealBlockSize();
    public static int BLOCK_SIZE = 110;//testing. Should be about 3 per block

    public static int CHILDREN_PER_IBLOCK = calculate_keys_in_IBlock() + 1;

    public static int KEYS_PER_IBLOCK = calculate_keys_in_IBlock();

    public static int KEYS_PER_LBLOCK = BLOCK_SIZE / (BYTES_IN_LONG * KEY_LENGTH);








}
