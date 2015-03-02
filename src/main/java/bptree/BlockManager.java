package bptree;import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by max on 2/13/15.
 */
public class BlockManager extends AbstractBlockManager{

    public BlockManager() throws FileNotFoundException {
        /*
        Long = 8 bytes.
        Tuple = 3 longs packed together.
         */
        String pathToBlockFile = "bptreeblockfile.bptree";
        blockFileChannel = getBlockStorageChannel(pathToBlockFile);

        rootBlock = createLBlock(); // Initialize the tree with only one block for now, a leaf block.

    }

    /**
     * Gets a block. If the block is already loaded into memory, then we can imemdately return it.
     * If the block is not found, then we need to load it from disk.
     * @param id
     * @return
     */
    public Block getBlock(long id){
        Block result = blocks.get(id);
        if(result != null){ return result;}
        else{
            return loadBlockFromDisk(id);
        }
    }

    /**
     * Finds the block on the disk, loads it and stores it into the memory.
     * TODO should remove a block from memory or overwrite a block in memory.
     * @param id of the block to load and return
     * @return The block reterived from disk
     */
    private Block loadBlockFromDisk(long id){
        return null;
    }

    private long getNewID(){
        return ++nextID;
    }

    public LBlock createLBlock(){
        long newBlockID = getNewID();
        blocks.put(newBlockID, new LBlock(this, newBlockID));
        return (LBlock)blocks.get(newBlockID);
    }
    public IBlock createIBlock(){
        long newBlockID = getNewID();
        blocks.put(newBlockID, new IBlock(this, newBlockID));
        return (IBlock)blocks.get(newBlockID);
    }

    //public long size() { try {return blockFileChannel.size();}  catch (IOException e) {return 0;} }

    private FileChannel getBlockStorageChannel(String path) throws FileNotFoundException {
        //File blockFile = new File(path);
        RandomAccessFile randomAccessBlockFile = new RandomAccessFile(path, "rwd");
        return randomAccessBlockFile.getChannel();
    }

    /**
     * Given a block id, begin reading bytes from the file channel
     * where this block id begins.
     * Cast bytes into longs and return this block as a 2d long array.
     * @param blockID
     * @return 2D array representing a block.
     */
    private long[][] readBlockFromFileChannel(int blockID) throws IOException {

        /*
        For now I'll assume block ID's are integers such as 1,2,3...N
         for however many (N) blocks we have. Then to read a block from the
          file channel we offset by block size to know the position
          of the first byte of this block.
          */
        long startingPosition = blockID * blockSize;
        ByteBuffer blockByteBuffer = ByteBuffer.allocate(blockSize);
        blockFileChannel.read(blockByteBuffer, startingPosition);

        /*
        Cast the bytes into the 2D long array
         */
        int numberOfTupleInBlock = (blockSize/8)/3;
        long[][] blockArray = new long[numberOfTupleInBlock][3];
        for(int i = 0; i < numberOfTupleInBlock; i++){
            blockArray[i][0] = blockByteBuffer.getLong();
            blockArray[i][1] = blockByteBuffer.getLong();
            blockArray[i][2] = blockByteBuffer.getLong();
        }
        return blockArray;
    }

    /**
     * Write a block to the file.
     * There are some design decisions to be made here. Blocks are different sizes since keys can be different sizes.
     * So, how do we store them in the file? Since maybe the first time you write a file to the disk its size N, if you add some keys to it.
     * ALSO you need to delimit between the keys in the blocks if you have different number of
     * @param block
     * @throws IOException
     */
    private void writeBlockToFileChannel(Block block) throws IOException {
        /*
        First convert the 2D block array to Byte Format for writing to the File Channel.
         */
        ByteBuffer blockByteBuffer = ByteBuffer.allocate(blockSize);
        for(int i = 0; i < block.keys.length; i ++){
            for(int j = 0; j < block.keys[i].vals.length; j++){
                blockByteBuffer.putLong(block.keys[i].vals[j]);
            }
        }
        blockByteBuffer.putLong(Long.MAX_VALUE); //Using this as a delimiter. I want to delimit between keys since keys are variable length.

        if(block instanceof IBlock){
            for(int i = 0; i < ((IBlock) block).children.length; i++){
                blockByteBuffer.putLong(((IBlock) block).children[i]);
            }
        }
        blockByteBuffer.putLong(Long.MAX_VALUE);//Delimiter

        long blockPosition = block.blockID * BLOCK_SIZE;
        blockFileChannel.write(blockByteBuffer, blockPosition);
    }


}
