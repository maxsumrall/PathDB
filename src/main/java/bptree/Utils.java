package bptree;import java.io.BufferedOutputStream;

/**
 * Created by max on 2/10/15.
 */
public class Utils {


    /**
     * Getting the block size as used by BufferedXXXStream classes.
     * Ideally we want to find a better way to optimize which block size to use.
     * Typically anything within a power of 2 is a reasonable choice.
     * Most systems use 4096, 8192
     */
        private static class MyBufferedOS extends BufferedOutputStream {
            public MyBufferedOS() {
                super(System.out);
            }
            public int bufferSize() {
                return buf.length;
            }
        }

    /**
     *
     * @return int The block size used by the BufferedXXXStream classes.
     */
    public static int getIdealBlockSize(){
        return new Utils.MyBufferedOS().bufferSize();
    }


    public static void printTree(Block block){
        System.out.println(block);
        if(block instanceof IBlock)
            for(long childID : ((IBlock)block).children)
                printTree(block.blockManagerInstance.getBlock(childID));
    }


}