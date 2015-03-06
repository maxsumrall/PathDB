package bptree;

import java.io.*;

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


    public byte[] serialize(PathIndex index) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] byteRep;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(index);
            byteRep = bos.toByteArray();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return byteRep;
    }
    public PathIndex deserialize(byte[] byteRep) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(byteRep);
        ObjectInput in = null;
    PathIndex index;
        try {
            in = new ObjectInputStream(bis);
            index = (PathIndex) in.readObject();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return index;
    }
}