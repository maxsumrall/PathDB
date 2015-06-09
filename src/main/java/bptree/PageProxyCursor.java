package bptree;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for managing page/disk access
 */
public abstract class PageProxyCursor implements Closeable{

    abstract public int getSize();

    abstract public void next(long page) throws IOException;

    abstract public long getCurrentPageId();

    abstract public int capacity();

    abstract public  void setOffset(int offset);

    abstract public int getOffset();

    abstract public  void getBytes(byte[] dest);

    abstract public byte getByte(int offset);

    abstract public void putBytes(byte[] src);

    abstract public void putByte(byte val);

    abstract public void putByte(int offset, byte val);

    abstract public long getLong();

    abstract public long getLong(int offset);

    abstract public  void putLong(long val);

    abstract public void putLong(int offset, long val);

    abstract public int getInt();

    abstract public int getInt(int offset);

    abstract public void putInt(int val);

    abstract public void putInt(int offset, int val);

    abstract public boolean internalNodeContainsSpaceForNewKeyAndChild(long[] newKey) throws IOException;

    abstract public boolean leafNodeContainsSpaceForNewKey(long[] newKey) throws IOException;

    abstract public void deferWriting();

    abstract public void resumeWriting();

}