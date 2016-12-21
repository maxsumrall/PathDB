package storage;

/**
 * An abstract that moves bytes from an application to a storage medium layer.
 */
public abstract class BlockStorage
{
    public abstract void writeBytes( int start, byte[] bytes );

    public abstract byte[] getBytes( long location );
}
