package storage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class InMemoryBlockStorage
{
    private final long maxPageSize;
    private Map<Integer,byte[]> storage;

    public InMemoryBlockStorage( long maxPageSize )
    {
        this.maxPageSize = maxPageSize;
        storage = new HashMap<>();
    }

    public void writeBytes( int start, int end, byte[] bytes )
    {
        if ( bytes.length < maxPageSize )
        {
            storage.put( start, Arrays.copyOf( bytes, end ) );
        }
        else
        {
            throw new IllegalArgumentException( "Attempted to write byte array larger than page size." );
        }
    }

    public byte[] getBytes( long location )
    {
        if ( storage.containsKey( location ) )
        {
            return storage.get( location );
        }
        else
        {
            throw new IllegalArgumentException( "Attempted to retrieve bytes from uninitialized location." );
        }
    }
}
