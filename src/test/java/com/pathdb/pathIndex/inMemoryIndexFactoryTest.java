package com.pathdb.pathIndex;

import com.pathdb.pathIndex.inMemoryTree.InMemoryIndexFactory;
import org.junit.Test;

import static org.junit.Assert.fail;

public class inMemoryIndexFactoryTest
{

    @Test
    public void inMemoryIndexTest() throws Exception
    {
        // given
        IndexFactory factory = new InMemoryIndexFactory();

        // when
        PathIndex ephemeralIndex = factory.getEphemeralIndex();

        // then
        //all good.
    }

    @Test
    public void diskBackedTest() throws Exception
    {
        // given
        IndexFactory factory = new InMemoryIndexFactory();

        // when
        try
        {
            PathIndex persistedDiskBasedIndex = factory.getPersistedDiskBasedIndex();
            fail( "this is not supported yet." );
        }
        catch ( UnsupportedOperationException e )
        {
            //expected
        }
        // then
        //all good.
    }
}
