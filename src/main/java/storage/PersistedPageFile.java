package storage;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;

public class PersistedPageFile<P extends Page> implements PageFile<P>
{
    public static int PAGE_SIZE = 8192;
    protected DefaultFileSystemAbstraction fs;
    protected PageCache pageCache;
    public transient PagedFile pagedFile;

    public PersistedPageFile( File pageCacheFile ) throws IOException
    {
        fs = new DefaultFileSystemAbstraction();
        this.pageCache = StandalonePageCacheFactory.createPageCache( fs, Config.empty() );
        pagedFile = this.pageCache.map( pageCacheFile, PAGE_SIZE );
    }

    @Override
    public int writePage( P page )
    {
        writeBytes( page.getPageId(), page.getByteRepresentation() );
        return page.getPageId();
    }

    @Override
    public Page readPage( int pageId )
    {
        byte[] bytes = getBytes( pageId );
        return new Page(bytes);
    }

    void writeBytes( int page, byte[] bytes )
    {
        PageCursor io = null;
        try
        {
            io = pagedFile.io( page, PagedFile.PF_NO_FAULT );
            io.next( page );
            io.putBytes( bytes );
        }
        catch ( IOException e )
        {
            //todo The write failed. What should happen then?
            e.printStackTrace();
        }
        finally
        {
            if ( io != null )
            {
                io.close();
            }
        }
    }

    byte[] getBytes( long page )
    {
        byte[] bytes = new byte[PAGE_SIZE];
        PageCursor io = null;
        try
        {
            io = pagedFile.io( page, PagedFile.PF_SHARED_READ_LOCK );
            io.next( page );
            io.getBytes( bytes ); //todo obviously needs review
        }
        catch ( IOException e )
        {
            //todo The write failed. What should happen then?
            e.printStackTrace();
        }
        finally
        {
            if ( io != null )
            {
                io.close();
            }
        }
        return bytes;
    }
}
