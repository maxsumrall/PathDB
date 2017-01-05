package storage;

public interface PageFile<P extends Page>
{
    /**
     * Writes a page to the pagefile, returning the id of the page written.
     * @param page
     * @return
     */
    int writePage(P page) throws WriteCapacityExceededException;

    /**
     * Reads a page with the given pageid from the pagefile.
     * @param pageId
     * @return
     */
    Page readPage(int pageId);
}
