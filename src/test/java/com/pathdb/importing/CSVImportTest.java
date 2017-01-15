package com.pathdb.importing;

import au.com.bytecode.opencsv.CSVWriter;
import com.pathdb.pathIndex.PathIndex;
import com.pathdb.pathIndex.inMemoryTree.InMemoryIndexFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CSVImportTest
{
    @Test
    public void importTest() throws IOException
    {
        // given
        int createdPaths = 100;
        File testCsvFile = createTestCsvFile( createdPaths );

        // when
        PathIndex index = new InMemoryIndexFactory().getInMemoryIndex();
        CSVImport importer = new CSVImport( index );
        long importedPaths = importer.doImport( testCsvFile );

        // then
        assertEquals(createdPaths, importedPaths);
    }

    /**
     * creates a csv file with the specified number of lines (paths)
     *
     * @param lines
     */
    public File createTestCsvFile( long lines ) throws IOException
    {
        File csvFile = createTempFile( getClass().getName() + ".csv" );
        CSVWriter writer = new CSVWriter( new FileWriter( csvFile ) );
        for ( int i = 0; i < lines; i++ )
        {
            String val = "" + i;
            writer.writeNext( new String[]{val, val, val, val, val} );
        }
        writer.close();
        return csvFile;
    }

    public File createTempFile( String filename )
    {
        File tempFile = new File( filename );
        tempFile.deleteOnExit();
        return tempFile;
    }

}
