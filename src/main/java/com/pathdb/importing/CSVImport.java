/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.importing;

import com.opencsv.CSVReader;
import com.pathdb.pathIndex.PathIndex;
import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.Node;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes an input file, and a source destination, and does an import.
 */
public class CSVImport
{
    private final PathIndex index;

    public
    CSVImport( PathIndex index )
    {
        this.index = index;
    }

    /**
     * Imports CSV files. Assumes the format of the file resembles:
     * 42, 4, 5, 6
     * 45, 6, 7, 8
     * Where 42 and 45 are path ID's and (4,5,6) and (6,7,8) are node ID's along that path.
     *
     * @param csvFile
     * @return The number of imported paths.
     * @throws IOException
     */
    public long doImport( File csvFile ) throws IOException
    {
        long importedPaths = 0;
        CSVReader csvReader = new CSVReader( new FileReader( csvFile ) );
        String[] line;
        while ( (line = csvReader.readNext()) != null )
        {
            long pathId = Long.parseLong( line[0] );
            List<Node> nodes = new ArrayList<>( line.length - 1 );
            for ( int i = 1; i < line.length; i++ )
            {
                nodes.add( new Node( Long.parseLong( line[i] ) ) );
            }

            index.insert( ImmutablePath.builder().addAllNodes( nodes ).pathId( pathId ).build() );
            importedPaths++;
        }
        return importedPaths;
    }
}
