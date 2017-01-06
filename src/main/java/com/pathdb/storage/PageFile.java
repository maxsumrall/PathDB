/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.storage;

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
