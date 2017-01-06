/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pageCacheSort;

import java.io.IOException;

public interface SetIterator {

    long[] getNext() throws IOException;

    public long[] peekNext() throws IOException;

    public boolean hasNext() throws IOException;
}
