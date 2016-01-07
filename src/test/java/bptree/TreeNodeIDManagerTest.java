/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree;

import bptree.impl.TreeNodeIDManager;
import org.junit.Test;

public class TreeNodeIDManagerTest
{

    @Test
    public void pushAndPopTest(){
        int maximumItems = 5;
        TreeNodeIDManager pool = new TreeNodeIDManager(maximumItems);
        assert(pool.acquire() == 0l);
        assert(pool.acquire() == 1l);
        assert(pool.acquire() == 2l);
        assert(pool.acquire() == 3l);
        assert(pool.acquire() == 4l);
        pool.release(0l);
        pool.release(3l);
        assert(pool.acquire() == 3l);
        assert(pool.acquire() == 0l);
    }

}