package bptree;

import bptree.impl.TreeNodeIDManager;
import org.junit.Test;

/**
 * Created by max on 4/13/15.
 */
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