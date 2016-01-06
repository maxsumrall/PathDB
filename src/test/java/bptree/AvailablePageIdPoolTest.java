package bptree;

import bptree.impl.AvailablePageIdPool;
import org.junit.Test;

/**
 * Created by max on 4/13/15.
 */
public class AvailablePageIdPoolTest {

    @Test
    public void pushAndPopTest(){
        int maximumItems = 5;
        AvailablePageIdPool pool = new AvailablePageIdPool(maximumItems);
        assert(pool.acquireId() == 0l);
        assert(pool.acquireId() == 1l);
        assert(pool.acquireId() == 2l);
        assert(pool.acquireId() == 3l);
        assert(pool.acquireId() == 4l);
        pool.releaseId(0l);
        pool.releaseId(3l);
        assert(pool.acquireId() == 3l);
        assert(pool.acquireId() == 0l);
    }

}