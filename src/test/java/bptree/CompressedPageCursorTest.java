package bptree;

import org.junit.Test;

import java.util.LinkedList;

/**
 * Created by max on 6/8/15.
 */
public class CompressedPageCursorTest {

    @Test
    public void insertTest(){
        LinkedList<Long[]> keys = new LinkedList<>();
        for(long i = 0; i < 1200; i++){
            keys.add(new Long[]{i,i,i,i});
        }
    }

}
