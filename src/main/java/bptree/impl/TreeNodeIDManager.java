package bptree.impl;

import java.util.LinkedList;

public class TreeNodeIDManager
{
    private static LinkedList<Long> pool = null;
    public static long currentID = 0;
    //private long maximumNumberOfPages;

    public TreeNodeIDManager(long maximumNumberOfPages){
        if(pool == null) {
            pool = new LinkedList<>();
        }
    }

    public static Long acquire(){
        if(pool == null) {
            pool = new LinkedList<>();
        }
        if(pool.size() > 0) {
            return pool.pop();
        }
        else{
            return currentID++;
        }
    }

    public static void release(Long id){
        pool.push(id);
    }

    public boolean isNodeIdInFreePool(Long search_id){
        for(Long id : pool){
            if(search_id.equals(id)){
                return true;
            }
        }
        return false;
    }
}
