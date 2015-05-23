package PageCacheSort;

import java.util.LinkedList;
import java.util.List;


public class PageSet{
    public LinkedList<Long> pagesInSet;
    public PageSet(){
        pagesInSet = new LinkedList<>();
    }
    public PageSet(long pageId){
        pagesInSet = new LinkedList<>();
        pagesInSet.push(pageId);
    }
    public PageSet(PageSet a, PageSet b){
        pagesInSet = new LinkedList<>();
        pagesInSet.addAll(a.getAll());
        pagesInSet.addAll(b.getAll());
    }
    public PageSet(PageSet a){
        pagesInSet = new LinkedList<>();
        pagesInSet.addAll(a.getAll());
    }
    public void push(long pageId){
        pagesInSet.push(pageId);
    }
    public boolean isEmpty(){
        return pagesInSet.isEmpty();
    }
    public Long pop(){
        return pagesInSet.pop();
    }
    public Long peek() {return pagesInSet.peek();}
    public List<Long> getAll(){return pagesInSet;}
}