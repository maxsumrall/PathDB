package bptree;

/**
 * A cursor for iterating over a result set
 */
abstract class Cursor {

    abstract public Long[] next();

    abstract public boolean hasNext();

    abstract public int size();

    abstract public void first();
}
