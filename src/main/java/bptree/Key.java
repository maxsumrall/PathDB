package bptree;

/**
 * Created by max on 3/26/15.
 */
public interface Key{

    Long[] getLabelPath();

    Long[] getNodes();

    Long[] getComposedKey(Long pathID);

}
