package NeoIntegration;

public class IndexConstraint{
    final public int indexInResultSet;
    final public String property;
    final public String value;

    public IndexConstraint(int indexInResultSet, String property, String value) {
        this.indexInResultSet = indexInResultSet;
        this.property = property;
        this.value = value;
    }
}
