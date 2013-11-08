package simpledb;
import java.io.Serializable;

public interface Histogram extends Serializable {

    public void addValue(int v);
    public void addValue(String s);
    public double estimateSelectivity(Predicate.Op op, int v);
    public double estimateSelectivity(Predicate.Op op, String s);
    public double avgSelectivity();
    public String toString();

}
