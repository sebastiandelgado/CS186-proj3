package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {

    private int numBuckets;
    private int[] buckets;
    private int bucketWidth;
    private int numTups;
    private int min;
    private int max;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
	numBuckets = buckets;
	this.buckets = new int[numBuckets];
	bucketWidth = (max - min + 1) / numBuckets;
	if (((max - min + 1) % numBuckets) != 0) { bucketWidth++; }
	for (int i = 0; i < numBuckets; i++) { this.buckets[i] = 0; }
	this.min = min;
	this.max = max;
	numTups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
	if ((v < min) || (v > max)) {
	    System.out.println("Histogram insertion out of bounds");
	}
	buckets[(v - min) / bucketWidth] += 1;
	numTups++;
    }
    public void addValue(String s) {
	// do nothing, just here to satisfy interface
	System.out.println("Adding a string to an IntHistogram (this should never happen)");
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
	double probEquals;
	double probLessThan;
	String opString = op.toString();
	int vBucket = (v - min) / bucketWidth;

	if (vBucket < 0) {
	    probEquals = 0.0;
	    probLessThan = 0.0;
	} else if (vBucket >= buckets.length) {
	    probEquals = 0.0;
	    probLessThan = 1.0;
	} else  {
		int bucketHeight = buckets[vBucket];
		double avgInBucket = (double) (((double)bucketHeight / (double)bucketWidth) / (double)numTups);
		probEquals = avgInBucket;
		int numSmallerTups = 0;
		for (int i = 0; i < vBucket; i++) {
		    numSmallerTups += buckets[i];
		}
		numSmallerTups += (v % bucketWidth) * avgInBucket;
		probLessThan = (double) numSmallerTups / (double) numTups; 
	}
	if (opString.equals("=")) { return probEquals; }
	if (opString.equals("<>")) { return 1.0 - probEquals; }
	if (opString.equals("<")) { return probLessThan; }
	if (opString.equals("<=")) { return probLessThan + probEquals; }
	if (opString.equals(">")) { return 1.0 - probLessThan - probEquals; }
	if (opString.equals(">=")) { return (1.0 - probLessThan); }
	return -1.0;
    }
    public double estimateSelectivity(Predicate.Op op, String s) {
	// do nothing, just here to satisfy interface
	System.out.println("Estimating selectivity using a String in IntHistogram (this should never happen)");
	return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
	String result = "";
	int lowerBound = min;
	for (int i = 0; i < numBuckets; i++) {
	    result += (lowerBound + " to " + (lowerBound + bucketWidth - 1) + ": ");
	    for (int j = 0; j < buckets[i]; j++) { result += "*"; }
	    result += "\n";
	    lowerBound += bucketWidth;
	}
        return result;
    }
}
