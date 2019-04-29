import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import utils.Maths;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chenglong Ma
 */
public class SimilarityMatrix {
    // matrix data
    private final Table<Integer, Integer, Double> data;
    // matrix dimension
    private int dim;

    /**
     * Construct a symmetric matrix
     *
     * @param dim matrix dimension
     */
    public SimilarityMatrix(int dim) {
        this.dim = dim;
        data = HashBasedTable.create(); // do not specify the cardinality here as a
        // sparse matrix
    }

    /**
     * Construct a symmetric matrix by deeply copying data from a given matrix
     *
     * @param mat a given matrix
     */
    public SimilarityMatrix(SimilarityMatrix mat) {
        dim = mat.dim;
        data = HashBasedTable.create(mat.data);
    }

    public static SimilarityMatrix buildSimilarityMatrix(DataFrame dataFrame, boolean isUser) {
        long start = System.currentTimeMillis();
        int numUsers = dataFrame.rowSize();
        int numItems = dataFrame.columnSize();
        int count = isUser ? numUsers : numItems;

        SimilarityMatrix simMat = new SimilarityMatrix(count);
        List<Integer> indexList = isUser ? dataFrame.getRowIndices() : dataFrame.getColumnIndices();
        indexList.parallelStream().forEach((Integer thisIndex) -> {
            Map<Integer, Double> thisVector = isUser ? dataFrame.getRow(thisIndex) : dataFrame.getColumn(thisIndex);
            if (!thisVector.isEmpty()) {
                // user/item itself exclusive
                for (int thatIndex = thisIndex + 1; thatIndex < count; thatIndex++) {
                    Map<Integer, Double> thatVector = isUser ? dataFrame.getRow(thatIndex) : dataFrame.getColumn(thatIndex);
                    if (thatVector.isEmpty()) {
                        continue;
                    }

                    double sim = getCorrelation(thisVector, thatVector);
                    if (!Double.isNaN(sim) && sim != 0.0) {
                        simMat.put(thisIndex, thatIndex, sim);
                    }
                }
            }
        });
        long end = System.currentTimeMillis();
        System.out.printf("Time cost: %.2f s\n", (end - start) / 1000D);
        return simMat;
    }

    /**
     * Find the common rated items by this user and that user, or the common
     * users have rated this item or that item. And then return the similarity.
     *
     * @param thisVector: the rated items by this user, or users that have rated this
     *                    item.
     * @param thatVector: the rated items by that user, or users that have rated that
     *                    item.
     * @return similarity
     */
    public static double getCorrelation(Map<Integer, Double> thisVector, Map<Integer, Double> thatVector) {
        // compute similarity
        List<Double> thisList = new ArrayList<>();
        List<Double> thatList = new ArrayList<>();

//        int thisPosition = 0, thatPosition = 0;
//        int thisSize = thisVector.size(), thatSize = thatVector.size();
//        int thisIndex, thatIndex;
        for (Integer id : thisVector.keySet()) {
            if (!thatVector.containsKey(id)) {
                continue;
            }
            thisList.add(thisVector.get(id));
            thatList.add(thatVector.get(id));
        }

        return getSimilarity(thisList, thatList);
    }

    /**
     * Calculate the similarity between thisList and thatList.
     *
     * @param thisList this list
     * @param thatList that list
     * @return similarity
     */
    public static double getSimilarity(List<? extends Number> thisList, List<? extends Number> thatList) {
        // compute similarity

        if (thisList == null || thatList == null || thisList.size() < 2 || thatList.size() < 2 || thisList.size() != thatList.size()) {
            return Double.NaN;
        }

        double thisMu = Maths.mean(thisList);
        double thatMu = Maths.mean(thatList);

        double num = 0.0, thisPow2 = 0.0, thatPow2 = 0.0;
        for (int i = 0; i < thisList.size(); i++) {
            double thisMinusMu = thisList.get(i).doubleValue() - thisMu;
            double thatMinusMu = thatList.get(i).doubleValue() - thatMu;

            num += thisMinusMu * thatMinusMu;
            thisPow2 += thisMinusMu * thisMinusMu;
            thatPow2 += thatMinusMu * thatMinusMu;
        }

        return num / (Math.sqrt(thisPow2) * Math.sqrt(thatPow2));
    }

    /**
     * Get a value at entry (row, col)
     *
     * @param row row index
     * @param col column index
     * @return value at entry (row, col)
     */
    public double get(int row, int col) {

        if (data.contains(row, col))
            return data.get(row, col);
        else if (data.contains(col, row))
            return data.get(col, row);

        return 0.0d;
    }

    /**
     * Get a value at entry (row, col)
     *
     * @param row row index
     * @param col column index
     * @return value at entry (row, col)
     */
    public boolean contains(int row, int col) {
        return data.contains(row, col) || data.contains(col, row);
    }

    /**
     * set a value to entry (row, col)
     *
     * @param row row index
     * @param col column index
     * @param val value to set
     */
    public void set(int row, int col, double val) {
        if (row >= col)
            data.put(row, col, val);
        else
            data.put(col, row, val);
    }

    public void put(int row, int col, double value) {
        if (contains(row, col)) {
            return;
        }
        data.put(row, col, value);
        data.put(col, row, value);
    }

    /**
     * plus a value to entry (row, col)
     *
     * @param row row index
     * @param col column index
     * @param val value to plus
     */
    public void add(int row, int col, double val) {
        if (row >= col)
            data.put(row, col, val + get(row, col));
        else
            data.put(col, row, val + get(col, row));
    }

    /**
     * Retrieve a complete row of similar items
     *
     * @param row row index
     * @return a complete row of similar items
     */
    public Map<Integer, Double> row(int row) {
        Map<Integer, Double> map = new HashMap<>();
        for (int col = 0; col < dim; col++) {
            double val = get(row, col);
            if (val != 0)
                map.put(col, val);
        }

        return map;
    }

    /**
     * @return the dim
     */
    public int getDim() {
        return dim;
    }

    /**
     * @return the data
     */
    public Table<Integer, Integer, Double> getData() {
        return data;
    }
}