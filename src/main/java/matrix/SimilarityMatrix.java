package matrix;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.checkerframework.checker.nullness.qual.NonNull;
import utils.Maths;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author Chenglong Ma
 */
public class SimilarityMatrix {
    // matrix data
    private final Table<String, String, CoFeature> data;
    // matrix dimension
    private int dim;

    public SimilarityMatrix() {
        data = HashBasedTable.create();
    }

    /**
     * Construct a symmetric matrix
     *
     * @param dim matrix dimension
     */
    public SimilarityMatrix(int dim) {
        this.dim = dim;
        data = HashBasedTable.create();
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

    public static SimilarityMatrix buildSimMat(DataFrame dataFrame) {
        long start = System.currentTimeMillis();
        int count = dataFrame.rowSize();

        SimCollector simCollector = new SimCollector(dataFrame);
        List<Integer> indices = new ArrayList<>();
        IntStream.range(0, count).forEach(indices::add);
        SimilarityMatrix simMat = indices.parallelStream().collect(simCollector);
        long end = System.currentTimeMillis();
        System.out.printf("Time cost: %.2f s\n", (end - start) / 1000D);
        return simMat;
    }

//    public static SimilarityMatrix buildSimilarityMatrix(DataFrame dataFrame, boolean isUserBased) {
//        long start = System.currentTimeMillis();
//        int numUsers = dataFrame.rowSize();
//        int numItems = dataFrame.columnSize();
//        int count = isUserBased ? numUsers : numItems;
//
//        SimilarityMatrix simMat = new SimilarityMatrix(count);
//        List<Integer> indexList = new ArrayList<>(isUserBased ? dataFrame.getRowIndices() : dataFrame.getColumnIndices());
////        indexList.sort(Integer::compareTo);
//        indexList.parallelStream().forEach(thisIndex -> {
//            Map<Integer, Double> thisVector = isUserBased ? dataFrame.getRow(thisIndex) : dataFrame.getColumn(thisIndex);
//            if (!thisVector.isEmpty()) {
//                // user/item itself exclusive
//                for (int thatIndex = thisIndex + 1; thatIndex < count; thatIndex++) {
//                    Map<Integer, Double> thatVector = isUserBased ? dataFrame.getRow(thatIndex) : dataFrame.getColumn(thatIndex);
//                    if (thatVector.isEmpty()) {
//                        continue;
//                    }
//
//                    CoFeature feats = getCorrelation(thisVector, thatVector);
//                    if (feats != null) {
//                        String thisId = dataFrame.getRealId(thisIndex, isUserBased);
//                        String thatId = dataFrame.getRealId(thatIndex, isUserBased);
//                        simMat.put(thisId, thatId, feats);
//                    }
//                }
//            }
//        });
//        long end = System.currentTimeMillis();
//        System.out.printf("Time cost: %.2f s\n", (end - start) / 1000D);
//        return simMat;
//    }

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
    public static CoFeature getCorrelation(Map<Integer, Double> thisVector, Map<Integer, Double> thatVector) {
        // compute similarity
        List<Double> thisList = new ArrayList<>();
        List<Double> thatList = new ArrayList<>();

        for (Integer id : thisVector.keySet()) {
            if (!thatVector.containsKey(id)) {
                continue;
            }
            thisList.add(thisVector.get(id));
            thatList.add(thatVector.get(id));
        }
        int coSize = thatList.size();
        double sim = getSimilarity(thisList, thatList);
        if (Double.isNaN(sim) || sim == 0.0) {
            return null;
        }
        return new CoFeature(sim, coSize);
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
    public CoFeature get(String row, String col) {

        if (data.contains(row, col))
            return data.get(row, col);
        else if (data.contains(col, row))
            return data.get(col, row);

        return new CoFeature(0, 0);
    }

    /**
     * Get a value at entry (row, col)
     *
     * @param row row index
     * @param col column index
     * @return value at entry (row, col)
     */
    public boolean contains(String row, String col) {
        return data.contains(row, col) || data.contains(col, row);
    }

//    /**
//     * set a value to entry (row, col)
//     *
//     * @param row row index
//     * @param col column index
//     * @param val value to set
//     */
//    public void set(String row, String col, double val) {
//        if (row >= col)
//            data.put(row, col, val);
//        else
//            data.put(col, row, val);
//    }

    public void put(String row, String col, CoFeature value) {
        if (contains(row, col)) {
            return;
        }
        data.put(row, col, value);
//        data.put(col, row, value);
    }

    public void putAll(Table<String, String, CoFeature> subset) {
        data.putAll(subset);
    }

    public void putAll(SimilarityMatrix subMatrix) {
        putAll(subMatrix.data);
    }

//    /**
//     * plus a value to entry (row, col)
//     *
//     * @param row row index
//     * @param col column index
//     * @param val value to plus
//     */
//    public void add(int row, int col, double val) {
//        if (row >= col)
//            data.put(row, col, val + get(row, col));
//        else
//            data.put(col, row, val + get(col, row));
//    }

//    /**
//     * Retrieve a complete row of similar items
//     *
//     * @param row row index
//     * @return a complete row of similar items
//     */
//    public Map<Integer, Double> row(int row) {
//        Map<Integer, Double> map = new HashMap<>();
//        for (int col = 0; col < dim; col++) {
//            double val = get(row, col);
//            if (val != 0)
//                map.put(col, val);
//        }
//
//        return map;
//    }

    /**
     * @return the dim
     */
    public int getDim() {
        return dim;
    }

    /**
     * @return the data
     */
    public Table<String, String, CoFeature> getData() {
        return data;
    }

    public void toCSV(@NonNull String resFilename, boolean append) {
        long start = System.currentTimeMillis();
        System.out.printf("Writing to %s...\n", resFilename);
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(resFilename, append), CSVFormat.DEFAULT)) {
            getData().cellSet().parallelStream().forEachOrdered(r -> {
                try {
//                    String value = String.format("%s,%s,%s", r.getRowKey(), r.getColumnKey(), r.getValue());
//                    printer.print(value);
                    @NonNull CoFeature value = r.getValue();
                    printer.printRecord(r.getRowKey(), r.getColumnKey(), value.similarity, value.coSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.printf("Time cost: %.2f s\n", (end - start) / 1000D);
    }

    @Override
    public String toString() {
        if (data.size() > 100) {
            return "Too large...";
        }
        return data.toString();
    }

    public int size() {
        return data.size();
    }
}
