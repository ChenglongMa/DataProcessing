package matrix;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.checkerframework.checker.nullness.qual.NonNull;
import utils.CosSimilarity;
import utils.JaccardSimilarity;
import utils.PccSimilarity;

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
    private static final PccSimilarity PCC = PccSimilarity.getInstance();
    private static final CosSimilarity COS = CosSimilarity.getInstance();
    private static final JaccardSimilarity JACCARD = JaccardSimilarity.getInstance();
    // matrix data
    private final Table<String, String, CoFeature> data;

    public SimilarityMatrix() {
        data = HashBasedTable.create();
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
    public static CoFeature getCoFeature(Map<Integer, Double> thisVector, Map<Integer, Double> thatVector) {
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
        double pcc = 0;//PCC.getSimilarity(thisList, thatList);
        double cos = 0;
//        double jaccard = 0;
//        double cos = COS.getSimilarity(thisList, thatList);
        double jaccard = JACCARD.getSimilarity(thisList, thatList);
        if (Double.isNaN(jaccard)) {
//        if (Double.isNaN(pcc) || pcc == 0.0) {
            return null;
        }
        return new CoFeature(pcc, cos, jaccard, coSize);
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

        return new CoFeature(0, 0, 0, 0);
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
                    printer.printRecord(r.getRowKey(), r.getColumnKey(), value.jaccard,/* value.cos, value.jaccard,*/ value.coSize);
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
