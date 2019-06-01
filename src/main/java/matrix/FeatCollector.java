package matrix;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * @author Chenglong Ma
 */
public class FeatCollector implements Collector<Integer, FeatCollector.Container, List<Row>> {
    private final DataFrame dataFrame;
    private final boolean isUser;
    private final double averageRating;

    private FeatCollector(DataFrame dataFrame, boolean isUser) {
        this.dataFrame = dataFrame;
        this.isUser = isUser;
        averageRating = dataFrame.getAverageRating();
    }

    static List<Row> buildFeatureSet(DataFrame dataFrame, boolean isUser) {
        long start = System.currentTimeMillis();
        FeatCollector featCollector = new FeatCollector(dataFrame, isUser);

//        int count = dataFrame.rowSize();
        Set<Integer> indices = dataFrame.getTestUserInnerIds();
//        IntStream.range(0, count).forEach(indices::add);
        List<Row> feats = indices.parallelStream().collect(featCollector);
        long end = System.currentTimeMillis();
        System.out.printf("Time cost: %.2f s\n", (end - start) / 1000D);
        return feats;
    }

    public static void buildFeatureSet(DataFrame df, boolean isUser, String resFilename) {
        System.out.println("Building Feature set...");
        List<Row> feats = FeatCollector.buildFeatureSet(df, isUser);
        System.out.println(feats.size());
        toCSV(feats, resFilename, false);
    }

    private static void toCSV(List<Row> feats, @NonNull String resFilename, boolean append) {
        long start = System.currentTimeMillis();
        System.out.printf("Writing to %s...\n", resFilename);
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(resFilename, append), CSVFormat.DEFAULT)) {
            feats.parallelStream().forEachOrdered(row -> {
                try {
                    printer.printRecord(
                            row.u,
                            row.i,
                            row.r_u,
                            row.v,
                            row.r_v,
                            row.sim,
                            row.co_size,
                            row.u_count,
                            row.u_mean,
                            row.mean_u_off,
                            row.i_count,
                            row.i_mean,
                            row.mean_i_off,
                            row.v_count,
                            row.v_mean,
                            row.mean_v_off,
                            row.v_i_off,
                            row.v_u_off,
                            row.v_mean_off
                    );
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
    public Supplier<Container> supplier() {
        return Container::new;
    }

    @Override
    public BiConsumer<Container, Integer> accumulator() {
        return Container::userAccumulate;
    }

    @Override
    public BinaryOperator<Container> combiner() {
        return Container::combine;
    }

    @Override
    public Function<Container, List<Row>> finisher() {
        return Container::getResult;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    class Container {

        List<Row> rows;

        Container() {
            rows = new ArrayList<>();
        }

        /**
         * Features:
         * u, i, r_u, v, r_v,
         * sim, co_size,
         * [ count, mean, median, mean_x_off ] * 3,
         * v_i_off, v_u_off, v_mean_off
         *
         * @param u_inner_id
         */
        void userAccumulate(int u_inner_id) {
            Map<Integer, Double> testUserVector = dataFrame.getTestUserRatings(u_inner_id);
            if (testUserVector.isEmpty()) {
                return;
            }
            Map<Integer, Double> userVector = dataFrame.getUserRatings(u_inner_id);
            if (userVector.isEmpty()) {
                return;
            }
            String u = dataFrame.getRealId(u_inner_id, true);
            double u_mean = dataFrame.getUserMean(u_inner_id);
            int u_count = dataFrame.getUserCount(u_inner_id);
            double mean_u_off = dataFrame.getUserMeanOff(u_inner_id);
            // median
            for (Map.Entry<Integer, Double> ratings : testUserVector.entrySet()) {
                int i_inner_id = ratings.getKey();
                Map<Integer, Double> itemVector = dataFrame.getItemRatings(i_inner_id);
                if (itemVector.isEmpty()) {
                    continue;
                }
                String i = dataFrame.getRealId(i_inner_id, false);
                double r_u = ratings.getValue();
                double i_mean = dataFrame.getItemMean(i_inner_id);
                int i_count = dataFrame.getItemCount(i_inner_id);
                double mean_i_off = dataFrame.getItemMeanOff(i_inner_id);
                for (Map.Entry<Integer, Double> nbrRatings : itemVector.entrySet()) {
                    int nbr_inner_id = nbrRatings.getKey();
                    if (u_inner_id == nbr_inner_id) {
                        continue;
                    }
                    String v = dataFrame.getRealId(nbr_inner_id, true);
                    double r_v = nbrRatings.getValue();
                    Map<Integer, Double> nbrVector = dataFrame.getUserRatings(nbr_inner_id);
                    CoFeature coFeat = SimilarityMatrix.getCoFeature(userVector, nbrVector);
                    if (coFeat == null) {
                        continue;// may reduce the size
                    }
                    double v_mean = dataFrame.getUserMean(nbr_inner_id);
                    int v_count = dataFrame.getUserCount(nbr_inner_id);
                    double mean_v_off = dataFrame.getUserMeanOff(nbr_inner_id);
                    double v_i_off = r_v - i_mean;
                    double v_u_off = r_v - u_mean;
                    double v_mean_off = r_v - averageRating;
                    Row row = new Row();
                    row.u = u;
                    row.i = i;
                    row.r_u = r_u;
                    row.v = v;
                    row.r_v = r_v;

                    row.sim = coFeat.jaccard;
                    row.co_size = coFeat.coSize;

                    row.u_count = u_count;
                    row.u_mean = u_mean;
                    row.mean_u_off = mean_u_off;

                    row.i_count = i_count;
                    row.i_mean = i_mean;
                    row.mean_i_off = mean_i_off;

                    row.v_count = v_count;
                    row.v_mean = v_mean;
                    row.mean_v_off = mean_v_off;

                    row.v_i_off = v_i_off;
                    row.v_u_off = v_u_off;
                    row.v_mean_off = v_mean_off;
                    rows.add(row);
                }
            }

        }

        FeatCollector.Container combine(FeatCollector.Container container) {
            this.rows.addAll(container.rows);
            return this;
        }

        List<Row> getResult() {
            return this.rows;
        }
    }
}
