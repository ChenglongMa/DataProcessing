package matrix;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * @author Chenglong Ma
 */
public class CountCollector implements Collector<Integer, CountCollector.Container, Map<Boolean, Integer>> {
    private final DataFrame dataFrame;
    private final boolean isUser;

    private CountCollector(DataFrame dataFrame, boolean isUser) {
        this.dataFrame = dataFrame;
        this.isUser = isUser;
    }

    static Map<Boolean, Integer> build(DataFrame dataFrame, boolean isUser) {
        long start = System.currentTimeMillis();
        CountCollector countCollector = new CountCollector(dataFrame, isUser);

//        int count = dataFrame.rowSize();
        Set<Integer> indices = dataFrame.getTestUserInnerIds();
//        IntStream.range(0, count).forEach(indices::add);
        Map<Boolean, Integer> counts = indices.parallelStream().collect(countCollector);
        long end = System.currentTimeMillis();
        System.out.printf("Time cost: %.2f s\n", (end - start) / 1000D);
        return counts;
    }

    public static void build(DataFrame df, boolean isUser, String resFilename) {
        System.out.println("Building Feature set...");
        Map<Boolean, Integer> counts = CountCollector.build(df, isUser);
        System.out.println("Matched users: " + counts.get(true));
        System.out.println("Unmatched users: " + counts.get(false));
    }

    @Override
    public Supplier<CountCollector.Container> supplier() {
        return CountCollector.Container::new;
    }

    @Override
    public BiConsumer<CountCollector.Container, Integer> accumulator() {
        return CountCollector.Container::accumulate;
    }

    @Override
    public BinaryOperator<CountCollector.Container> combiner() {
        return CountCollector.Container::combine;
    }

    @Override
    public Function<Container, Map<Boolean, Integer>> finisher() {
        return CountCollector.Container::getResult;
    }


    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    class Container {

        Map<Boolean, Integer> counts;

        Container() {
            counts = new HashMap<>();
            counts.put(true, 0);
            counts.put(false, 0);
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
        void accumulate(int u_inner_id) {
//            Map<Integer, Double> testUserVector = dataFrame.getTestUserRatings(u_inner_id);
//            if (testUserVector.isEmpty()) {
//                return;
//            }
            Map<Integer, Double> userVector = dataFrame.getUserRatings(u_inner_id);
            boolean isEmpty = userVector == null || userVector.isEmpty();
            counts.merge(isEmpty, 1, Integer::sum);

        }

        CountCollector.Container combine(CountCollector.Container container) {
            int trueValue = container.counts.getOrDefault(true, 0);
            int falseValue = container.counts.getOrDefault(false, 0);
            this.counts.merge(true, trueValue, Integer::sum);
            this.counts.merge(false, falseValue, Integer::sum);
            return this;
        }

        Map<Boolean, Integer> getResult() {
            return this.counts;
        }
    }
}
