package matrix;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static matrix.SimilarityMatrix.getCoFeature;

/**
 * @author Chenglong Ma
 */
public class SimCollector implements Collector<Integer, SimCollector.Container, SimilarityMatrix> {
    private final DataFrame dataFrame;
    private final int count;

    public SimCollector(DataFrame dataFrame) {
        this.dataFrame = dataFrame;
        this.count = dataFrame.rowSize();
    }

    @Override
    public Supplier<Container> supplier() {
        return Container::new;
    }

    @Override
    public BiConsumer<Container, Integer> accumulator() {
        return Container::accumulate;
    }

    @Override
    public BinaryOperator<Container> combiner() {
        return Container::combine;
    }

    @Override
    public Function<Container, SimilarityMatrix> finisher() {
        return Container::getResult;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    class Container {

        SimilarityMatrix matrix;

        Container() {
            matrix = new SimilarityMatrix();
        }

        void accumulate(int thisIndex) {
            Map<Integer, Double> thisVector = dataFrame.getUserRatings(thisIndex);
            if (!thisVector.isEmpty()) {
                // user/item itself exclusive
                for (int thatIndex = thisIndex + 1; thatIndex < count; thatIndex++) {
                    Map<Integer, Double> thatVector = dataFrame.getUserRatings(thatIndex);
                    if (thatVector.isEmpty()) {
                        continue;
                    }

                    CoFeature feats = getCoFeature(thisVector, thatVector);
                    if (feats != null) {
                        String thisId = dataFrame.getRealId(thisIndex, true);
                        String thatId = dataFrame.getRealId(thatIndex, true);
                        matrix.put(thisId, thatId, feats);
                    }
                }
            }
        }

        Container combine(Container container) {
            this.matrix.putAll(container.matrix);
            return this;
        }

        SimilarityMatrix getResult() {
            return this.matrix;
        }
    }
}
