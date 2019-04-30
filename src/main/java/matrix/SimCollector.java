package matrix;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static matrix.SimilarityMatrix.getCorrelation;

/**
 * @author Chenglong Ma
 */
public class SimCollector implements Collector<Integer, SimCollector.Container, SimilarityMatrix> {
    private final DataFrame dataFrame;
    private final boolean isUser;
    private final int count;

    public SimCollector(DataFrame dataFrame, boolean isUser) {
        this.dataFrame = dataFrame;
        this.isUser = isUser;
        int numUsers = dataFrame.rowSize();
        int numItems = dataFrame.columnSize();
        this.count = isUser ? numUsers : numItems;
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
            Map<Integer, Double> thisVector = isUser ? dataFrame.getRow(thisIndex) : dataFrame.getColumn(thisIndex);
            if (!thisVector.isEmpty()) {
                // user/item itself exclusive
                for (int thatIndex = thisIndex + 1; thatIndex < count; thatIndex++) {
                    Map<Integer, Double> thatVector = isUser ? dataFrame.getRow(thatIndex) : dataFrame.getColumn(thatIndex);
                    if (thatVector.isEmpty()) {
                        continue;
                    }

                    CoFeature feats = getCorrelation(thisVector, thatVector);
                    if (feats != null) {
                        String thisId = dataFrame.getRealId(thisIndex, isUser);
                        String thatId = dataFrame.getRealId(thatIndex, isUser);
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
