package matrix;

/**
 * @author Chenglong Ma
 */
public class CoFeature {
    public final double similarity;
    public final int coSize;

    public CoFeature(double similarity, int coSize) {
        this.similarity = similarity;
        this.coSize = coSize;
    }

    @Override
    public String toString() {
        return String.format("Sim: %.2f, Co-size: %d", similarity, coSize);
    }
}
