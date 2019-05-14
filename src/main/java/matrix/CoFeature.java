package matrix;

/**
 * @author Chenglong Ma
 */
public class CoFeature {
    public final double pcc, cos, jaccard;
    public final int coSize;

    public CoFeature(double pcc, double cos, double jaccard, int coSize) {
        this.pcc = pcc;
        this.cos = cos;
        this.jaccard = jaccard;
        this.coSize = coSize;
    }

    @Override
    public String toString() {
        return String.format("Sim: %.2f, Co-size: %d", pcc, coSize);
    }
}
