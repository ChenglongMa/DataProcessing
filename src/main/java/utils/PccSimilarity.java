package utils;

import java.util.List;

/**
 * @author Chenglong Ma
 */
public class PccSimilarity implements Similarity {
    private static PccSimilarity instance;

    private PccSimilarity() {
        // for singleton pattern
    }

    public static synchronized PccSimilarity getInstance() {
        if (instance == null) {
            instance = new PccSimilarity();
        }
        return instance;
    }

    @Override
    public double getSimilarity(List<? extends Number> thisList, List<? extends Number> thatList) {
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
}
