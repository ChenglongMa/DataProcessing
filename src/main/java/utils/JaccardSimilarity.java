package utils;

import java.util.List;

/**
 * @author Chenglong Ma
 */
public class JaccardSimilarity implements Similarity {
    private static JaccardSimilarity instance;

    private JaccardSimilarity() {
        // for singleton pattern
    }

    public static synchronized JaccardSimilarity getInstance() {
        if (instance == null) {
            instance = new JaccardSimilarity();
        }
        return instance;
    }

    @Override
    public double getSimilarity(List<? extends Number> thisList, List<? extends Number> thatList) {
        if (thisList == null || thatList == null || thisList.size() < 1 || thatList.size() < 1 || thisList.size() != thatList.size()) {
            return Double.NaN;
        }
        int size = thisList.size();
        double intersection = 0D;
        for (int i = 0; i < size; i++) {
            Number thisVal = thisList.get(i);
            Number thatVal = thatList.get(i);
            if (thisVal.equals(thatVal)) {
                intersection++;
            }
        }
        return intersection / size;
    }
}
