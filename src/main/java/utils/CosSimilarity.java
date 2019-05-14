package utils;

import java.util.List;

/**
 * @author Chenglong Ma
 */
public class CosSimilarity implements Similarity {
    private static CosSimilarity instance;

    private CosSimilarity() {
        // for singleton pattern
    }

    public static synchronized CosSimilarity getInstance() {
        if (instance == null) {
            instance = new CosSimilarity();
        }
        return instance;
    }

    @Override
    public double getSimilarity(List<? extends Number> thisList, List<? extends Number> thatList) {
        if (thisList == null || thatList == null || thisList.size() < 1 || thatList.size() < 1 || thisList.size() != thatList.size()) {
            return Double.NaN;
        }

        double innerProduct = 0.0, thisPower2 = 0.0, thatPower2 = 0.0;
        for (int i = 0; i < thisList.size(); i++) {
            innerProduct += thisList.get(i).doubleValue() * thatList.get(i).doubleValue();
            thisPower2 += thisList.get(i).doubleValue() * thisList.get(i).doubleValue();
            thatPower2 += thatList.get(i).doubleValue() * thatList.get(i).doubleValue();
        }
        return innerProduct / Math.sqrt(thisPower2 * thatPower2);
    }
}
