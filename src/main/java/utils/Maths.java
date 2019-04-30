package utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chenglong Ma
 */
public class Maths {
    public static double mean(List<? extends Number> numbers) {
        double sum = 0.0;
        int count = 0;
        for (Number d : numbers) {
            if (!Double.isNaN(d.doubleValue())) {
                sum += d.doubleValue();
                count++;
            }
        }
        return sum / count;
    }

    public static List<int[]> combinations(int n) {
        List<int[]> combs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                combs.add(new int[]{i, j});
            }
        }
        return combs;
    }
}
