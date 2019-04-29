package utils;

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
}
