package utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Chenglong Ma
 */
public class JaccardSimilarityTest {

    @Test
    public void getSimilarity() {
        List<Integer> a = Arrays.asList(1, 0, 0);
        List<Integer> b = Arrays.asList(1, 1, 1);
        double sim = new JaccardSimilarity().getSimilarity(a, b);
        assertEquals(1 - 0.66, sim, 0.01);
    }

    @Test
    public void name() {
        double sim = calculateJaccardSimilarity("100", "111");
        System.out.println(sim);
    }

    private Double calculateJaccardSimilarity(CharSequence left, CharSequence right) {
        Set<String> intersectionSet = new HashSet<String>();
        Set<String> unionSet = new HashSet<String>();
        boolean unionFilled = false;
        int leftLength = left.length();
        int rightLength = right.length();
        if (leftLength == 0 || rightLength == 0) {
            return 0d;
        }

        for (int leftIndex = 0; leftIndex < leftLength; leftIndex++) {
            unionSet.add(String.valueOf(left.charAt(leftIndex)));
            for (int rightIndex = 0; rightIndex < rightLength; rightIndex++) {
                if (!unionFilled) {
                    unionSet.add(String.valueOf(right.charAt(rightIndex)));
                }
                if (left.charAt(leftIndex) == right.charAt(rightIndex)) {
                    intersectionSet.add(String.valueOf(left.charAt(leftIndex)));
                }
            }
            unionFilled = true;
        }
        return (double) intersectionSet.size() / (double) unionSet.size();
    }
}