package utils;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author Chenglong Ma
 */
public class MathsTest {

    @Test
    public void combinations() {
        Maths.combinations(5).forEach(v -> System.out.println(Arrays.toString(v)));
    }
}