package utils;

import java.util.List;

/**
 * @author Chenglong Ma
 */
public interface Similarity {
    double getSimilarity(List<? extends Number> thisList, List<? extends Number> thatList);
}
