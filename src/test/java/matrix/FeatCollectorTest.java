package matrix;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author Chenglong Ma
 */
public class FeatCollectorTest {
    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        df = new DataFrame();
        //-src "C:\\My Current Projects\\Python\\knn-aware\\data\\ml-1m\\ratings.dat" -res "sim.csv" -sep "::" -header 0
        String filename = "C:\\Projects\\LibRecApp\\lib\\librec\\data\\test\\datamodeltest\\matrix4by4-date.txt";
        String sep = "[ \t,]+";
        int headers = 0;
        int rows = -1;
        df.read(filename, sep, headers, rows);
    }

    @After
    public void tearDown() throws Exception {
        df = null;
    }

    @Test
    public void buildFeatureSet() {
        List<Row> feats = FeatCollector.buildFeatureSet(df, true);
        System.out.println(feats);
    }

    @Test
    public void buildFeats() {
        FeatCollector.buildFeatureSet(df, true, "tmp.csv");
    }
}