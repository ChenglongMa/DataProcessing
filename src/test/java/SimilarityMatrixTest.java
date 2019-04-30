import matrix.DataFrame;
import matrix.SimilarityMatrix;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Chenglong Ma
 */
public class SimilarityMatrixTest {

    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        df = new DataFrame();
        //-src "C:\\My Current Projects\\Python\\knn-aware\\data\\ml-1m\\ratings.dat" -res "sim.csv" -sep "::" -header 0
        String filename = "C:\\My Current Projects\\Python\\knn-aware\\data\\ml-1m\\ratings.dat";
        String sep = "::";
        int headers = 0;
        int rows = 300;
        df.read(filename, sep, headers, rows);
    }

    @After
    public void tearDown() throws Exception {
        df = null;
    }

    @Test
    public void buildSimilarityMatrix() {
        int inner1 = df.getInnerId("1", true);
        int inner2 = df.getInnerId("2", true);
        String sim = SimilarityMatrix.getCorrelation(df.getRow(inner1), df.getRow(inner2));
        System.out.println(sim);
        SimilarityMatrix sims = SimilarityMatrix.buildSimilarityMatrix(df, true);
        System.out.println(sims);
        String simInMat = sims.get("1", "2");
        System.out.println(simInMat);
        Assert.assertEquals(sim, simInMat);
        //14624338 + 3612217
        //18237780
    }

    @Test
    public void testSave() {
        SimilarityMatrix sims = SimilarityMatrix.buildSimilarityMatrix(df, true);
        System.out.println(sims);
        sims.toCSV("tmp.csv");
    }

    @Test
    public void getCorrelation() {

    }

    @Test
    public void getSimilarity() {
    }
}