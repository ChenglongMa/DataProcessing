import matrix.CoFeature;
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
    public void buildSimilarityMatrix() {
        int inner1 = df.getInnerId("1", true);
        int inner2 = df.getInnerId("2", true);
        CoFeature sim = SimilarityMatrix.getCoFeature(df.getUserRatings(inner1), df.getUserRatings(inner2));
        System.out.println(sim);
        SimilarityMatrix sims = SimilarityMatrix.buildSimMat(df);
        System.out.println(sims);
        Assert.assertEquals(6, sims.size());
        CoFeature simInMat = sims.get("1", "2");
        System.out.println(simInMat);
        Assert.assertEquals(sim.pcc, simInMat.pcc, 0.01);
        //14624338 + 3612217
        //18237780
    }

    @Test
    public void testSave() {
        boolean isUser = false;
        SimilarityMatrix sims = SimilarityMatrix.buildSimMat(df);
        System.out.println(sims);
        sims.toCSV("tmp.csv", false);
        Assert.assertEquals(6, sims.size());
    }

    @Test
    public void testParallel() {

    }

    @Test
    public void getSimilarity() {
    }
}