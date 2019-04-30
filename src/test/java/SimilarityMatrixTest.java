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
        int inner1 = df.getInnerId("3", true);
        int inner2 = df.getInnerId("4", true);
        CoFeature sim = SimilarityMatrix.getCorrelation(df.getRow(inner1), df.getRow(inner2));
        System.out.println(sim);
        SimilarityMatrix sims = SimilarityMatrix.buildSimMat(df, true);
        System.out.println(sims);
        Assert.assertEquals(8, sims.size());
        CoFeature simInMat = sims.get("3", "4");
        System.out.println(simInMat);
        Assert.assertEquals(sim.similarity, simInMat.similarity, 0.01);
        //14624338 + 3612217
        //18237780
    }

    @Test
    public void testSave() {
        SimilarityMatrix sims = SimilarityMatrix.buildSimMat(df, true);
        System.out.println(sims);
        sims.toCSV("tmp.csv",false);
        Assert.assertEquals(8, sims.size());
    }

//    public void run() {
//        // 填充原始数据，nums中填充0-9 10个数
//        IntStream.range(0, 10).forEach(nums::add);
//        //实现Collector接口
//        result = nums.stream().parallel().collect(new Collector<Integer, Container, Set<Double>>() {
//
//            @Override
//            public Supplier<Container> supplier() {
//                return Container::new;
//            }
//
//            @Override
//            public BiConsumer<Container, Integer> accumulator() {
//                return Container::accumulate;
//            }
//
//            @Override
//            public BinaryOperator<Container> combiner() {
//                return Container::combine;
//            }
//
//            @Override
//            public Function<Container, Set<Double>> finisher() {
//                return Container::getResult;
//            }
//
//            @Override
//            public Set<Collector.Characteristics> characteristics() {
//                // 固定写法
//                return Collections.emptySet();
//            }
//        });
//    }
    @Test
    public void testParallel() {

    }

    @Test
    public void getSimilarity() {
    }
//    class Container {
//        public Set<Double> set;
//
//        public Container() {
//            this.set = new HashSet<>();
//        }
//
//        public Container accumulate(int num) {
//            this.set.add(compute.compute(num));
//            return this;
//        }
//
//        public Container combine(Container container) {
//            this.set.addAll(container.set);
//            return this;
//        }
//
//        public Set<Double> getResult() {
//            return this.set;
//        }
//    }
}