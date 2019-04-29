/**
 * @author Chenglong Ma
 */
public class Main {
    public static void main(String[] args) {
        DataFrame df = new DataFrame();
        String filename = args.length > 0 ? args[0] : null;
        String sep = args.length > 1 ? args[1] : "[ \t,]+";
        df.read(filename, sep);
        System.out.println(df.size());
        System.out.println(df.rowSize());
        System.out.println(df.columnSize());
        SimilarityMatrix similarityMatrix = SimilarityMatrix.buildSimilarityMatrix(df, true);
        System.out.println(similarityMatrix.getData().size());
        System.out.println(similarityMatrix.getData().rowKeySet().size());
        System.out.println(similarityMatrix.getData().columnKeySet().size());
    }
}
