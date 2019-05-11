import matrix.DataFrame;
import matrix.SimilarityMatrix;
import utils.Command;

/**
 * @author Chenglong Ma
 */
public class Main {
    public static void main(String[] args) {
        DataFrame df = new DataFrame();
        Command cmd = new Command(args);
        String filename = cmd.getSourceFile();
        String resFilename = cmd.getResultFile();
        String sep = cmd.getSeparator();
        boolean isUser = cmd.isUserBased();
        int readRows = -1;
//        int readRows = args.length > 2 ? Integer.parseInt(args[2]) : -1;
        int headers = cmd.getNumOfHeaders();
        df.read(filename, sep, headers, readRows);
        System.out.printf("Ratings: %d\n", df.size());
        System.out.printf("Users: %d\n", df.rowSize());
        System.out.printf("Items: %d\n", df.columnSize());
        buildSim(df, resFilename, isUser);
    }

    private static void buildSim(DataFrame df, String resFilename, boolean isUser) {

        System.out.println("Building Similarity Matrix...");
        SimilarityMatrix similarityMatrix = SimilarityMatrix.buildSimMat(df, isUser);
        System.out.println(similarityMatrix.size());
        System.out.println(similarityMatrix);
        similarityMatrix.toCSV(resFilename, false);
    }
}
