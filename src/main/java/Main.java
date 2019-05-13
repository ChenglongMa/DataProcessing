import matrix.DataFrame;
import matrix.SimilarityMatrix;
import utils.Command;

/**
 * @author Chenglong Ma
 */
public class Main {
    public static void main(String[] args) {
        Command cmd = new Command(args);
        String filename = cmd.getSourceFile();
        String resFilename = cmd.getResultFile();
        String sep = cmd.getSeparator();
        boolean isUser = cmd.isUserBased();
        System.out.printf("cmd params:\nfilename: %s\n", filename);
        System.out.printf("result filename: %s\n", resFilename);
        System.out.printf("sep: %s\n", sep);
        System.out.printf("user based: %s\n", isUser);
        int readRows = -1;
//        int readRows = args.length > 2 ? Integer.parseInt(args[2]) : -1;
        int headers = cmd.getNumOfHeaders();
        DataFrame df = new DataFrame();
        df.read(filename, sep, headers, readRows, isUser);
        System.out.printf("Ratings: %d\n", df.size());
        System.out.printf("Users: %d\n", df.rowSize());
        System.out.printf("Items: %d\n", df.columnSize());
        buildSim(df, resFilename);
    }

    private static void buildSim(DataFrame df, String resFilename) {

        System.out.println("Building Similarity Matrix...");
        SimilarityMatrix similarityMatrix = SimilarityMatrix.buildSimMat(df);
        System.out.println(similarityMatrix.size());
        System.out.println(similarityMatrix);
        similarityMatrix.toCSV(resFilename, false);
    }
}
