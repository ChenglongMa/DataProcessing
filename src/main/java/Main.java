import matrix.DataFrame;
import matrix.SimilarityMatrix;
import utils.Command;

import java.util.Map;
import java.util.Set;

/**
 * @author Chenglong Ma
 */
public class Main {
    public static void main(String[] args) {
        Command cmd = new Command(args);
        String trainFile = cmd.getTrainFile();
        String testFile = cmd.getTestFile();
        String resFilename = cmd.getResultFile();
        String sep = cmd.getSeparator();
        boolean isUser = cmd.isUserBased();
        boolean isSimOnly = cmd.isSimOnly();
        System.out.printf("cmd params:\nfilename: %s\n", trainFile);
        System.out.printf("result filename: %s\n", resFilename);
        System.out.printf("sep: %s\n", sep);
        System.out.printf("user based: %s\n", isUser);
        int readRows = -1;
//        int readRows = args.length > 2 ? Integer.parseInt(args[2]) : -1;
        int headers = cmd.getNumOfHeaders();
        DataFrame df = new DataFrame();
        df.read(trainFile, sep, headers, readRows);
        System.out.printf("Ratings: %d\n", df.size());
        System.out.printf("Users: %d\n", df.rowSize());
        System.out.printf("Items: %d\n", df.columnSize());
        if (isSimOnly) {
            buildSim(df, resFilename);
        } else {
            df.readTest(testFile, sep, headers, readRows);
//            buildFeatureSet(df, isUser, resFilename);
//            CountCollector.build(df,isUser,resFilename);
            Set<Integer> indices = df.getTestUserInnerIds();
            System.out.println(indices.size());
            int trueVal = 0, falseVal = 0;
            for (Integer index : indices) {
                Map ratings = df.getUserRatings(index);
                if (ratings == null || ratings.isEmpty()) {
                    falseVal++;
                } else {
                    trueVal++;
                }
            }
            System.out.println("True: " + trueVal);
            System.out.println("False: " + falseVal);
        }
    }

    private static void buildSim(DataFrame df, String resFilename) {

        System.out.println("Building Similarity Matrix...");
        SimilarityMatrix similarityMatrix = SimilarityMatrix.buildSimMat(df);
        System.out.println(similarityMatrix.size());
        System.out.println(similarityMatrix);
        similarityMatrix.toCSV(resFilename, false);
    }

}
