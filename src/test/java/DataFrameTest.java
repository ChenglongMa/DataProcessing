import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import matrix.DataFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author Chenglong Ma
 */
public class DataFrameTest {

    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        df = new DataFrame();
        //-src "C:\\My Current Projects\\Python\\knn-aware\\data\\ml-1m\\ratings.dat" -res "sim.csv" -sep "::" -header 0
        String filename = "C:\\My Current Projects\\Python\\knn-aware\\data\\ml-1m\\ratings.dat";
        String sep = "::";
        int headers = 0;
        int rows = -1;
        df.read(filename, sep, headers, rows, false);
    }

    @After
    public void tearDown() throws Exception {
        df = null;
    }

    @Test
    public void testRowFetch() {
        int count = df.rowSize();
        long start = System.currentTimeMillis();
        long size = 0;
        for (int i = 0; i < count; i++) {
            Map<Integer, Double> row = df.getRow(i);
            size += row.size();
        }
        long end = System.currentTimeMillis();
        System.out.printf("Size: %d\nTime cost: %.9f s\n", size, (end - start) / 1000D);
    }

    @Test
    public void testColumnFetch() {
        int count = df.columnSize();
        long start = System.currentTimeMillis();
        long size = 0;
        for (int i = 0; i < count; i++) {
            Map<Integer, Double> col = df.getColumn(i);
            size += col.size();
        }
        long end = System.currentTimeMillis();
        System.out.printf("Size: %d\nTime cost: %.9f s\n", size, (end - start) / 1000D);
    }

    @Test
    public void testTable() {
        Table<Integer, Integer, Double> data = TreeBasedTable.create();
        data.put(1, 2, 3.2);
        data.put(4, 3, 3.2);
        data.put(3, 4, 5.0);
        data.put(1, 4, 5.0);
        System.out.println(data);
//        System.out.println(data.rowKeySet());
//        System.out.println(data.columnKeySet());
//        System.out.println(data.rowMap());
//        System.out.println(data.columnMap());
    }
}