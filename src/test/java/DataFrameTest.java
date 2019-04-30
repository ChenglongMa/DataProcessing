import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Chenglong Ma
 */
public class DataFrameTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
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