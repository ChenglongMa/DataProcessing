import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import okio.BufferedSource;
import okio.Okio;
import okio.Source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Chenglong Ma
 */
public class DataFrame {
    private final Table<Integer, Integer, Double> data;

    public DataFrame() {
        data = HashBasedTable.create();
    }

    public Table<Integer, Integer, Double> getData() {
        return data;
    }

    public void read(String filename, String sep) {
        Pattern pattern = Pattern.compile(sep);
        try (Source fileSource = Okio.source(new File(filename));
             BufferedSource bufferedSource = Okio.buffer(fileSource)) {
            String temp;
            while ((temp = bufferedSource.readUtf8Line()) != null) {
                if ("".equals(temp.trim())) {
                    break;
                }
                String[] eachRow = pattern.split(temp);
                add(eachRow);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void add(String... eachRow) {
        assert eachRow.length >= 3;
        int u_id = Integer.parseInt(eachRow[0]);
        int i_id = Integer.parseInt(eachRow[1]);
        double rating = Double.parseDouble(eachRow[2]);
        data.put(u_id, i_id, rating);
    }

    public int size() {
        return data.size();
    }

    public int rowSize() {
        return data.rowKeySet().size();
    }

    public List<Integer> getRowIndices() {
        return new ArrayList<>(data.rowKeySet());
    }

    public List<Integer> getColumnIndices() {
        return new ArrayList<>(data.columnKeySet());
    }

    public int columnSize() {
        return data.columnKeySet().size();
    }

    public Map<Integer, Double> getRow(int index) {
        return data.rowMap().get(index);
    }

    public Map<Integer, Double> getColumn(int index) {
        return data.columnMap().get(index);
    }

}
