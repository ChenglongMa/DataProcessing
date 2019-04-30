package matrix;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import okio.BufferedSource;
import okio.Okio;
import okio.Source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Chenglong Ma
 */
public class DataFrame {
    private final Table<Integer, Integer, Double> data;
    private final BiMap<String, Integer> innerUserMap, innerItemMap;
    @Deprecated
    private List<Map<Integer, Double>> rowVectors, columnVectors;

    public DataFrame() {
        data = TreeBasedTable.create();
        innerUserMap = HashBiMap.create();
        innerItemMap = HashBiMap.create();
    }

    public Table<Integer, Integer, Double> getData() {
        return data;
    }

    /**
     * @param filename
     * @param sep
     * @param headers
     * @param readRows read rows if readRows > 0; read nothing if readRows == 0;
     */
    public void read(String filename, String sep, int headers, int readRows) {
        Pattern pattern = Pattern.compile(sep);
        try (Source fileSource = Okio.source(new File(filename));
             BufferedSource bufferedSource = Okio.buffer(fileSource)) {
            String temp;
            while ((temp = bufferedSource.readUtf8Line()) != null && readRows != 0) {
                if ("".equals(temp.trim())) {
                    break;
                }
                if (headers-- > 0) {
                    continue;
                }
                String[] eachRow = pattern.split(temp);
                add(eachRow);
                readRows--;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void add(String... eachRow) {
        assert eachRow.length >= 3;
        String u_id = eachRow[0];
        String i_id = eachRow[1];
        int u_inner_id = putIfAbsent(u_id, innerUserMap);
        int i_inner_id = putIfAbsent(i_id, innerItemMap);
        double rating = Double.parseDouble(eachRow[2]);
        data.put(u_inner_id, i_inner_id, rating);
    }

    private int putIfAbsent(String realId, BiMap<String, Integer> innerMap) {
        int newId = innerMap.size();
        Integer inner_id = innerMap.putIfAbsent(realId, newId);
        if (inner_id == null) {
            inner_id = newId;
        }
        return inner_id;
    }

    public int getInnerId(String realId, boolean isUser) {
        BiMap<String, Integer> innerMap = isUser ? innerUserMap : innerItemMap;
        return innerMap.get(realId);
    }

    public String getRealId(int innerId, boolean isUser) {
        BiMap<String, Integer> innerMap = isUser ? innerUserMap : innerItemMap;
        return innerMap.inverse().get(innerId);
    }

    public int size() {
        return data.size();
    }

    public int rowSize() {
        return innerUserMap.size();
    }

    public SortedSet<Integer> getRowIndices() {
        return (SortedSet<Integer>) data.rowKeySet();
    }

    public SortedSet<Integer> getColumnIndices() {
        return (SortedSet<Integer>) data.columnKeySet();
    }

    public int columnSize() {
        return innerItemMap.size();
    }

    @Deprecated
    public void buildRowVectors() {
        if (rowVectors != null) {
            return;
        }
        Set<Integer> indices = getRowIndices();
        rowVectors = indices.parallelStream().map(data::row).collect(Collectors.toList());
    }

    @Deprecated
    public void buildColumnVectors() {
        if (columnVectors != null) {
            return;
        }
        Set<Integer> indices = getColumnIndices();
        columnVectors = indices.parallelStream().map(data::column).collect(Collectors.toList());
    }

    public Map<Integer, Double> getRow(int innerId) {
//        buildRowVectors();
        return data.row(innerId);
    }

    public Map<Integer, Double> getColumn(int innerId) {
//        buildColumnVectors();
        return data.column(innerId);
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
