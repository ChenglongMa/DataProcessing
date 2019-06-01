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
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Chenglong Ma
 */
public class DataFrame {
    private final Table<Integer, Integer, Double> userBasedData, itemBasedData;
    private final Table<Integer, Integer, Double> testUserBasedData, testItemBasedData;
    private final BiMap<String, Integer> innerUserMap, innerItemMap;
    private final Map<Integer, Double> userRatingSum, itemRatingSum;
//    private final BiMap<Integer, Integer> testUIIds;

    @Deprecated
    private List<Map<Integer, Double>> rowVectors, columnVectors;
    private long ratingCount = 0;
    private double ratingSum = 0;

    public DataFrame() {
        userBasedData = TreeBasedTable.create();
        itemBasedData = TreeBasedTable.create();
        testUserBasedData = TreeBasedTable.create();
        testItemBasedData = TreeBasedTable.create();
        innerUserMap = HashBiMap.create();
        innerItemMap = HashBiMap.create();
        userRatingSum = new HashMap<>();
        itemRatingSum = new HashMap<>();
//        testUIIds = HashBiMap.create();
    }

    /**
     * @param filename
     * @param sep
     * @param headers  how many rows are headers, 0 for no header row
     * @param readRows read rows if readRows > 0; read nothing if readRows == 0; read all if readRows < 0
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

    public void readTest(String filename, String sep, int headers, int readRows) {
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
                addTest(eachRow);
                readRows--;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addTest(String... eachRow) {
        if (eachRow.length < 3) {
            return;
        }
        String u_id = eachRow[0];
        String i_id = eachRow[1];
        Integer u_inner_id = innerUserMap.get(u_id);
        Integer i_inner_id = innerItemMap.get(i_id);
        if (u_inner_id == null || i_inner_id == null) {
            // filter out nonexistent keys
            return;
        }
        double rating = Double.parseDouble(eachRow[2]);
        testUserBasedData.put(u_inner_id, i_inner_id, rating);
        testItemBasedData.put(i_inner_id, u_inner_id, rating);
//        testUIIds.put(u_inner_id, i_inner_id);
    }

    public Set<Integer> getTestUserInnerIds() {
        return testUserBasedData.rowKeySet();
    }


    private void add(String... eachRow) {
        if (eachRow.length < 3) {
            return;
        }
        String u_id = eachRow[0];
        String i_id = eachRow[1];
        int u_inner_id = putIfAbsent(u_id, innerUserMap);
        int i_inner_id = putIfAbsent(i_id, innerItemMap);
        double rating = Double.parseDouble(eachRow[2]);
        ratingSum += rating;
        ratingCount++;
        userBasedData.put(u_inner_id, i_inner_id, rating);
        itemBasedData.put(i_inner_id, u_inner_id, rating);
        addUserRatingSum(u_inner_id, rating);
        addItemRatingSum(i_inner_id, rating);
    }

    private void addUserRatingSum(int innerId, double rating) {
        userRatingSum.merge(innerId, rating, Double::sum);
    }

    private void addItemRatingSum(int innerId, double rating) {
        itemRatingSum.merge(innerId, rating, Double::sum);
    }

    public double getUserMean(int innerId) {
        return userRatingSum.get(innerId) / userRatingSum.size();
    }

    public int getUserCount(int innerId) {
        return userBasedData.row(innerId).size();
    }

    public int getItemCount(int innerId) {
        return itemBasedData.row(innerId).size();
    }

    public double getItemMean(int innerId) {
        return itemRatingSum.get(innerId) / itemRatingSum.size();
    }

    public double getUserMeanOff(int innerId) {
        return getUserMean(innerId) - getAverageRating();
    }

    public double getItemMeanOff(int innerId) {
        return getItemMean(innerId) - getAverageRating();
    }

    private int putIfAbsent(String realId, BiMap<String, Integer> innerMap) {
        int newId = innerMap.size();
        Integer inner_id = innerMap.putIfAbsent(realId, newId);
        if (inner_id == null) {
            inner_id = newId;
        }
        return inner_id;
    }

    public double getAverageRating() {
        return ratingSum / ratingCount;
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
        return userBasedData.size();
    }

    public int rowSize() {
        return innerUserMap.size();
    }

    public SortedSet<Integer> getRowIndices() {
        return (SortedSet<Integer>) userBasedData.rowKeySet();
    }

    public SortedSet<Integer> getColumnIndices() {
        return (SortedSet<Integer>) userBasedData.columnKeySet();
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
        rowVectors = indices.parallelStream().map(userBasedData::row).collect(Collectors.toList());
    }

    @Deprecated
    public void buildColumnVectors() {
        if (columnVectors != null) {
            return;
        }
        Set<Integer> indices = getColumnIndices();
        columnVectors = indices.parallelStream().map(userBasedData::column).collect(Collectors.toList());
    }

    public Map<Integer, Double> getUserRatings(int innerId) {
        return userBasedData.row(innerId);
    }

    public Map<Integer, Double> getTestUserRatings(int innerId) {
        return testUserBasedData.row(innerId);
    }

    public Map<Integer, Double> getItemRatings(int innerId) {
        return itemBasedData.row(innerId);
    }


    public Map<Integer, Double> getTestItemRatings(int innerId) {
        return testItemBasedData.row(innerId);
    }


    public Map<Integer, Double> getColumn(int innerId) {
        return userBasedData.column(innerId);
    }

    public void buildUserFeature() {

    }

    @Override
    public String toString() {
        return userBasedData.toString();
    }
}
