package db;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by shivanesabharwal on 2/27/17.
 */
public class Row {
    public HashMap<Column, Integer> mapIndex;
    public ArrayList<String> entries;
    public ArrayList<Column> columnList;
    public Table part;

    public Row(ArrayList<String> rowEntries, Table t) {
        part = t;
        mapIndex = new HashMap<>();
        columnList = t.getTableColumns();
        entries = rowEntries;
        for (int i = 0; i < entries.size(); i++) {
            //System.out.println(entries.size());
            Column theColumn = columnList.get(i);
            mapIndex.put(theColumn, i);
        }
    }

    public void addColumn(String toAdd) {
        //mapIndex.put(entries.size(), getColumn());
        entries.add(toAdd);
    }

    public Column getColumn(String name) {
        for (Column c : columnList) {
            if (name.equals(c.getColumnName())) {
                return c;
            }
        }
        throw new RuntimeException("Error: No column with that name");
    }


    @Override
    public String toString() {
        String result = "";
//        for (String x : entries) {
//            result += x + ",";
//        }
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).equals("NOVALUE")) {
                result += entries.get(i) + ",";
            } else if (part.getTableColumns().get(i).getColumnType().equals("float")) {
                String temp = entries.get(i);
                Double test = Double.parseDouble(temp);
                String better = Double.toString(test);
                int dotIndex = better.indexOf(".");
                int lengthAfterDot = better.substring(dotIndex + 1, better.length()).length();
                if (lengthAfterDot < 3) {
                    String zeros = "00000000000";
                    better += zeros;
                }
                result += better.substring(0, dotIndex + 4) + ",";

            } else {
                result += entries.get(i) + ",";
            }
        }
        return result.substring(0, result.length() - 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Row row = (Row) o;

        if (mapIndex != null ? !mapIndex.equals(row.mapIndex) : row.mapIndex != null) return false;
        if (entries != null ? !entries.equals(row.entries) : row.entries != null) return false;
        return columnList != null ? columnList.equals(row.columnList) : row.columnList == null;
    }

    @Override
    public int hashCode() {
        int result = mapIndex != null ? mapIndex.hashCode() : 0;
        result = 31 * result + (entries != null ? entries.hashCode() : 0);
        result = 31 * result + (columnList != null ? columnList.hashCode() : 0);
        return result;
    }

    public static void main(String[] args) {
        String d = "09.8333";
        Double test = Double.parseDouble(d);
        System.out.println(test);
    }

}
