package db;
import org.junit.Test;


import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;


import static org.junit.Assert.assertEquals;

/**
 * Created by shivanesabharwal on 2/28/17.
 */
public class Table {
    private Database citizen;
    private String name;
    private ArrayList<Row> tableRows;
    private ArrayList<Column> tableColumns;

    //taken from STACKOVERFLOW
    //http://stackoverflow.com/questions/9568403/how-to-store-an-array-of-pairs-in-java
    public static class MyPair<T>
    {
        private T first;
        private T second;

        public MyPair(T aKey, T aValue)
        {
            first   = aKey;
            second = aValue;
        }

        public T getFirst()   { return first; }
        public T getSecond() { return second; }
    }



    public Table(String nameReal, ArrayList<String> columnNames, ArrayList<String> typeNames) {
        tableColumns = new ArrayList<>();
        tableRows = new ArrayList<>();
        if (columnNames.size() <= 0) {
            throw new RuntimeException("ERROR: You cannot create a table with 0 columns");
        }
        this.name = nameReal;
        for (int i = 0; i < columnNames.size(); i++) {
            Column toAdd = new Column(columnNames.get(i), typeNames.get(i));
            tableColumns.add(toAdd);
        }
    }


    

    public String getName() { return name;}

    public Table join(Table a, Table b) {

        //check if any column names are in common
        Boolean inCommon = false;
        ArrayList<String> matchingNames = new ArrayList<>();
        ArrayList<String> matchingTypes = new ArrayList<>();
        for (Column c : a.tableColumns) {
            for (Column x : b.tableColumns) {
                if (c.getColumnName().equals(x.getColumnName())) {
                    inCommon = true;
                    matchingNames.add(c.getColumnName());
                    matchingTypes.add(c.getColumnType());
                }
            }
        }
        if (inCommon) {
            //check for matching column indexes
            Row firstRow = a.tableRows.get(0);
            Row otherFirstRow = b.tableRows.get(0);
            ArrayList<MyPair> indexes = new ArrayList<>();

            for (String s : matchingNames) {
                Column currentCol = firstRow.getColumn(s);
                Column otherCurrent = otherFirstRow.getColumn(s);
                if (firstRow.mapIndex.containsKey(currentCol) && otherFirstRow.mapIndex.containsKey(otherCurrent)) {
                    MyPair<Integer> indexPair = new MyPair<>(firstRow.mapIndex.get(currentCol),
                            otherFirstRow.mapIndex.get(otherCurrent));
                    indexes.add(indexPair);
                }
            }


            //We now know where to look.
            /* TO DO:
                1. Iterate through all of the rows in table a and determine which rows have matching data. Store the rows.
                    *If no rows have matching data (size of storage == 0), return an empty table
                2. Create new rows with data from table a hstacked with data from table b
                3. Create new table
                4. Insert Rows
            */

            //check for matching data
            Boolean allMatch = false;
            ArrayList<MyPair> rowsToBeJoined = new ArrayList<>();

            for (Row r : a.tableRows) {
                for (Row l : b.tableRows) {
                    for (MyPair x : indexes) {
                        if (r.entries.get((Integer) x.getFirst()).equals(l.entries.get((Integer) x.getSecond()))) {
                            allMatch = true;
                            MyPair<Row> matchingRows = new MyPair<>(r, l);
                        } else {
                            allMatch = false;
                        }
                    }
                    if (allMatch) {
                        MyPair<Row> matchingRows = new MyPair<>(r, l);
                        rowsToBeJoined.add(matchingRows);
                    }
                }
            }


            /*need to put together the column names and type names in order to pass
             to table constructor*/
            ArrayList<String> columnNames = new ArrayList<>();
            ArrayList<String> typeNames = new ArrayList<>();

            // adds matching names first as those columns come first in the joined table.
            for (String x : matchingNames) {
                columnNames.add(x);
            }
            for (String x : matchingTypes) {
                typeNames.add(x);
            }
            for (Column c : a.tableColumns) {
                if (!columnNames.contains(c.getColumnName())) {
                    columnNames.add(c.getColumnName());
                    typeNames.add(c.getColumnType());
                }
            }
            for (Column c : b.tableColumns) {
                if (!columnNames.contains(c.getColumnName())) {
                    columnNames.add(c.getColumnName());
                    typeNames.add(c.getColumnType());
                }
            }

            Table joinedTable = new Table("random", columnNames, typeNames);

            /* table has been constructed. now, we need to construct the joined rows. */
            ArrayList<ArrayList> newRowArrayLists = new ArrayList<>();
            //first add all the data of joined rows
            //adds an array list of strings of the new joined rows to newRows
            for (int i = 0; i < rowsToBeJoined.size(); i++) {

                //new array list that will be the actual joined row's data
                ArrayList<String> actualJoin = new ArrayList<>();

                //get the first and second row in the first pair of rows
                Row firstR = (Row) rowsToBeJoined.get(i).getFirst();
                Row secondR = (Row) rowsToBeJoined.get(i).getSecond();

                for (MyPair x : indexes) {
                    // indexOfData is where the matching data is stored in the row
                    int indexOfData = (Integer) x.getFirst();

                    //add the matching data to the row array list
                    actualJoin.add(firstR.entries.get(indexOfData));

                }

                for (int k = 0; k < firstR.entries.size(); k++) {
                    Boolean ifSame = false;
                    for (MyPair x : indexes) {
                        if (k == (Integer) x.getFirst()) {
                            ifSame = true;
                        }
                    }
                    if (!ifSame) {
                        actualJoin.add(firstR.entries.get(k));
                    }
                }

                for (int k = 0; k < secondR.entries.size(); k++) {
                    Boolean ifSame = false;
                    for (MyPair x : indexes) {
                        if (k == (Integer) x.getSecond()) {
                            ifSame = true;
                        }
                    }
                    if (!ifSame) {
                        actualJoin.add(secondR.entries.get(k));
                    }
                }
                newRowArrayLists.add(actualJoin);
            }

            /*
            ArrayList<Row> newRows = new ArrayList<>();
            for (ArrayList temp : newRowArrayLists) {
                Row newAdd = new Row(temp, joinedTable);
                newRows.add(newAdd);
            }
            */

            for (ArrayList row : newRowArrayLists) {
                joinedTable.insertRow(row);
            }

            //citizen.insertTable(joinedTable);
            return joinedTable;
        } else {
            //Cartesian product
            ArrayList<String> newColumnNames = new ArrayList<>();
            ArrayList<String> newTypeNames = new ArrayList<>();
            for (Column c : a.tableColumns) {
                newColumnNames.add(c.getColumnName());
                newTypeNames.add(c.getColumnType());
            }
            for (Column d : b.tableColumns) {
                newColumnNames.add(d.getColumnName());
                newTypeNames.add(d.getColumnType());
            }
            Table cartesianJoin = new Table("rand", newColumnNames, newTypeNames);

            for (Row r : a.tableRows) {
                for (Row l : b.tableRows) {
                    ArrayList<String> cartJoin = new ArrayList<>();
                    for (String x : r.entries) {
                        cartJoin.add(x);
                    }
                    for (String m : l.entries) {
                        cartJoin.add(m);
                    }
                    cartesianJoin.insertRow(cartJoin);
                }
            }
            return cartesianJoin;
        }
    }

    public String printTable() {
        return toString();
    }

    public String insertRow(ArrayList<String> row) {
//        if (!citizen.databaseTables.contains(table)) {
//            throw new RuntimeException("ERROR: You cannot insert into a table that does not exist.");
//        }

        Row toBeInserted = new Row(row, this);
        tableRows.add(toBeInserted);
        return "";
    }



    @Override
    public String toString() {
        String stringRep = "";
        for (Column x: tableColumns) {
            stringRep += x.toString() + ",";
        }
        stringRep = stringRep.substring(0, stringRep.length() - 1);
        stringRep += "\n";
        for (Row y: tableRows) {
            stringRep += y.toString() + "\n";
        }
        return stringRep.substring(0,stringRep.length() - 1);
    }

    public ArrayList<Column> getTableColumns() {
        return tableColumns;
    }

    public ArrayList<Row> getTableRows() {
        return tableRows;
    }

    public static class TestThisShit {


        @Test
        public void test2(){
            ArrayList<String> columnNames = new ArrayList<>();
            columnNames.add("x");
            columnNames.add("y");
            columnNames.add("a");

            ArrayList<String> columnTypes = new ArrayList<>();
            columnTypes.add("int");
            columnTypes.add("int");
            columnTypes.add("int");

            Table test = new Table("test", columnNames, columnTypes);

            ArrayList<String> a = new ArrayList<>();
            a.add("1");
            a.add("2");
            a.add("1");

            ArrayList<String> b = new ArrayList<>();
            b.add("3");
            b.add("4");
            b.add("6");

            test.insertRow(a);
            test.insertRow(b);

            ArrayList<String> columnNames2 = new ArrayList<>();
            columnNames2.add("x");
            columnNames2.add("y");
            columnNames2.add("b");

            ArrayList<String> columnTypes2 = new ArrayList<>();
            columnTypes2.add("int");
            columnTypes2.add("int");
            columnTypes2.add("int");

            Table test2 = new Table("test2", columnNames2, columnTypes2);

            ArrayList<String> a2 = new ArrayList<>();
            a2.add("1");
            a2.add("3");
            a2.add("4");

            ArrayList<String> b2 = new ArrayList<>();
            b2.add("3");
            b2.add("4");
            b2.add("7");

            test2.insertRow(a2);
            test2.insertRow(b2);

            System.out.println(test.join(test, test2));

        }


    }


    public static void main(String[] args) {
        System.out.println();

    }
}
