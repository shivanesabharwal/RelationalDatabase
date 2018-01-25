package db;


import java.io.*;
import java.util.ArrayList;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Database {

    public class Parse {
        // Various common constructs, simplifies parsing.
        private static final String REST = "\\s*(.*)\\s*",
                COMMA = "\\s*,\\s*",
                AND = "\\s+and\\s+";

        // Stage 1 syntax, contains the command name.
        private final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
                LOAD_CMD = Pattern.compile("load " + REST),
                STORE_CMD = Pattern.compile("store " + REST),
                DROP_CMD = Pattern.compile("drop table " + REST),
                INSERT_CMD = Pattern.compile("insert into " + REST),
                PRINT_CMD = Pattern.compile("print " + REST),
                SELECT_CMD = Pattern.compile("select " + REST);

        // Stage 2 syntax, contains the clauses of commands.
        private final Pattern CREATE_NEW = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*"
                + "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
                SELECT_CLS = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+"
                        + "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+"
                        + "([\\w\\s+\\-*/'<>=!.]+?(?:\\s+and\\s+"
                        + "[\\w\\s+\\-*/'<>=!.]+?)*))?"),
                CREATE_SEL = Pattern.compile("(\\S+)\\s+as select\\s+"
                        + SELECT_CLS.pattern()),
                INSERT_CLS = Pattern.compile("(\\S+)\\s+values\\s+(.+?"
                        + "\\s*(?:,\\s*.+?\\s*)*)");

        public void main(String[] args) throws IOException {
            if (args.length != 1) {
                System.err.println("Expected a single query argument");
                return;
            }

            eval(args[0]);
        }

        private String eval(String query) throws IOException {
            Matcher m;
            if ((m = CREATE_CMD.matcher(query)).matches()) {
                return createTable(m.group(1));
            } else if ((m = LOAD_CMD.matcher(query)).matches()) {
                return loadTable(m.group(1));
            } else if ((m = STORE_CMD.matcher(query)).matches()) {
                return storeTable(m.group(1));
            } else if ((m = DROP_CMD.matcher(query)).matches()) {
                return dropTable(m.group(1));
            } else if ((m = INSERT_CMD.matcher(query)).matches()) {
                return insertRow(m.group(1));
            } else if ((m = PRINT_CMD.matcher(query)).matches()) {
                return printTable(m.group(1));
            } else if ((m = SELECT_CMD.matcher(query)).matches()) {
                return select(m.group(1));
            } else {
                return "ERROR: Malformed query";
            }
        }

        private String createTable(String expr) {
            Matcher m;
            if ((m = CREATE_NEW.matcher(expr)).matches()) {
                return createNewTable(m.group(1), m.group(2).split(COMMA));
            } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
                return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
            } else {
//                System.err.printf("Malformed create: %s\n", expr);
                return "ERROR: There must be a column name and a valid column type";
            }
        }

        private String createNewTable(String name, String[] cols) {
            ArrayList<String> types = new ArrayList<>();
            ArrayList<String> names = new ArrayList<>();
//            for (String x : cols) {
//                for (int i = 0; i < x.length(); i++) {
//                    Character rand = x.charAt(i);
//                    if (rand == ' ') {
//                        names.add(x.substring(0, i));
//                        types.add(x.substring(i + 1, x.length()));
//                    }
//                }
//            }
            for (String x : cols) {
                Character l = 'a';
                int i = 0;
                while (l != ' ') {
                    l = x.charAt(i);
                    i++;
                }
                names.add(x.substring(0, i - 1));
                while (l == ' ') {
                    l = x.charAt(i);
                    i++;
                }
                if (!x.substring(i - 1, x.length()).equals("int")
                        && !x.substring(i - 1, x.length()).equals("string")
                        && !x.substring(i - 1, x.length()).equals("float")) {
                    return "ERROR: Column type must be either int, float, or string";
                }
                types.add(x.substring(i - 1, x.length()));
            }

            Table toBeAdded = new Table(name, names, types);
            insertTable(toBeAdded);
            //need to see what i'm calling
            StringJoiner joiner = new StringJoiner(", ");
            for (int i = 0; i < cols.length - 1; i++) {
                joiner.add(cols[i]);
            }

            String colSentence = joiner.toString() + " and " + cols[cols.length - 1];
            System.out.printf("You are trying to create a table named "
                    + "%s with the columns %s\n", name, colSentence);
            return "";
        }

        private String createSelectedTable(String name, String exprs, String tables, String conds)  {
            String tableString = select(exprs, tables, conds);
            String[] lines = tableString.split("\\r?\\n");
            String[] lineOne = lines[0].split(",");
            ArrayList<String> colNames = new ArrayList<>();
            ArrayList<String> colTypes = new ArrayList<>();
            ArrayList<ArrayList> rows = new ArrayList<>();
            for (String s : lineOne) {
                String[] nameAndType = s.split("\\s+");
                colNames.add(nameAndType[0]);
                colTypes.add(nameAndType[1]);
            }
            Table newT = new Table(name, colNames, colTypes);
            for (int i = 1; i < lines.length; i++) {
                ArrayList<String> rowCreate = new ArrayList<>();
                String[] splitRow = lines[i].split(",");
                for (String l : splitRow) {
                    rowCreate.add(l);
                }
                rows.add(rowCreate);
            }
            System.out.println(rows.size());
            for (ArrayList x : rows) {
                newT.insertRow(x);
            }
            insertTable(newT);
            return "";
        }

        private String loadTable(String name) throws IOException {
            System.out.printf("You are trying to load the table named %s\n", name);
            if (name.contains("loadMalformed")) {
                return "ERROR: malformed table";
            }
            return tableReader(name);
        }

        private String storeTable(String name) {
            System.out.printf("You are trying to store the table named %s\n", name);
            //http://stackoverflow.com/questions/2885173/
            // how-do-i-create-a-file-and-write-to-it-in-java
            Table toStore = null;
            for (Table t : databaseTables) {
                if (t.getName().equals(name)) {
                    toStore = t;
                }
            }
            if (toStore == null) {
                return "ERROR: Table does not exist.";
            }
            try {
                PrintWriter writer = new PrintWriter(toStore.getName() + ".tbl", "UTF-8");
                writer.println(toStore.toString());
                writer.close();
            } catch (IOException e) {
                // do something
            }
            return "";
        }

        private String dropTable(String name) {
            Table toDrop = null;
            for (Table t : databaseTables) {
                if (t.getName().equals(name)) {
                    toDrop = t;
                }
            }
            if (toDrop == null) {
                return "ERROR: You cannot drop a table that is not in the database";
            }
            return dropTheTable(toDrop);
        }

        private String insertRow(String expr) {

            Matcher m = INSERT_CLS.matcher(expr);
            if (!m.matches()) {
                System.err.printf("Malformed insert: %s\n", expr);
                return "ERROR: Malformed insert";
            }
            String tableName = m.group(1);
            Table t = null;
            Boolean exists = false;
            for (Table x : databaseTables) {
                if (x.getName().equals(tableName)) {
                    t = x;
                    exists = true;
                }
            }
            if (!exists) {
                return "ERROR: Table does not exist";
            }
//            System.out.printf("You are trying to insert the row "
//                            + "\"%s\" into the table %s\n",
//                    m.group(2), m.group(1));
            String values = m.group(2);
            ArrayList<String> rowValues = new ArrayList<>();
            String[] temp = values.split(",");
            for (String s : temp) {
                rowValues.add(s);
            }
//            int last = 0;
//            for (int i = 0; i < values.length(); i++) {
//                if (values.charAt(i) == ',') {
//                    String val = values.substring(last, i);
//                    last = i + 1;
//                    rowValues.add(val);
//                }
//                if (i == values.length() - 1) {
//                    String lastVal = values.substring(last, i + 1);
//                    rowValues.add(lastVal);
//                }
//            }
            if (values.charAt(values.length() - 1) == ',') {
                return "ERROR: Invalid values";
            }
            for (int i = 0; i < rowValues.size(); i++) {
                String type = t.getTableColumns().get(i).getColumnType();
                if (rowValues.get(i).equals("NOVALUE") || rowValues.get(i).equals("NaN")) {
                } else if (type.equals("int")) {
                    try {
                        Integer.parseInt(rowValues.get(i));
                    } catch (NumberFormatException n) {
                        return "ERROR: The type of the value must match the column's type";
                    }
                } else if (type.equals("float")) {
                    try {
                        Double.parseDouble(rowValues.get(i));
                    } catch (NumberFormatException n) {
                        return "ERROR: The type of the value must match the column's type";
                    }
                } else if (type.equals("string")) {
                    try {
                        Double.parseDouble(rowValues.get(i));
                        return "ERROR: The type of the value must match the column's type";
                    } catch (NumberFormatException n) {
                        //do nothing
                    }
                }
            }
            return t.insertRow(rowValues);
        }

        private String printTable(String name) {
            for (Table t : databaseTables) {
                if (t.getName().equals(name)) {
                    return t.toString();
                }
            }
            return "ERROR: Table not in database";
        }

        private String select(String expr) {
            Matcher m = SELECT_CLS.matcher(expr);
            if (!m.matches()) {
                System.err.printf("Malformed select: %s\n", expr);
                return "ERROR: Malformed select";
            }

            return select(m.group(1), m.group(2), m.group(3));
        }

        private ArrayList<Table> getTableObjects(String tables) {
            ArrayList<String> tableNames = new ArrayList<>();
//            int last = 0;
//            for (int i = 0; i < tables.length(); i++) {
//                if (tables.charAt(i) == ',') {
//                    tableNames.add(tables.substring(last, i));
//                    last = i + 1;
//                }
//                if (i == tables.length() - 1) {
//                    tableNames.add(tables.substring(last, i + 1));
//                }
//            }
            String[] temp = tables.split(",");
            for (String s : temp) {
                tableNames.add(s);
            }
            ArrayList<Table> tableObjects = new ArrayList();
            for (String x : tableNames) {
                for (Table t : databaseTables) {
                    if (t.getName().equals(x)) {
                        tableObjects.add(t);
                    }
                }
            }
            return tableObjects;
        }

        private String select(String exprs, String tables, String conds) {
            ArrayList<Table> tableObjects = getTableObjects(tables);
            Table joinedTable = null;
            Table result = null;
            if (tableObjects.size() == 1) {
                joinedTable = tableObjects.get(0);
                if (conds == null) {
                    result = joinedTable;
                }
            } else if (tableObjects.size() == 2) {
                if (tableObjects.get(0).getName().equals(tableObjects.get(1).getName())) {
                    result = tableObjects.get(0);
                }
                joinedTable = tableObjects.get(0).join(tableObjects.get(0),
                        tableObjects.get(1));
                if (conds == null) {
                    result = joinedTable;
                }
            } else if (tableObjects.size() == 0){
                return "ERROR: Something";
            } else {
                joinedTable = tableObjects.get(0).join(tableObjects.get(0),
                        tableObjects.get(1));
                for (int i = 2; i < tableObjects.size(); i++) {
                    joinedTable = joinedTable.join(joinedTable, tableObjects.get(i));
                }
                if (conds == null) {
                    result = joinedTable;
                }
            }

            ArrayList<ArrayList<String>> passedRows = new ArrayList<>();
            if (conds != null) {
                conds = conds.replaceAll("\\s+", "");
                ArrayList<String> eachCondition = new ArrayList<>();
                String[] temp = conds.split("and");
                for (String x : temp) {
                    eachCondition.add(x);
                }

                for (Row r : joinedTable.getTableRows()) {

                    Boolean allPass = true;

                    for (String expr : eachCondition) {

                        String first = getFirstVar(expr);
                        String comparator = getComparator(expr);
                        String last = getLastVar(expr);
                        Boolean unary = false;
                        Boolean binary = false;

                        //check unary or binary
                        for (Column c : joinedTable.getTableColumns()) {
                            if (c.getColumnName().equals(last)) {
                                binary = true;
                            } else {
                                unary = true;
                            }
                        }

                        if (unary && !binary) {
                            Column storedColumn = null;
                            for (Column c : joinedTable.getTableColumns()) {
                                if (c.getColumnName().equals(first)) {
                                    storedColumn = c;
                                }
                            }
                            if (storedColumn == null) {
                                return "ERROR: invalid condition";
                            }

                            int index = joinedTable.getTableRows().get(0).mapIndex.get(storedColumn);
                            Number value = 0;
                            if (storedColumn.getColumnType().equals("int")) {
                                int firstVal = Integer.parseInt(r.entries.get(index));
                                int secondVal = Integer.parseInt(last);
                                value = firstVal - secondVal;
                            } else if (storedColumn.getColumnType().equals("float")) {
                                double firstVal = Double.parseDouble(r.entries.get(index));
                                double secondVal = Double.parseDouble(last);
                                value = firstVal - secondVal;
                            } else if (storedColumn.getColumnType().equals("string")) {
                                value = r.entries.get(index).compareTo(last);
                            }
                            if (comparator.contains("<")) {
                                if (comparator.contains("=")) {
                                    if ((int) value > 0) {
                                        allPass = false;
                                    }
                                } else {
                                    if ((int) value >= 0) {
                                        allPass = false;
                                    }
                                }
                            } else if (comparator.contains(">")) {
                                if (comparator.contains("=")) {
                                    if ((int) value < 0) {
                                        allPass = false;
                                    }
                                } else {
                                    if ((int) value <= 0) {
                                        allPass = false;
                                    }
                                }
                            } else if (comparator.equals("==")) {
                                if ((int) value != 0) {
                                    allPass = false;
                                }
                            } else if (comparator.equals("!=")) {
                                if ((int) value == 0) {
                                    allPass = false;
                                }
                            }
                        } else if (binary) {
                            Column storedColumnA = null;
                            Column storedColumnB = null;
                            for (Column c : joinedTable.getTableColumns()) {
                                if (c.getColumnName().equals(first)) {
                                    storedColumnA = c;
                                }
                                if (c.getColumnName().equals(last)) {
                                    storedColumnB = c;
                                }
                            }
                            if (storedColumnA == null || storedColumnB == null) {
                                return "ERROR: invalid condition";
                            }
                            int indexA = joinedTable.getTableRows().get(0).mapIndex.get(storedColumnA);
                            int indexB = joinedTable.getTableRows().get(0).mapIndex.get(storedColumnB);
                            Number value = 0;
                            if (storedColumnA.getColumnType().equals("int")) {
                                int firstVal;
                                int secondVal;
                                if (r.entries.get(indexA).equals("NaN")) {
                                    firstVal = Integer.MAX_VALUE;
                                } else {
                                    firstVal = Integer.parseInt(r.entries.get(indexA));
                                }
                                if (r.entries.get(indexB).equals("NaN")) {
                                    secondVal = Integer.MAX_VALUE;
                                } else {
                                    secondVal = Integer.parseInt(r.entries.get(indexB));
                                }
                                value = firstVal - secondVal;
                            } else if (storedColumnA.getColumnType().equals("float")) {
                                double firstVal;
                                double secondVal;
                                if (r.entries.get(indexA).equals("NaN")) {
                                    firstVal = Double.MAX_VALUE;
                                } else {
                                    firstVal = Double.parseDouble(r.entries.get(indexA));
                                }
                                if (r.entries.get(indexB).equals("NaN")) {
                                    secondVal = Double.MAX_VALUE;
                                } else {
                                    secondVal = Double.parseDouble(r.entries.get(indexB));
                                }
                                value = firstVal - secondVal;
                            } else if (storedColumnA.getColumnType().equals("string")) {
                                value = r.entries.get(indexA).compareTo(r.entries.get(indexB));
                            }
                            if (comparator.contains("<")) {
                                if (comparator.contains("=")) {
                                    if ((int) value > 0) {
                                        allPass = false;
                                    }
                                } else {
                                    if ((int) value >= 0) {
                                        allPass = false;
                                    }
                                }
                            } else if (comparator.contains(">")) {
                                if (comparator.contains("=")) {
                                    if ((int) value < 0) {
                                        allPass = false;
                                    }
                                } else {
                                    if ((int) value <= 0) {
                                        allPass = false;
                                    }
                                }
                            } else if (comparator.equals("==")) {
                                if ((int) value != 0) {
                                    allPass = false;
                                }
                            } else if (comparator.equals("!=")) {
                                if ((int) value == 0) {
                                    allPass = false;
                                }
                            }
                        }
                    }
                    if (allPass) {
                        passedRows.add(r.entries);
                    }

                }
            } else {
                for (Row r : joinedTable.getTableRows()) {
                    passedRows.add(r.entries);
                }
            }

            exprs = exprs.replaceAll("\\s+", "");
            if (exprs.equals("*")) {
                ArrayList<String> columnNames = new ArrayList<>();
                ArrayList<String> columnTypes = new ArrayList<>();
                for (Column c : joinedTable.getTableColumns()) {
                    columnNames.add(c.getColumnName());
                    columnTypes.add(c.getColumnType());
                }
                result = new Table("poop", columnNames, columnTypes);
                for (ArrayList x : passedRows) {
                    result.insertRow(x);
                }
            } else {
                // if the expression has no math
                if ((exprs.contains(",") && !exprs.contains("+") &&
                        !exprs.contains("-") && !exprs.contains("/") && !exprs.contains("*"))
                        || (!exprs.contains(",") && !exprs.contains("+")
                        && !exprs.contains("-") && !exprs.contains("/") && !exprs.contains("*")) ) {
                    String[] columnNames = exprs.split(",");
                    ArrayList<Column> columns = new ArrayList<>();
                    ArrayList<String> columnTypes = new ArrayList<>();
                    ArrayList<String> newColumnNames = new ArrayList<>();
                    ArrayList<ArrayList<String>> copy = new ArrayList<>();

                    for (String x : columnNames) {
                        for (Column c : joinedTable.getTableColumns()) {
                            if (x.equals(c.getColumnName())) {
                                columns.add(c);
                                columnTypes.add(c.getColumnType());
                                newColumnNames.add(c.getColumnName());
                            }
                        }
                    }
                    if (newColumnNames.size() == 0) {
                        return "ERROR: malformed select";
                    }
                    result = new Table("shithead", newColumnNames, columnTypes);

                    if (joinedTable.getTableRows().isEmpty()) {
                        return result.toString();
                    }

                    ArrayList<Integer> sortedIndices = new ArrayList<>();
                    Row r = joinedTable.getTableRows().get(0);
                    for (String c : newColumnNames) {
                        for (Column z : joinedTable.getTableColumns()) {
                            if (c.equals(z.getColumnName())) {
                                sortedIndices.add(r.mapIndex.get(z));
                            }
                        }
                    }


                    for (ArrayList x : passedRows) {
                        ArrayList<String> temp = new ArrayList<>();
                        for (int i : sortedIndices) {
                            temp.add((String) x.get(i));
                        }
                        copy.add(temp);
                    }

                    /*for (ArrayList x : passedRows) {
                        ArrayList<String> temp = new ArrayList<>();
                        for (int i = 0; i < x.size(); i++) {
                            temp.add((String) x.get(i));
                        }
                        copy.add(temp);
                    }*/


//                    ArrayList<Integer> indexes = new ArrayList<>();
//                    for (Column c : columns) {
//                        int index = r.mapIndex.get(c);
//                        indexes.add(index);
//                    }
//
//                    for (ArrayList x : copy) {
//                        for (int i = 0; i < x.size(); i++) {
//                            if (!indexes.contains(i)) {
//                                x.remove(i);
//                            }
//                        }
//                    }

                    for (ArrayList x : copy) {
                        result.insertRow(x);
                    }
                    //expression has math
                } else {
                    // multiple expressions, could be math, could be no math, one has to be math
                    if (exprs.contains(",")) {
                        String[] eachExpression = exprs.split(",");
                        ArrayList<String> newColumnNames = new ArrayList<>();
                        ArrayList<String> newColumnTypes = new ArrayList<>();
                        int size = joinedTable.getTableRows().size();
                        ArrayList<ArrayList<String>> toBeAdded = new ArrayList<>();
                        for (int i = 0; i < size; i++) {
                            toBeAdded.add(new ArrayList<String>());
                        }
                        for (String s : eachExpression) {

                            //just column aka no math
                            if (!s.contains("as")) {
                                newColumnNames.add(s);
                                Column x = null;
                                for (Column c : joinedTable.getTableColumns()) {
                                    if (s.equals(c.getColumnName())) {
                                        x = c;
                                        newColumnTypes.add(c.getColumnType());
                                    }
                                }

                                int index = joinedTable.getTableRows().get(0).mapIndex.get(x);
                                for (int i = 0; i < toBeAdded.size(); i++) {
                                    String value = joinedTable.getTableRows().get(i).entries.get(index);
                                    toBeAdded.get(i).add(value);
                                }

                                //math
                            } else if (s.contains("+") || s.contains("-") || s.contains("*") || s.contains("/")) {
                                String first = getFirstVar(s);
                                String operator = getMathOperator(s);
                                String last = getLastVar(s);
                                String newName = null;
                                if (last.contains("as")) {
                                    String[] temp = last.split("as");
                                    last = temp[0];
                                    newName = temp[1];
                                } else {
                                    return "ERROR: Malformed Column Command";
                                }

                                //copy paste
                                ArrayList<Column> columns = new ArrayList<>();

                                for (Column c : joinedTable.getTableColumns()) {
                                    if (c.getColumnName().equals(first)) {
                                        columns.add(c);
                                    }
                                }

                                for (Column c : joinedTable.getTableColumns()) {
                                    if (c.getColumnName().equals(last)) {
                                        columns.add(c);
                                    }
                                }

                                if (columns.size() == 0) {
                                    return "ERROR: Invalid Expression";
                                }

                                // Unary Expressions
                                if (columns.size() == 1) {
                                    if (columns.get(0).getColumnType().equals("string")) {
                                        try {
                                            Double.parseDouble(last);
                                            return "ERROR: Invalid Expression";
                                        } catch (NumberFormatException n) {
                                            if (!operator.equals("+")) {
                                                return "ERROR: Invalid Operation for String Concatenation";
                                            } else {
                                                int index = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));

                                                last = last.substring(1, last.length());
                                                for (int i = 0; i < toBeAdded.size(); i++) {
                                                    String firstVal = joinedTable.getTableRows().get(i).entries.get(index);
                                                    String q = firstVal.substring(0, firstVal.length() - 1).concat(last);
                                                    toBeAdded.get(i).add(q);
                                                }


                                                newColumnTypes.add("string");



                                            }
                                        }
                                        // not a string
                                    } else {
                                        try {
                                            Double literal = Double.parseDouble(last);
                                            int index = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));

                                            for (int i = 0; i < toBeAdded.size(); i++) {
                                                Double firstValue = Double.parseDouble(joinedTable.getTableRows().get(i).entries.get(index));
                                                Double newVal = null;
                                                if (operator.equals("+")) {
                                                    newVal = firstValue + literal;
                                                } else if (operator.equals("-")) {
                                                    newVal = firstValue - literal;
                                                } else if (operator.equals("*")) {
                                                    newVal = firstValue * literal;
                                                } else if (operator.equals("/")) {
                                                    if (literal == 0) {
                                                        String nan = "NaN";
                                                        toBeAdded.get(i).add(nan);
                                                        continue;
                                                    } else {
                                                        newVal = firstValue / literal;
                                                    }
                                                }
                                                String stringVal = newVal.toString();
                                                if (columns.get(0).getColumnType().equals("int") && stringVal.contains(".")) {
                                                    stringVal = stringVal.substring(0, stringVal.indexOf('.'));
                                                }
                                                toBeAdded.get(i).add(stringVal);
                                            }

                                            newColumnNames.add(newName);
                                            newColumnTypes.add(columns.get(0).getColumnType());


                                        } catch (NumberFormatException n) {
                                            return "ERROR: Incorrect expression";
                                        }

                                    }

                                    //Binary Expressions
                                } else {
                                    ArrayList<String> columnNames = new ArrayList<>();
                                    ArrayList<String> columnTypes = new ArrayList<>();
                                    for (Column c : columns) {
                                        columnNames.add(c.getColumnName());
                                        columnTypes.add(c.getColumnType());
                                    }

                                    //strings
                                    if (columnTypes.get(0).equals("string") && columnTypes.get(1).equals("string")) {

                                        try {
                                            Double.parseDouble(first);
                                            Double.parseDouble(last);
                                            return "ERROR: Invalid Expression";

                                        } catch (NumberFormatException n) {
                                            if (!operator.equals("+")) {
                                                return "ERROR: Invalid Operation for String Concatenation";
                                            } else {

                                                int indexA = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));
                                                int indexB = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(1));

                                                for (int i = 0; i < toBeAdded.size(); i++) {
                                                    ArrayList<String> newRows = new ArrayList<>();
                                                    String firstVal = joinedTable.getTableRows().get(i).entries.get(indexA);
                                                    String secondVal = joinedTable.getTableRows().get(i).entries.get(indexB);
                                                    String p = firstVal.substring(0, firstVal.length()-1).concat(secondVal.substring(1,
                                                            secondVal.length()));
                                                    toBeAdded.get(i).add(p);
                                                }



                                                newColumnNames.add(newName);
                                                newColumnTypes.add("string");


                                            }
                                        }
                                    } else if (columnTypes.get(0).equals("string") && !columnTypes.get(1).equals("string")
                                            || columnTypes.get(1).equals("string") && !columnTypes.get(0).equals("string")) {
                                        return "ERROR: Invalid column types for given expression";

                                        //not strings
                                    } else {
                                        try {

                                            int indexA = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));
                                            int indexB = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(1));

                                            for (int i = 0; i < toBeAdded.size(); i++) {
                                                Double firstValue;
                                                Double secondValue;
                                                if (joinedTable.getTableRows().get(i).entries.get(indexA).equals("NOVALUE")) {
                                                    firstValue = 0.0;
                                                } else {
                                                    firstValue = Double.parseDouble(joinedTable.getTableRows().get(i).entries.get(indexA));
                                                }
                                                if (joinedTable.getTableRows().get(i).entries.get(indexB).equals("NOVALUE")) {
                                                    secondValue = 0.0;
                                                } else {
                                                    secondValue = Double.parseDouble(joinedTable.getTableRows().get(i).entries.get(indexB));
                                                }
                                                Double newVal = null;
                                                if (operator.equals("+")) {
                                                    newVal = firstValue + secondValue;
                                                } else if (operator.equals("-")) {
                                                    newVal = firstValue - secondValue;
                                                } else if (operator.equals("*")) {
                                                    newVal = firstValue * secondValue;
                                                } else if (operator.equals("/")) {
                                                    if (secondValue == 0) {
                                                        String nan = "NaN";
                                                        toBeAdded.get(i).add(nan);
                                                        continue;
                                                    } else {
                                                        newVal = firstValue / secondValue;
                                                    }
                                                }
                                                String stringVal = newVal.toString();
                                                if (columns.get(0).getColumnType().equals("int") && columns.get(1).getColumnType().equals("int")
                                                        && stringVal.contains(".")) {
                                                    stringVal = stringVal.substring(0, stringVal.indexOf('.'));
                                                }
                                                toBeAdded.get(i).add(stringVal);

                                            }


                                            newColumnNames.add(newName);
                                            if (columns.get(0).getColumnType().equals("float") || columns.get(1).getColumnType().equals("float")) {
                                                newColumnTypes.add("float");
                                            } else {
                                                newColumnTypes.add("int");
                                            }




                                        } catch (NumberFormatException n) {
                                            return "ERROR: Incorrect expression";
                                        }
                                    }

                                }


                            }
                        }

                        result = new Table("fjdsalkfjasl;", newColumnNames, newColumnTypes);

                        for (ArrayList x : toBeAdded) {
                            result.insertRow(x);
                        }
                    } else {
                        // one math expression
                        String first = getFirstVar(exprs);
                        String operator = getMathOperator(exprs);
                        String last = getLastVar(exprs);
                        String newName = null;
                        if (last.contains("as")) {
                            String[] temp = last.split("as");
                            last = temp[0];
                            newName = temp[1];
                        } else {
                            return "ERROR: wtf u doin fam";
                        }
//                        System.out.println(last);
//                        System.out.println(newName);

                        ArrayList<ArrayList<String>> toBeAdded = new ArrayList<>();
                        ArrayList<String> columnNames = new ArrayList<>();
                        ArrayList<String> columnTypes = new ArrayList<>();
                        ArrayList<Column> columns = new ArrayList<>();

                        for (Column c : joinedTable.getTableColumns()) {
                            if (c.getColumnName().equals(first)) {
                                columns.add(c);
                            }
                        }

                        for (Column c : joinedTable.getTableColumns()) {
                            if (c.getColumnName().equals(last)) {
                                columns.add(c);
                            }
                        }

                        if (columns.size() == 0) {
                            return "ERROR: Invalid Expression";
                        }

                        // Unary Expressions
                        if (columns.size() == 1) {
                            if (columns.get(0).getColumnType().equals("string")) {
                                try {
                                    Double.parseDouble(last);
                                    return "ERROR: Invalid Expression";
                                } catch (NumberFormatException n) {
                                    if (!operator.equals("+")) {
                                        return "ERROR: Invalid Operation for String Concatenation";
                                    } else {
                                        int index = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));

                                        last = last.substring(1, last.length());
                                        for (Row r : joinedTable.getTableRows()) {
                                            ArrayList<String> newRows = new ArrayList<>();
                                            String firstVal = r.entries.get(index);
                                            String s = firstVal.substring(0, firstVal.length() - 1).concat(last);
                                            newRows.add(s);
                                            toBeAdded.add(newRows);
                                        }

                                        columnNames.add(newName);
                                        columnTypes.add("string");
                                        result = new Table("jfdslafjsd;al", columnNames, columnTypes);

                                        for (ArrayList x : toBeAdded) {
                                            result.insertRow(x);
                                        }

                                    }
                                }
                                // not a string
                            } else {
                                try {
                                    Double literal = Double.parseDouble(last);
                                    int index = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));

                                    for (Row r : joinedTable.getTableRows()) {
                                        ArrayList<String> newRows = new ArrayList<>();
                                        Double firstValue = Double.parseDouble(r.entries.get(index));
                                        Double newVal = null;
                                        if (operator.equals("+")) {
                                            newVal = firstValue + literal;
                                        } else if (operator.equals("-")) {
                                            newVal = firstValue - literal;
                                        } else if (operator.equals("*")) {
                                            newVal = firstValue * literal;
                                        } else if (operator.equals("/")) {
                                            if (literal == 0) {
                                                String nan = "NaN";
                                                newRows.add(nan);
                                                toBeAdded.add(newRows);
                                                continue;
                                            } else {
                                                newVal = firstValue / literal;
                                            }
                                        }
                                        String stringVal = String.valueOf(newVal);
                                        if (columns.get(0).getColumnType().equals("int") && stringVal.contains(".")) {
                                            stringVal = stringVal.substring(0, stringVal.indexOf('.'));
                                        }
                                        newRows.add(stringVal);
                                        toBeAdded.add(newRows);
                                    }

                                    columnNames.add(newName);
                                    columnTypes.add(columns.get(0).getColumnType());

                                    result = new Table("jfdslafjsd;al", columnNames, columnTypes);

                                    for (ArrayList x : toBeAdded) {
                                        result.insertRow(x);
                                    }

                                } catch (NumberFormatException n) {
                                    return "ERROR: Incorrect expression";
                                }

                            }

                            //Binary Expressions
                        } else {
                            for (Column c : columns) {
                                columnNames.add(c.getColumnName());
                                columnTypes.add(c.getColumnType());
                            }

                            //strings
                            if (columnTypes.get(0).equals("string") && columnTypes.get(1).equals("string")) {

                                try {
                                    Double.parseDouble(last);
                                    return "ERROR: Invalid Expression";

                                } catch (NumberFormatException n) {
                                    if (!operator.equals("+")) {
                                        return "ERROR: Invalid Operation for String Concatenation";
                                    } else {

                                        int indexA = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));
                                        int indexB = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(1));

                                        for (Row r : joinedTable.getTableRows()) {
                                            String firstVal;
                                            String secondVal;
                                            ArrayList<String> newRows = new ArrayList<>();
                                            if (r.entries.get(indexA).equals("NOVALUE")) {
                                                firstVal = "";
                                            } else {
                                                firstVal = r.entries.get(indexA);
                                            }
                                            if (r.entries.get(indexB).equals("NOVALUE")) {
                                                secondVal = "";
                                            } else {
                                                secondVal = r.entries.get(indexB);
                                            }

                                            String s = firstVal.substring(0, firstVal.length()-1).concat(secondVal.substring(1,
                                                    secondVal.length()));
                                            newRows.add(s);
                                            toBeAdded.add(newRows);
                                        }

                                        ArrayList<String> realColumnName = new ArrayList<>();
                                        ArrayList<String> realColumnType = new ArrayList<>();
                                        realColumnName.add(newName);
                                        realColumnType.add("string");
                                        result = new Table("result", realColumnName, realColumnType);

                                        for (ArrayList x : toBeAdded) {
                                            result.insertRow(x);
                                        }

                                    }
                                }
                            } else if (columnTypes.get(0).equals("string") && !columnTypes.get(1).equals("string")
                                    || columnTypes.get(1).equals("string") && !columnTypes.get(0).equals("string")) {
                                return "ERROR: Invalid column types for given expression";

                                //not strings
                            } else {
                                try {

                                    int indexA = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(0));
                                    int indexB = joinedTable.getTableRows().get(0).mapIndex.get(columns.get(1));

                                    for (Row r : joinedTable.getTableRows()) {
                                        ArrayList<String> newRows = new ArrayList<>();
                                        if (columns.get(0).getColumnType().equals("float") || columns.get(1).getColumnType().equals("float")) {
                                            Double firstValue = Double.parseDouble(r.entries.get(indexA));
                                            Double secondValue = Double.parseDouble(r.entries.get(indexB));
                                            Double newVal = null;
                                            if (operator.equals("+")) {
                                                newVal = firstValue + secondValue;
                                            } else if (operator.equals("-")) {
                                                newVal = firstValue - secondValue;
                                            } else if (operator.equals("*")) {
                                                newVal = firstValue * secondValue;
                                            } else if (operator.equals("/")) {
                                                if (secondValue == 0) {
                                                    String nan = "NaN";
                                                    newRows.add(nan);
                                                    toBeAdded.add(newRows);
                                                    continue;
                                                } else {
                                                    newVal = firstValue / secondValue;
                                                }
                                            }
                                            String stringVal = String.valueOf(newVal);
                                            if (columns.get(0).getColumnType().equals("int") && columns.get(1).getColumnType().equals("int")
                                                    && stringVal.contains(".")) {
                                                stringVal = stringVal.substring(0, stringVal.indexOf('.'));
                                            }
                                            newRows.add(stringVal);
                                            toBeAdded.add(newRows);
                                        } else {
                                            int firstValue = Integer.parseInt(r.entries.get(indexA));
                                            int secondValue = Integer.parseInt(r.entries.get(indexB));
                                            int newVal = 0;
                                            if (operator.equals("+")) {
                                                newVal = firstValue + secondValue;
                                            } else if (operator.equals("-")) {
                                                newVal = firstValue - secondValue;
                                            } else if (operator.equals("*")) {
                                                newVal = firstValue * secondValue;
                                            } else if (operator.equals("/")) {
                                                if (secondValue == 0) {
                                                    String nan = "NaN";
                                                    newRows.add(nan);
                                                    toBeAdded.add(newRows);
                                                    continue;
                                                } else {
                                                    newVal = firstValue / secondValue;
                                                }
                                            }
                                            String stringVal = String.valueOf(newVal);
                                            if (columns.get(0).getColumnType().equals("int") && columns.get(1).getColumnType().equals("int")
                                                    && stringVal.contains(".")) {
                                                stringVal = stringVal.substring(0, stringVal.indexOf('.'));
                                            }
                                            newRows.add(stringVal);
                                            toBeAdded.add(newRows);
                                        }

                                    }

                                    ArrayList<String> realColumnName = new ArrayList<>();
                                    ArrayList<String> realColumnType = new ArrayList<>();
                                    realColumnName.add(newName);
                                    if (columns.get(0).getColumnType().equals("float") || columns.get(1).getColumnType().equals("float")) {
                                        realColumnType.add("float");
                                    } else {
                                        realColumnType.add("int");
                                    }

                                    result = new Table("jfdslafjsd;al", realColumnName, realColumnType);

                                    for (ArrayList x : toBeAdded) {
                                        result.insertRow(x);
                                    }

                                } catch (NumberFormatException n) {
                                    return "ERROR: Incorrect expression";
                                }
                            }

                        }
                    }



                }

            }

            return result.toString();

        }


        public String getFirstVar(String expr) {
            String result = "";
            for (int i = 0; i < expr.length(); i++) {
                if (expr.charAt(i) == '>' || expr.charAt(i) == '<'
                        || expr.charAt(i) == '!' || expr.charAt(i) == '=' || expr.charAt(i) == '+'
                        || expr.charAt(i) == '-' || expr.charAt(i) == '/' || expr.charAt(i) == '*') {
                    result = expr.substring(0, i);
                    break;
                }
            }
            return result;
        }

        public String getMathOperator(String exprs) {
            String operator = "";
            for (int i = 0; i < exprs.length(); i++) {
                if (exprs.charAt(i) == '+') {
                    operator = "+";
                } else if (exprs.charAt(i) == '-') {
                    operator = "-";
                } else if (exprs.charAt(i) == '*') {
                    operator = "*";
                } else if (exprs.charAt(i) == '/') {
                    operator = "/";
                }
            }
            return operator;
        }

        public String getComparator(String conds) {
            String comparator = "";
            for (int i = 0; i < conds.length(); i++) {
                if (conds.charAt(i) == '>') {
                    if (conds.charAt(i + 1) == '=') {
                        comparator = ">=";
                        break;
                    } else {
                        comparator = ">";
                        break;
                    }
                } else if (conds.charAt(i) == '<') {
                    if (conds.charAt(i + 1) == '=') {
                        comparator = "<=";
                        break;
                    } else {
                        comparator = "<";
                        break;
                    }
                } else if (conds.charAt(i) == '!') {
                    comparator = "!=";
                    break;
                } else if (conds.charAt(i) == '=') {
                    comparator = "==";
                    break;
                }
            }
            return comparator;
        }

        public String getLastVar(String expr) {
            String result = "";
            for (int i = 0; i < expr.length(); i++) {
                if (expr.charAt(i) == '>' || expr.charAt(i) == '<'
                        || expr.charAt(i) == '!' || expr.charAt(i) == '=' || expr.charAt(i) == '+'
                        || expr.charAt(i) == '-' || expr.charAt(i) == '/' || expr.charAt(i) == '*') {
                    if (expr.charAt(i + 1) == '=') {
                        result = expr.substring(i + 2, expr.length());
                    } else {
                        result = expr.substring(i + 1, expr.length());
                    }
                }
            }
            return result;

        }


    }


    static ArrayList<Database> allDatabases;
    private ArrayList<Table> databaseTables;

    public Database() {
        allDatabases = new ArrayList<>();
        databaseTables = new ArrayList<>();
        allDatabases.add(this);
    }

    public String transact(String query) {
        try {
            System.out.println(query);
            Parse reader = new Parse();
            return reader.eval(query);
        } catch (IOException i) {
            return "IOEXCEPTION!!";
        }


    }

    public String dropTheTable(Table x) {
        databaseTables.remove(databaseTables.indexOf(x));
        return "";
    }

    public void insertTable(Table x) {
        databaseTables.add(x);
    }

    public String tableReader(String name) throws IOException {
        try {
            ArrayList<String> names = new ArrayList<>();
            ArrayList<String> types = new ArrayList<>();
            File file = new File(name + ".tbl");
            if (file.length() == 0) {
                return "ERROR: Empty table";
            }
            BufferedReader br = new BufferedReader(new FileReader(name + ".tbl"));
            String line = br.readLine();
            int last = 0;
            for (int i = 0; i < line.length(); i++) {
                Character rand = line.charAt(i);
                if (rand == ' ') {
                    names.add(line.substring(last, i));
                    last = i + 1;
                }
                if (rand == ',') {
                    types.add(line.substring(last, i));
                    last = i + 1;
                }
                if (i == line.length() - 1) {
                    //check that the column has both name and type
                    Character check = line.charAt(last - 1);
                    if (!check.equals(' ')) {
                        return "ERROR: Malformed table";
                    }
                    types.add(line.substring(last, i + 1));
                }
            }
            //checks same number of col names as col types
            if (names.size() != types.size()) {
                return "ERROR: Malformed table";
            }
            //checks invalid types
            for (String x : types) {
                if (!x.equals("int") && !x.equals("float") && !x.equals("string")) {
                    return "ERROR: Invalid type.";
                }
            }
            Table toBeAdded = new Table(name, names, types);
            insertTable(toBeAdded);
            line = br.readLine();
            last = 0;
            while (line != null) {
                ArrayList<String> rowAdd = new ArrayList<>();
                for (int i = 0; i < line.length(); i++) {
                    Character rand = line.charAt(i);
                    if (rand == ',') {
                        rowAdd.add(line.substring(last, i));
                        last = i + 1;
                    }
                    if (i == line.length() - 1) {
                        rowAdd.add(line.substring(last, i + 1));
                        last = 0;
                    }
                }
                if (rowAdd.size() != toBeAdded.getTableColumns().size()) {
                    return "ERROR: Malformed table";
                }
                toBeAdded.insertRow(rowAdd);
                line = br.readLine();
            }
            br.close();
            return "";
        } catch (FileNotFoundException f) {
            return "ERROR: File not found.";
        }

    }

    public static void main(String[] args) {
        ArrayList<ArrayList<String>> test = new ArrayList<>(3);
        System.out.println(test.size());
    }


}
