package db;

/**
 * Created by nicol on 2/28/2017.
 */
public class Column {
    private String columnName;
    private String type;

    public Column(String name, String typez) {
        columnName = name;
        type = typez;
    }

    @Override
    public String toString() {
        String stringRep = "";
        stringRep += columnName + " " + type;
        return stringRep;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return type;

    }

}
