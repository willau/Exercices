/**
 * User class
 * Implements basic function to access information of user.
 * No insertion of new value is done through this class
 *
 * Created by willyau on 31/10/16.
 */

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class User {

    // Attributes
    final protected byte[] familyInfo      = Bytes.toBytes("info");
    final protected byte[] familyFriends   = Bytes.toBytes("friends");
    final protected byte[] columnBff       = Bytes.toBytes("bff");
    final protected byte[] columnOthers    = Bytes.toBytes("others");
    final protected String separator       = " " ;
    protected String name;
    protected Put put;
    protected Get get;
    protected Append append;
    protected Table table;

    // Constructor
    protected User(String name, Table table) {
        this.name       = name;
        byte[] nameByte = bytify(name);
        this.put        = new Put(nameByte);
        this.get        = new Get(nameByte);
        this.append     = new Append(nameByte);
        this.table      = table;
    }

    // Check existence of main user
    protected boolean exists() throws IOException {
        return this.table.exists(get);
    }

    // Change String to byte[]
    protected byte[] bytify(String string){
        return Bytes.toBytes(string.toLowerCase());
    }


    // If value does not exist, return bytes of an empty string ''
    protected byte[] getByteValue(byte[] family, byte[] column) throws IOException {
        Result row = this.table.get(this.get);
        byte[] byteValue = bytify("");
        // If user exists in the database
        if (this.exists() && row.containsColumn(family, column)) {
                byteValue = row.getValue(family, column);
        }
        return byteValue;
    }


    // Get the value from a row
    protected String getRowValue(Result row, byte[] family, byte[] column){
        String value = "";
        if( !row.isEmpty() && row.containsColumn(family, column) ){
            value = Bytes.toString(row.getValue(family, column));
        }
        return value;
    }


    // Get the name of user's bff
    protected String getBffName() throws IOException {
        Result row = this.table.get(this.get);
        return getRowValue(row, familyFriends, columnBff);
    }


    // Get other's name
    protected ArrayList<String> getFriendsName() throws IOException {
        Result row = this.table.get(this.get);
        String friendNames = this.getRowValue(row, familyFriends, columnOthers);
        String[] friendArray = friendNames.split(separator);
        ArrayList<String> friendList = new ArrayList<String>();
        for( String friend : friendArray ){
            if( !friend.equals("") ) friendList.add(friend) ;
        }
        return friendList;
    }

    
    // Verify user possess a specific friend
    protected boolean hasFriend(String someoneName) throws IOException {
        ArrayList<String> friendsName = getFriendsName();
        if( friendsName.contains(someoneName) ){
            return true;
        }else{
            return false;
        }
    }
}
