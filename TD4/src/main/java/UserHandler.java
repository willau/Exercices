/**
 * UserHandler is a class that encapsulates everything necessary for :
 * - Creating a new user
 * - Updating an existing user
 * It will automatically decides between a 'Put' or an 'Append'.
 * An instance of UserHandler will just need to add information and call insertIntoDatabase().
 *
 * Created by willyau on 26/10/16.
 */

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;


public class UserHandler {

    // Static variables for families and columns
    final static byte[] familyInfo      = Bytes.toBytes("info");
    final static byte[] familyFriends   = Bytes.toBytes("friends");
    final static byte[] colBff          = Bytes.toBytes("bff");
    final static byte[] colOthers       = Bytes.toBytes("others");

    // Variable
    private Set<String> listFriend;
    private String name;
    private Put put;
    private Get get;
    private Append append;
    private boolean appendOk;
    private boolean putOk;
    private Table table;

    // Constructor : we instantiate UserHandler with user's name and table of HBase
    public UserHandler(String name, Table table){
        this.listFriend = new TreeSet<String>(); // Set of unique friends (bff and others)
        this.name       = name; // Name of user
        
        // Put, Get and Append of user
        byte[] nameByte = bytify(name);
        this.put        = new Put(nameByte);
        this.get        = new Get(nameByte);
        this.append     = new Append(nameByte);

        this.appendOk   = false; // Boolean indicating if put action is to be done.
        this.putOk      = false; // Boolean indicating if append action is to be done
        this.table      = table; // Table handling interaction with HBase database
    }


    // Check existence of main user
    private boolean exists() throws IOException{
        return this.table.exists(get);
    }


    // Change String to byte[]
    private byte[] bytify(String string){
        return Bytes.toBytes(string.toLowerCase());
    }


    // Get value from existing row with given family and column
    // If it does not exist, return bytes representing an empty string ""
    private byte[] getValue(byte[] family, byte[] column) throws IOException {
        byte[] value = bytify("");
        // If user's row exists
        if (this.exists()) {
            Result r = table.get(get);
            // If user's row contains given column for given family
            if (r.containsColumn(family, column)) {
                value = r.getValue(family, column);
            }
        }
        return value;
    }


    // Update value only if there is a change
    private void updateValue(byte[] family, byte[] column, byte[] value) throws IOException {
        String oldVal = Bytes.toString(getValue(family, column));
        String newVal = Bytes.toString(value);
        boolean equality = oldVal.equalsIgnoreCase(newVal);
        // If new value is different from existing value, replace it
        if( !equality ){
            put.addColumn(family, column, value);
            putOk = true; // Indicate that a 'put' action is to be done
        }
    }


    // Append value to existing one if it is a new value
    private void appendValue(byte[] family, byte[] column, byte[] value) throws IOException {
        String newVal = Bytes.toString(value);

        // Get old values and split it into a list and an array of values
        String oldVal = Bytes.toString(getValue(family, column));
        String[] arrayVal = oldVal.split(" ");
        ArrayList<String> listVal = new ArrayList<String>(Arrays.asList(arrayVal));

        // If row did not exist (empty string), update value
        if( arrayVal[0] == "" ){
            put.addColumn(family, column, value);
            putOk = true; // Indicate that a 'put' action is to be done

        // Else append it with separator space
        }else{
            if ( !listVal.contains(newVal) ){
                append.add(family, column, bytify(" ".concat(newVal)));
                appendOk = true; // Indicate that an 'append' action is to be done
            }
        }
    }


    // Add information with a given name of column
    public UserHandler addInfo(String column, String info) throws IOException {
        if( info.length() > 0 ) updateValue(familyInfo, bytify(column), bytify(info));
        return this;
    }


    // Add bff and check if not empty string
    public UserHandler addBff(String nameOfBff) throws IOException {
        if( nameOfBff.length() > 0 ){
            updateValue(familyFriends, colBff, bytify(nameOfBff));
            this.listFriend.add(nameOfBff); // Add it to user's list of friends
        }
        return this;
    }


    // Add friend
    public UserHandler addFriend(String friend) throws IOException {
        // If friend is not empty and is not bff and is not user, append it (we don't want repeated information)
        if( friend.length() > 0 && !listFriend.contains(friend) && !friend.equals(this.name) ){
            appendValue(familyFriends, colOthers, bytify(friend));
            this.listFriend.add(friend); // Add it to user's list of friends
        }
        return this;
    }


    // Insert update if we updated some values
    private void updateUser() throws IOException {
        if( putOk ) this.table.put(this.put);
        if( appendOk ) this.table.append(this.append);
    }


    // Check existence of user's friends and :
    // - Either update information of existing friend by appending user to its list of friends
    // - Or create new friend user with bff value set to user
    private void updateFriends(String friendName) throws IOException {

        // If friend's name is not empty and user exists in database
        if( friendName.length() > 0 || this.exists() ) {
            UserHandler friend = new UserHandler(friendName, this.table);

            // If friend exists, we append user's name to the row
            if (friend.exists()) {
                friend.addFriend(this.name);

            // Else we create friend with user as its bff !
            } else {
                friend.addBff(this.name);
            }
            friend.updateUser();
        }
    }


    // Insert user then update friends' information in the database
    public void updateIntoDatabase() throws IOException {
        this.updateUser();
        for(String friend: listFriend) this.updateFriends(friend) ;
    }

}

