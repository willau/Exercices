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

    final static byte[] familyInfo      = Bytes.toBytes("info");
    final static byte[] familyFriends   = Bytes.toBytes("friends");
    final static byte[] columnBff       = Bytes.toBytes("bff");
    final static byte[] columnOthers    = Bytes.toBytes("others");
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
        this.listFriend = new TreeSet<String>();
        this.name       = name;
        byte[] nameByte = bytify(name);
        this.put        = new Put(nameByte);
        this.get        = new Get(nameByte);
        this.append     = new Append(nameByte);
        this.appendOk   = false;
        this.putOk      = false;
        this.table      = table;
    }


    // Check existence of main user
    private boolean exists() throws IOException{
        return this.table.exists(get);
    }


    // Change String to byte[]
    private byte[] bytify(String string){
        return Bytes.toBytes(string.toLowerCase());
    }


    // If value does not exist, return bytes of an empty string ''
    private byte[] getValue(final byte[] family, final byte[] column) throws IOException {
        byte[] value = bytify("");
        // If user exists in the database
        if (this.exists()) {
            Result row = table.get(get);
            if (row.containsColumn(family, column)) {
                value = row.getValue(family, column);
            }
        }
        return value;
    }


    // Update value only if there is a change
    private void updateValue(final byte[] family, final byte[] column, final byte[] value) throws IOException {
        String oldValue = Bytes.toString(getValue(family, column));
        String newValue = Bytes.toString(value);
        boolean equality = oldValue.equalsIgnoreCase(newValue);
        // If new value is different from existing value, replace it
        if( !equality ){
            put.addColumn(family, column, value);
            putOk = true; // Indicate that a 'put' action is to be done
        }
    }


    // Append value to existing one if it is a new value
    private void appendValue(final byte[] family, final byte[] column, final byte[] value) throws IOException {
        String newValue = Bytes.toString(value);

        // Get old values and split it into a list of values and an array of values
        String oldValue = Bytes.toString(getValue(family, column));
        String separator = "";
        String[] arrayVal = oldValue.split(separator);
        ArrayList<String> listVal = new ArrayList<String>(Arrays.asList(arrayVal));

        // If column did not exist (empty string), create a new column with the value
        if( arrayVal[0] == "" ){
            put.addColumn(family, column, value);
            putOk = true; // Indicate that a 'put' action is to be done

        // Otherwise, append it to existing values with separator
        }else{

            // Verify that list do not contain new value
            if ( !listVal.contains(newValue) ){
                append.add(family, column, bytify(separator.concat(newValue)));
                appendOk = true; // Indicate that an 'append' action is to be done
            }
        }
    }


    // Add information with a given name of column
    public UserHandler addInfo(String column, String info) throws IOException {
        if( info.length() > 0 ) updateValue(familyInfo, bytify(column), bytify(info));
        return this;
    }


    // Add bff
    public UserHandler addBff(String nameOfBff) throws IOException {
        if( nameOfBff.length() > 0 ){
            updateValue(familyFriends, columnBff, bytify(nameOfBff));
            this.listFriend.add(nameOfBff); // Add it to user's list of friends
        }
        return this;
    }


    // Add friend
    public UserHandler addFriend(String friend) throws IOException {
        // If friend is not empty and is not bff and is not user, append it to the list of existing friends
        if( friend.length() > 0 && !listFriend.contains(friend) && !friend.equals(this.name) ){
            appendValue(familyFriends, columnOthers, bytify(friend));
            this.listFriend.add(friend); // Add it to user's list of friends
        }
        return this;
    }


    // Insert the changes in the database
    private void updateUser() throws IOException {
        if( putOk ) this.table.put(this.put);
        if( appendOk ) this.table.append(this.append);
    }


    // Check existence of user's friends and :
    // - Either update information of existing friend by appending user to its list of friends.
    // - Or create new friend user with bff value set to user.
    private void updateFriend(String friendName) throws IOException {

        // If friend's name is not empty
        if( friendName.length() > 0) {
            UserHandler friend = new UserHandler(friendName, this.table);

            // If friend exists, we append user's name to the column 'others'
            if (friend.exists()) {
                friend.addFriend(this.name);

            // Else we create friend and we insert user's name in the column 'bff'
            } else {
                friend.addBff(this.name);
            }
            // Insert the change into the database
            friend.updateUser();
        }
    }


    // Insert user then update friends' information in the database
    public void updateIntoDatabase() throws IOException {
        this.updateUser();
        for(String friend: listFriend) this.updateFriend(friend) ;
    }

}

