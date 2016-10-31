/**
 * UserHandler : extension of User class
 * Use for adding and updating new information into user's row.
 * UserHandler is a class that encapsulates everything necessary for :
 * - Creating a new user
 * - Updating an existing user
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


public class UserHandler extends User {

    // Attributes
    private Set<String> listNewFriends;
    private String appendString;
    private boolean putOk;

    // Constructor
    public UserHandler(String name, Table table) {
        super(name, table);
        this.appendString = "";
        this.putOk      = false;
        this.listNewFriends = new TreeSet<String>();
    }


    // Insert value only if there is a change
    private void insertValue(byte[] family, byte[] column, byte[] value) throws IOException {
        String oldValue = Bytes.toString(this.getByteValue(family, column));
        String newValue = Bytes.toString(value);
        boolean equality = oldValue.equalsIgnoreCase(newValue);
        // If new value is different from existing value, replace it
        if( ! equality ){
            put.addColumn(family, column, value);
            // Indicate that a 'put' action is to be done
            putOk = true;
        }
    }


    // Append value to existing one if it is a new value
    private void updateOtherFriends(byte[] value) throws IOException {
        String newValue = Bytes.toString(value);

        // Get old values and split it into a list of values and an array of values
        String oldValue = Bytes.toString(this.getByteValue(familyFriends, columnOthers));
        String[] arrayVal = oldValue.split(separator);
        ArrayList<String> listVal = new ArrayList<String>(Arrays.asList(arrayVal));

        // Verify that list do not contain new value
        if ( ! listVal.contains(newValue) ){
            appendString = appendString.concat(separator.concat(newValue));
            insertValue(familyFriends, columnOthers, bytify(oldValue.concat(appendString).trim()));
        }
    }


    // Add information with a given name of column
    public UserHandler addInfo(String column, String info) throws IOException {
        if( info.length() > 0 ) insertValue(familyInfo, bytify(column), bytify(info));
        return this;
    }


    // Add bff
    public UserHandler addBff(String nameOfBff) throws IOException {
        byte[] byteBff = bytify(nameOfBff);
        if( nameOfBff.length() > 0 ){
            insertValue(familyFriends, columnBff, byteBff);
            if( ! nameOfBff.equals(this.name) ){
                this.listNewFriends.add(nameOfBff);
                updateOtherFriends(byteBff);
            }
        }
        return this;
    }


    // Add friend
    public UserHandler addFriend(String friend) throws IOException {
        // If friend is not user and is not already a friend, append it to the list of existing friends
        if( friend.length() > 0  && ! friend.equals(this.name) && ! listNewFriends.contains(friend) ){
            updateOtherFriends(bytify(friend));
            this.listNewFriends.add(friend);
        }
        return this;
    }


    // Insert the changes in the database
    private void updateUserIntoDatabase() throws IOException {
        if( putOk ) this.table.put(this.put);
        appendString = "" ;
        putOk = false ;
    }


    // Check existence of user's friends and :
    // - Either update information of existing friend by appending user to its list of friends.
    // - Or create new friend user with bff value set to user.
    private void updateFriendIntoDatabase(String friendName) throws IOException {

        if( friendName.length() > 0) {
            UserHandler friend = new UserHandler(friendName, this.table);

            // If friend exists, we add user's name to its list of other friends
            if( friend.exists() ){
                friend.addFriend(this.name);

            // Otherwise, its bff is user
            }else{
                friend.addBff(this.name);
            }

            friend.updateUserIntoDatabase();
        }
    }


    // Insert user then update friends' information in the database
    public void updateIntoDatabase() throws IOException {
        this.updateUserIntoDatabase();
        for(String friend: listNewFriends) this.updateFriendIntoDatabase(friend) ;
    }



}

