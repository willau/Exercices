/**
 * UserChecker analyzes a user row id and will determine its consistency by checking if :
 * - it has to possess a bff (someone or himself)
 * - its bff is an id (column bff)
 * - its other friends are also ids (column others)
 * - all friends are unique (no redundancy)
 * - friends and bff ids have user as friend or bff (reciprocity of friendship)
 *
 * Created by willyau on 30/10/16.
 */

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;


public class UserChecker extends User{

    // Constructor
    public UserChecker(String name, Table table) {
        super(name, table);
    }


    // Is this friend also a row id ?
    private boolean isId(String friendName) throws IOException {
        Get friendGet = new Get(this.bytify(friendName));
        Result rowFriend = this.table.get(friendGet);
        if( rowFriend.isEmpty() ){
            return false;
        }else{
            return true;
        }
    }


    // Does user have a bff ?
    public boolean hasBff() throws IOException {
        if( "".equals(this.getBffName()) ){
            return false;
        }else{
            return true;
        }
    }


    // Is user's bff also a row id ?
    public boolean bffHasId() throws IOException {
        String bffName = this.getBffName();
        return isId(bffName);
    }


    // Are all of user's friends also row ids ?
    public boolean otherFriendsHaveIds() throws IOException {
        ArrayList<String> friendList = this.getFriendsName();
        for(String friend : friendList){
            if( !this.isId(friend) ){
                return false;
            }
        }
        return true;
    }



    // Does user's bff have him as a friend ?
    public boolean bffHasUserAsFriend() throws IOException {
        User userBff = new User(this.getBffName(), this.table);
        return userBff.hasFriend(this.name);
    }


    // Do all of user's friends have him as a friend ?
    public boolean friendsHaveUserAsFriend() throws IOException {
        for(String friend : this.getFriendsName()){
            User userFriend = new User(friend, this.table);
            if( ! userFriend.hasFriend(this.name) ){
                return false;
            }
        }
        return true;
    }


    // Do user have unique friends ? (no redundancy)
    public boolean uniqueFriends() throws IOException {
        ArrayList<String> friendList = this.getFriendsName();
        TreeSet<String> friendSet = new TreeSet<String>(friendList);

        // If set's length equals list's length, we have unique friends
        if( friendList.size() == friendSet.size() ){
            return true;
        }else{
            return false;
        }
    }

}
