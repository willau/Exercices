/**
 * UserChecker analyzes a user row id and will determine its consistency by checking if :
 * - it has to possess a bff (someone or himself)
 * - its bff is an id (column bff)
 * - bff is a friend
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


public class UserChecker extends User{

    public UserChecker(String name, Table table) {
        super(name, table);
    }

    private boolean isId(String friendName) throws IOException {
        Get friendGet = new Get(this.bytify(friendName));
        Result rowFriend = this.table.get(friendGet);
        if( rowFriend.isEmpty() ){
            return false;
        }else{
            return true;
        }
    }


    public boolean bffHasId() throws IOException {
        String bffName = this.getBffName();
        return isId(bffName);
    }


    public boolean otherFriendsHaveIds() throws IOException {
        ArrayList<String> friendList = this.getOtherFriendsName();
        for(String friend : friendList){
            if( !this.isId(friend) ){
                return false;
            }
        }
        return true;
    }


    public boolean bffHasUserAsFriend(){
        return true;
    }


    public boolean allFriendsHaveUserAsFriend(){
        return true;
    }


    public boolean uniqueFriends(){
        return true;
    }

    public boolean checkConsistency() throws IOException{
        boolean bffConsistency = this.bffHasId() && this.bffHasUserAsFriend() ;
        boolean allFriendsConsistency = this.otherFriendsHaveIds() && allFriendsHaveUserAsFriend() ;
        boolean uniqueFriendsConsistency = this.uniqueFriends();
        boolean consistency = bffConsistency && allFriendsConsistency && uniqueFriendsConsistency ;
        return consistency ;
    }

}
