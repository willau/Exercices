/**
 * WARNING :
 * "SocialNetworkBFF" needs to exist before launching the main.
 * It also needs to have 2 families "friends" and "info".
 *
 * ConsoleReader is a class that handles all interactions with the user.
 * It will create a connection with the HBase Database and will use
 * a class UserHandler for each user to :
 * - handle the creation of new users
 * - update existing user
 * Everything is based on answers to the questions.
 *
 * The social network enforces a few rules :
 * - For simplicity, each person has an unique name (no redundancy)
 * - Bff value is mandatory (best friend for life)
 * - Every one has its own account (friends will be automatically created if they don't exist)
 * - An user's bff will have user as friend or as bff (reciprocity).
 * - A user can be its own bff.
 * - A bff is also a friend
 *
 * The console reader is case insensitive and only accepts as answers :
 * - Name without accents, special characters or numbers
 * - Age between 0 and 99
 * - Exact answers for multiple choice question
 *
 * BONUS :
 * ConsoleReader will also ask user if he wants to check consistency of a name
 * It will use UserChecker to check a user.
 *
 * Created by willyau on 26/10/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Scanner;

import static java.util.regex.Pattern.matches;


public class ConsoleReader {
    Scanner scan ;

    public ConsoleReader(){
        this.scan = new Scanner(System.in);
    }


    // Check format of answer with a given answer and a regex formula
    private boolean checkFormat(String answer, String responseType, String regexFormula){
        if( matches(regexFormula, answer.toLowerCase()) ){
            return true ;
        }else{
            System.out.println("Invalid format, please answer with a valid '" + responseType + "'");
            return false;
        }
    }


    // Confirmation of answer with only 'y' or 'n'
    private boolean askConfirmation(String answer, String responseType){

        // If it's an empty string '', 'q' or 's', do not ask for confirmation !
        if( answer.equals("") || answer.equals("q") || answer.equals("s")) {
            return true;
        }

        // Ask for confirmation of answer
        System.out.println("Confirm that your " + responseType + " is '" + answer + "' (y/n)");
        String yesNoAnswer = "";
        boolean valid = false;

        // Until console receives 'y' or 'n' as answer, repeat question
        while ( !valid ){
            yesNoAnswer = this.scan.nextLine();
            String ynRegex = "^y$|^n$";
            valid = checkFormat(yesNoAnswer, "y or n", ynRegex); // Only accept 'y' or 'n'
        }

        if( yesNoAnswer.equals("y") ){
            return true;
        }
        else{
            return false;
        }
    }


    // Ask question to user with an expected response type and a regex formula to check format
    // Return an empty string "" if user skips question
    public String askQuestion(String question, String responseType, String regexFormula){
        String answer = "";
        boolean confirmation = false;

        // Until user provides valid answer AND confirms its answer with 'y' or 'n', repeat question
        while( !confirmation ){
            System.out.println(question);
            answer = this.scan.nextLine();

            // If format respects regex formula, ask for confirmation
            if( checkFormat(answer, responseType, regexFormula) ){
                confirmation = askConfirmation(answer, responseType);
            }
        }
        return answer;
    }


    public static void main(String[] args) throws IOException {

        // Establishing connection to HBase
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        Connection connection = ConnectionFactory.createConnection(conf);

        try {
            // Access HBase table "SocialNetworkBFF" (it has to exist)
            Table table = connection.getTable(TableName.valueOf("wauHTable"));
            System.out.println("\nConnection to HBase established\n\n\n");
            String name = "" ;

            try {
                // Instance of ConsoleReader for asking input from user
                ConsoleReader consoleReader = new ConsoleReader();

                // Creating regex formula for checking answer format
                final String qRegex                 = "^$|^q$";                         // matches 'q' or ''
                final String obligatoryNameRegex    = "^[a-z]+$";                       // matches name with only alphabet standard character (no accents)
                final String nameRegex              = "^$|".concat(obligatoryNameRegex);// matches name or ''
                final String ageRegex               = "^$|^[1-9]$|^[1-9][1-9]$";        // matches 0 to 99 or ''
                final String technologyChoiceRegex  = "^$|^flink$|^apex$|^spark$";      // matches 'apex','flink' or 'spark' or ''
                final String sRegex                 = "^$|^s$";                         // matches 's' or ''


                // MAIN FUNCTION : Starting the REPL for filling the HBase database
                boolean startSession = "".equals(consoleReader.askQuestion("Enter SocialNetworkBFF ? ('q' to quit / enter to continue)", "choice", qRegex));
                while( startSession ) {

                    name = consoleReader.askQuestion("What is your name ?", "name", obligatoryNameRegex);
                    UserHandler user = new UserHandler(name, table);

                    // Asking information about the user
                    String bff      = consoleReader.askQuestion("Who is your best friend for life, a.k.a BFF ? (obligatory)", "bff name", obligatoryNameRegex);
                    String friend   = consoleReader.askQuestion("Who is your other friend ? (enter to skip)", "friend", nameRegex);
                    String age      = consoleReader.askQuestion("How old are you ? (enter to skip)", "age", ageRegex);
                    String technology = consoleReader.askQuestion("Do you like Flink, Apex or Spark ? (enter to skip)", "technology", technologyChoiceRegex);

                    // Adding main user's information (UserHandler instance user takes care of creating requests for updating database)
                    user = user.addBff(bff).addFriend(friend).addInfo("age", age).addInfo("technology", technology);

                    // Insert updates into HBase database
                    user.updateIntoDatabase();

                    startSession = "s".equals(consoleReader.askQuestion("Quit SocialNetworkBFF ? (enter to quit / 's' to stay )", "choice", sRegex));
                }



                // BONUS : Starting the REPL for checking consistency of a user
                boolean startCheck = "".equals(consoleReader.askQuestion("Do you want to check consistency of an user ? ('q' to quit / enter to continue)", "choice", qRegex));
                while( startCheck ) {

                    String nameToCheck = consoleReader.askQuestion("Whose consistency do you want to check ? ", "name to check", obligatoryNameRegex);
                    UserChecker userChecker = new UserChecker(nameToCheck, table);
                    String warning = "Inconsistency found : ";

                    if( userChecker.exists() ){
                        boolean consistent = true ;
                        if( ! userChecker.hasBff() ){
                            System.out.println(warning + nameToCheck + " do not have bff.");
                            consistent = false ;
                        }
                        if( ! userChecker.bffHasId() ){
                            System.out.println(warning + "bff is not a row id.");
                            consistent = false ;
                        }
                        if( ! userChecker.bffHasUserAsFriend() ){
                            System.out.println(warning + "bff does not have " + nameToCheck + " as friend.");
                            consistent = false ;
                        }
                        if( ! userChecker.otherFriendsHaveIds() ){
                            System.out.println(warning + "one friend is not a row id.");
                            consistent = false ;
                        }
                        if( ! userChecker.friendsHaveUserAsFriend() ){
                            System.out.println(warning + "one friend do not have " + nameToCheck + " as friend.");
                            consistent = false ;
                        }
                        if( ! userChecker.uniqueFriends() ){
                            System.out.println(warning + "list of all friends is not unique");
                            consistent = false ;
                        }
                        if( consistent ){
                            System.out.println(nameToCheck + " is consistent with his friends.");
                        }

                    }else{
                        System.out.println("The name you gave does not exist within SocialNetworkBFF, please give another name.");
                    }

                    startCheck = "s".equals(consoleReader.askQuestion("Quit consistency checking ? (enter to quit / 's' to stay )", "choice", sRegex));
                }


            // Close table
            }finally {
                if( table != null ) table.close();
            }

        // Close connection
        }finally{
            connection.close();
        }



    }
}
