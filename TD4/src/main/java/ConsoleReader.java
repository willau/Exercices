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
 * - For simplicity, each person has an unique name (non redundant)
 * - Bff value is mandatory (best friend for life)
 * - Every one has its own account (friends will be automatically created if they don't exist)
 * - An user's bff will have user as friend or as bff (reciprocity).
 * - A user can be its own bff.
 * - You are a bff or a friend not both (exclusivity).
 *
 * The console reader is case insensitive and only accepts as answers :
 * - Name without accents, special characters or numbers
 * - Age between 0 and 99
 * - Exact answers for multiple choice question
 *
 * Created by willyau on 26/10/16.
 */

import org.apache.hadoop.conf.Configuration;
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

    // Constructor
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
        Connection connection = ConnectionFactory.createConnection(conf);

        try {
            // Access HBase table "SocialNetworkBFF" (it has to exist)
            Table table = connection.getTable(TableName.valueOf("SocialNetworkBFF"));
            System.out.println("\nConnection to HBase established\n\n\n");

            try {
                // Instance of ConsoleReader for asking input from user
                ConsoleReader consoleReader = new ConsoleReader();

                // Creating regex formula for checking answer format
                final String qRegex       = "^$|^q$";                     // matches 'q' or ''
                final String bffRegex     = "^[a-z]+$";                   // matches name with only alphabet standard character (no accents)
                final String nameRegex    = "^$|".concat(bffRegex);       // matches name or ''
                final String ageRegex     = "^$|^[1-9]$|^[1-9][1-9]$";    // matches 0 to 99 or ''
                final String choiceRegex  = "^$|^flink$|^apex$|^spark$";  // matches 'apex','flink' or 'spark' or ''
                final String sRegex       = "^$|^s$";                     // matches 's' or ''

                boolean startSession = "".equals(consoleReader.askQuestion("Enter SocialNetworkBFF ? ('q' to quit / enter to continue)", "choice", qRegex));
                while( startSession ) {

                    String name = consoleReader.askQuestion("What is your name ?", "name", nameRegex);
                    UserHandler user = new UserHandler(name, table);

                    // Asking information about the user
                    String bff      = consoleReader.askQuestion("Who is your best friend for life, a.k.a BFF ? (obligatory)", "bff name", bffRegex);
                    String friend   = consoleReader.askQuestion("Who is your other friend ? (enter to skip)", "friend", nameRegex);
                    String age      = consoleReader.askQuestion("How old are you ? (enter to skip)", "age", ageRegex);
                    String technology = consoleReader.askQuestion("Do you like Flink, Apex or Spark ? (enter to skip)", "technology", choiceRegex);

                    // Adding main user's information (UserHandler instance user takes care of creating requests for updating database)
                    user = user.addBff(bff).addFriend(friend).addInfo("age", age).addInfo("technology", technology);

                    // Insert updates into HBase database
                    user.updateIntoDatabase();

                    startSession = "s".equals(consoleReader.askQuestion("Quit SocialNetworkBFF ? (enter to quit / 's' to stay )", "choice", sRegex));
                }

            }finally {
                // Close table
                if( table != null ) table.close();
            }

        }finally{
            // Close connection
            connection.close();
        }

    }
}
