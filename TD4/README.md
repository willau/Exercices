
# TD4 : Social Network built on HBase

### SocialNetworkBFF

SocialNetworkBFF table needs to exist in the HBase database before launching the software.

It also needs to have 2 families "friends" and "info".


### ConsoleReader

ConsoleReader is a class that handles all interactions with the user.

It will create a connection with the HBase Database.
It will use a class UserHandler for each user to handle :
- The creation of new user 
- The update of existing user according to answers to its questions.

The social network enforces a few rules :
- For simplicity, each person has an unique name (non redundant)
- Bff value is mandatory (best friend for life)
- Every one has its own account (friends will be automatically created if they don't exist)
- An user's bff will have user as friend or as bff (reciprocity).
- A user can be its own bff.
- You are a bff or a friend not both (exclusivity).
 
The console reader is case insensitive and only accepts as answers :
- Name without accents, special characters or numbers
- Age between 0 and 99
- Exact answers to its multiple choice question


### UserHandler 

UserHandler is a class that encapsulates everything necessary for :
- Creating a new user
- Updating an existing user
It will automatically decides between a 'Put' or an 'Append' and an instance of UserHandler 
will just need to add information and call insertIntoDatabase().
