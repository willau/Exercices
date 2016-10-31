# Basic Social Network built on HBase

**Source code is located in TD4/src/main/java**

### ConsoleReader 

It is the main program that lauches 2 REPL :
- One for filling HBase database by asking questions
- One for checking consistency of user in database


### User

A class that enable accessing information of an user.
It does not provide tools for updating user's information.


### UserHandler (inherit User)

A class that enable updating information of an user.


### UserChecker (inherit User)

A class that checks the consistency of an user.
