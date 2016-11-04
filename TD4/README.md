# Basic Social Network built on HBase

**Source code is located in TD4/src/main/java**

The HBase table is called "wauHTable" on the cluster. The implementation seems complicated but only used one instance of Put to insert data into the HTable during each REPL session, instead of creating an instance Put for each answer to the questions.

In order to launch the code on the hadoop cluster, type the following in the command line :

`HADOOP_CLASSPATH=$(hbase classpath) hadoop jar SocialNetwork.jar ConsoleReader`

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
