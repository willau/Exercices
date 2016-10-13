# Name Statistics Computation using Hadoop

Three Map/Reduce classes that process the 'prenoms.csv' file for computing :
- Name count by origin
- Name count by origin count
- Name gender proportion in percentage

For creating the jar using maven, typing this command in the directory /TD2 should create a jar file in the directory /target :
- $ mvn package

### Name Count By Origin
In order to count the number of name for each origin, there are 3 useful classes that can be called by using the following command :
- $ hadoop jar 'jar file' NameCountByOrigin input/path output/path

### Name Count By Origin Count
In order to count the number of name for each origin count, type:
- $ hadoop jar 'jar file' NameCountByOriginCount input/path output/path

### Name Gender Proportion
In order to generate the percentage of male name and female name, type:
- $ hadoop jar 'jar file' NameGenderProp input/path output/path
