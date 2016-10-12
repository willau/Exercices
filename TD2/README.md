# Name Statistics Computation using Hadoop

Three Map/Reduce classes that process the 'prenoms.csv' file for computing the demanded name statistics.

### Name Count By Origin
In order to count the number of name for each origin, type:
- $ hadoop jar <jar file> NameCountByOrigin input/path output/path

### Name Count By Origin Count
In order to count the number of name for each origin count, type:
- $ hadoop jar <jar file> NameCountByOriginCount input/path output/path

### Name Gender Proportion
In order to generate the percentage of male name and female name, type:
- $ hadoop jar <jar file> NameGenderProp input/path output/path
