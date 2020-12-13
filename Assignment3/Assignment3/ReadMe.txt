System Requirements:
1) Apache Hadoop 2.7.7 
2) Java 1.8 
3) Apace Hadoop common and map reduce client common version 2.7.7

Name of the jar : equijoin.jar


The aprroach can be divided into two parts:

Mapper part: Which performs Mapping 
Reducer part : which perform reducing of mapper output

Part A MAPPER :

First, we are mapping key value pair based on column no the 2nd value in line.
The input given in pdf will look like the following:

Input :- 
R, 2, Don, Larson, Newark, 555-3221
S, 1, 33000, 10000, part1
S, 2, 18000, 2000, part1
S, 2, 20000, 1800, part1
R, 3, Sal, Maglite, Nutley, 555-6905
S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1
R, 4, Bob, Turley, Passaic, 555-8908

will be converted into 

key	value
1	S, 1, 33000, 10000, part1

2	R, 2, Don, Larson, Newark, 555-3221
	S, 2, 18000, 2000, part1
	S, 2, 20000, 1800, part1

3	R, 3, Sal, Maglite, Nutley, 555-6905
	S, 3, 24000, 5000, part1

4	S, 4, 22000, 7000, part1
	R, 4, Bob, Turley, Passaic, 555-8908


Part B REDUCER:

Then Reducer will take the above keys files as input and reduce it to no of reducers provided
Working of Reducer Class:-

1. Every Key in mapper its creating two list : one with table1 and other with tablei2
So for each entry of table1 we will combine each entry of table2 in reducing step


In answer we will combine :
S, 2, 20000, 1800, part1,R, 2, Don, Larson, Newark, 555-3221
S, 2, 18000, 2000, part1,R, 2, Don, Larson, Newark, 555-3221
S, 3, 24000, 5000, part1,R, 3, Sal, Maglite, Nutley, 555-6905
R, 4, Bob, Turley, Passaic, 555-8908,S, 4, 22000, 7000, part1
