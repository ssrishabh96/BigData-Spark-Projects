
## Matrix Multiplication on Map-Reduce


### Description

The purpose of this project is to develop a simple Map-Reduce program on Hadoop for graph processing.

In this project, you are asked to implement a simple graph algorithm that needs two Map-Reduce jobs. A directed graph is represented as a text file where each line represents a graph edge. For example,

20,40

represents the directed edge from node 20 to node 40. First, for each graph node, you compute the number of node neighbors. Then, you group the nodes by their number of neighbors and for each group you count how many nodes belong to this group. That is, the result will have lines such as:

10 30

which says that there are 30 nodes that have 10 neighbors.

### Compile & Running

Compile Multiply.java using:

```java
run graph.build
```

and you can run Graph.java in standalone mode over a small dataset using:

```java
sbatch graph.local.run
```

Results generated by the program will be in the directory output. These results should be:

```java

2 2
3 2
4 1
5 2
7 1
```


The results generated by your program will be in the directory output. These results should be:

```java
sbatch graph.distr.run
```

This will process the graph on the large dataset large-graph.txt and will write the result in the directory output-distr. These results should be similar to the results in the file large-solution.txt. 

Results generated by the program will be in the directory output-distr. 

##### Pseudo-Code

```java

// The first Map-Reduce is:

map ( key, line ):
  read 2 long integers from the line into the variables key2 and value2
  emit (key2,value2)

reduce ( key, nodes ):
  count = 0
  for n in nodes
      count++
  emit(key,count)

// The second Map-Reduce is:

map ( node, count ):
  emit(count,1)

reduce ( key, values ):
  sum = 0
  for v in values
      sum += v
  emit(key,sum)
```

Java main program, args[0] is the graph file and args[1] is the output directory. The input file format for reading the input graph and the output format for the final result must be text formats, while the format for the intermediate results between the Map-Reduce jobs must be binary formats.
