A = LOAD  'large-graph.txt' USING PigStorage() AS(id:int,friends:int);
B = GROUP A BY id;
C = FOREACH B GENERATE FLATTEN(COUNT(A.friends)) AS cnt;
D = GROUP C BY cnt;
E = FOREACH D GENERATE COUNT(C.cnt),group;
F = ORDER E BY group DESC;
STORE F INTO 'output' USING PigStorage (',');
DUMP F;
