mat1 = LOAD 'M-matrix-large.txt' USING PigStorage(',') AS (row,col,value);
mat2 = LOAD 'N-matrix-large.txt' USING PigStorage(',') AS (row,col,value);

Jon = JOIN mat1 BY col FULL OUTER, mat2 BY row;

Mul = FOREACH Jon GENERATE mat1::row AS r1, mat2::col AS c2, (mat1::value)*(mat2::value) AS value;

Grp = GROUP Mul BY (r1, c2);

Res = FOREACH Grp GENERATE group.$0 as row, group.$1 as col, SUM(Mul.value) AS val;

STORE Res INTO 'output';
