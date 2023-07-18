A = LOAD '$G' USING PigStorage(',') AS (a: int, n: double);
N = FILTER A BY (a IS NOT NULL);
U = GROUP N BY a;

Use_Rate = FOREACH U GENERATE group, AVG(N.n) as R;
G = GROUP Use_Rate BY FLOOR(R*10)/10;
S = FOREACH G GENERATE group, COUNT(Use_Rate);

STORE S INTO '$O' USING PigStorage (' ');