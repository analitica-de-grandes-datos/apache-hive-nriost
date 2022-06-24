/* 
Pregunta
===========================================================================

Para el archivo `data.tsv` compute la cantidad de registros por letra.
Almacene los resultados separados por comas. 

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
u = LOAD 'data.tsv' USING PigStorage('\t') as (f1:CHARARRAY, f2:CHARARRAY, f3:INT);
dump u;
y = FOREACH u GENERATE $0;
dump y;
 words = FOREACH y GENERATE FLATTEN(f1) AS word;
 grouped = GROUP words BY word;
 wordcount = FOREACH grouped GENERATE $0, COUNT($1);
dump wordcount;

store wordcount into 'output'; 
