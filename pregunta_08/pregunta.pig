/*
Pregunta
===========================================================================

Para el archivo `data.tsv` compute la cantidad de registros por letra de la 
columna 2 y clave de al columna 3; esto es, por ejemplo, la cantidad de 
registros en tienen la letra `b` en la columna 2 y la clave `jjj` en la 
columna 3 es:

  ((b,jjj), 216)

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f outpu.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
u = LOAD 'data.tsv' USING PigStorage('\t') as (f1:CHARARRAY, f2:Bag{(letra:CHARARRAY)} , f3:Map[]);
y = FOREACH u GENERATE FLATTEN(f2), FLATTEN(f3);
w = FOREACH y GENERATE TOTUPLE($0,$1);
grouped = GROUP w BY $0;
wordcount = FOREACH grouped GENERATE $0, COUNT($1);
dump wordcount;
store wordcount into 'output' USING PigStorage('\t'); 

