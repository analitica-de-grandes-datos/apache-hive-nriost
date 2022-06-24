/*
Pregunta
===========================================================================

Para el archivo `data.tsv` compute Calcule la cantidad de registros en que 
aparece cada letra minúscula en la columna 2.

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f output.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
u = LOAD 'data.tsv' USING PigStorage('\t') as (f1:CHARARRAY, f2:BAG{}, f3:map[]);
y = FOREACH u GENERATE $1;
words = FOREACH y GENERATE FLATTEN(f2) AS word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE $0, COUNT($1);
dump wordcount; 

store wordcount into 'output'; 
