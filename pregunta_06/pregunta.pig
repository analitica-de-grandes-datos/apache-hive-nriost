/*
Pregunta
===========================================================================

Para el archivo `data.tsv` Calcule la cantidad de registros por clave de la 
columna 3. En otras palabras, cuántos registros hay que tengan la clave 
`aaa`?

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f output.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
u = LOAD 'data.tsv' USING PigStorage('\t') as (f1:CHARARRAY, f2:BAG{}, f3:map[]);
y = FOREACH u GENERATE $2;
words = FOREACH y GENERATE FLATTEN(f3) AS word; 
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE $0, COUNT($1);
dump wordcount; 
store wordcount into 'output'USING PigStorage(','); 
