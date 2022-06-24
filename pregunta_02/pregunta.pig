/*
Pregunta
===========================================================================

Ordene el archivo `data.tsv`  por letra y valor (3ra columna). Escriba el
resultado separado por comas.

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f output.pig

     >>> Escriba el codigo del mapper a partir de este punto <<<
*/
u = LOAD 'data.tsv' USING PigStorage('\t') as (f1:CHARARRAY, f2:CHARARRAY, f3:INT);
y = ORDER u BY $0, $2;
dump y;

store y into 'output';
