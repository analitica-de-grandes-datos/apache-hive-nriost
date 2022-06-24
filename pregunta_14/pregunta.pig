/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Escriba el código equivalente a la siguiente consulta SQL.
   SELECT 
       color 
   FROM 
       u 
   WHERE 
       color NOT LIKE 'b%';
Escriba el resultado a la carpeta `output` del directorio actual. Para la  
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

*/
u = LOAD 'data.csv' USING PigStorage(',') 
    AS (id:int, 
        firstname:CHARARRAY, 
        surname:CHARARRAY, 
        birthday:CHARARRAY, 
        color:CHARARRAY, 
        quantity:INT);

y = FOREACH u GENERATE color;
z = FILTER y BY (SUBSTRING($0,0,1) MATCHES '[c-z]'); 
dump z; 
store z into 'output';
