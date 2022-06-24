/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Escriba el código en Pig para manipulación de fechas que genere la siguiente 
salida.

   1971-07-08,jul,07,7
   1974-05-23,may,05,5
   1973-04-22,abr,04,4
   1975-01-29,ene,01,1
   1974-07-03,jul,07,7
   1974-10-18,oct,10,10
   1970-10-05,oct,10,10
   1969-02-24,feb,02,2
   1974-10-17,oct,10,10
   1975-02-28,feb,02,2
   1969-12-07,dic,12,12
   1973-12-24,dic,12,12
   1970-08-27,ago,08,8
   1972-12-12,dic,12,12
   1970-07-01,jul,07,7
   1974-02-11,feb,02,2
   1973-04-01,abr,04,4
   1973-04-29,abr,04,4

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/

fs -rm -f -r output;

u = LOAD 'data.csv' USING PigStorage(',') 
    AS (id:int, 
        firstname:CHARARRAY, 
        surname:CHARARRAY, 
        birthday:CHARARRAY, 
        color:CHARARRAY, 
        quantity:INT);

y = FOREACH u GENERATE birthday;
w = FOREACH y GENERATE ToDate(birthday, 'yyyy-mm-dd');
z = FOREACH w GENERATE ToString($0, 'yyyy-mm-dd'),Case(ToString($0, 'mm')) WHEN '01' THEN 'ene' WHEN '02' THEN 'feb' WHEN '03' THEN 'mar' WHEN '04' THEN 'abr' WHEN '05' THEN 'may' WHEN '06' THEN 'jun' WHEN '07' THEN 'jul' WHEN '08' THEN 'ago' WHEN '09' THEN 'sep' WHEN '10' THEN 'oct' WHEN '11' THEN 'nov' WHEN '12' THEN 'dic' END,ToString($0, 'mm'),ToString($0, 'm');
dump z;
store z into 'output' USING PigStorage(',');

