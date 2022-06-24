/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS DATA;
DROP TABLE IF EXISTS WORDCOUNT;
DROP TABLE IF EXISTS RESULT;

CREATE TABLE DATA (LINE STRING);
LOAD DATA LOCAL INPATH 'data.tsv' overwrite INTO TABLE DATA;
CREATE TABLE WORDCOUNT AS
SELECT WORD, COUNT(1) AS  COUNT FROM
(SELECT EXPLODE (split(LINE, '\\S')) AS WORD FROM DATA) W
GROUP BY WORD
ORDER BY WORD DESC
LIMIT 5;

CREATE TABLE RESULT AS
SELECT * FROM WORDCOUNT
ORDER BY WORD;

INSERT OVERWRITE DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM RESULT;

