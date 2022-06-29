
##! /usr/local/bin/python3
r"""Test cases
>>> run_answer() # doctest: +NORMALIZE_WHITESPACE
11,74,59:21.7
11,74,59:22.5
14,25,59:21.4
18,16,59:21.7
20,41,59:22.5
22,87,59:21.7
22,87,59:22.3
23,68,59:22.4
27,105,59:21.7
32,42,59:22.5
<BLANKLINE>

"""
import doctest
import subprocess
import os

def run_answer():
    '''Codigo especifico para ejecutar la respuesta'''
    #----------------------------------------------------------------------------------------------
    # Ejecuta el código del estudiante
    #----------------------------------------------------------------------------------------------
    result = os.popen("pig -execute 'run question.pig'").read()
    result = os.popen("cat output/*").read()
    print(result)

#--------------------------------------------------------------------------------------------------
# Grader (generic)
#--------------------------------------------------------------------------------------------------
subprocess.run(['rm', '-f', '_SUCCESS']) # borra el flag de exito de la corrida
RESULT = doctest.testmod()               # ejecuta el doctest
FAIL, _ = RESULT                         # fail, total
if FAIL == 0:                            # grading
    open('_SUCCESS', 'a').close()        #
else:
    print('\n')
#--------------------------------------------------------------------------------------------------
