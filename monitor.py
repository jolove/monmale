#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cassandraDBconnection # Librería propia del APP
import usefulLibraryFiles    # Libreria propia del APP
import usefulLibrary         # Libreria propia del APP

import logging, time
import sys, traceback
from datetime import datetime

from cassandra.cluster import Cluster

log = logging.getLogger()
log.setLevel('INFO')

# -- CONSTANTES
procID="MON000"
pathFile="/Users/jorgelozano/Monmalev1.0/monmale/monitor.cnf"
# Debemos sacarla de una variable del entorno previamente definida
# --
# -- CLASES
    
# --
# -- FUNCIONES
def loadBasicConfiguration(dic):
    # Cargaremos todas la variables que sabemos con antelación que necesitaremos
    dbHost=dic['dbHost']
    keyspace=dic['keyspace']
    return dbHost,keyspace

def usedVariablesCom(dic):
    # Crearemos un diccionario con todas las variables COMUNES (monmale.Configuracion) necesarias que están definidas en la BD
    dic['monitorWait']=''
    dic['connection1']=''
    dic['connection2']=''
    dic['tracesLevel']=''
    dic['logPath']=''

def main():
    logging.basicConfig()
    # ---- Sacamos las variables mínimas necesarias del fichero de configuración
    dicConfig = usefulLibraryFiles.readJSONfile(pathFile)
    dbHost,keyspace = loadBasicConfiguration(dicConfig)
    # ---- Creamos el objeto para trabajar con la BD
    client = cassandraDBconnection.connectionCassandra()
    client.connect([dbHost])
    # ---- Comprobamos la conexión contra la BD
    client.checkDBconnection(keyspace)
    # ---- Cargamos todas la variables COMUNES necesarias de la BD
    dicDBvariables={}
    usedVariablesCom(dicDBvariables)
    client.loadConfiguration(keyspace,dicDBvariables)
    # --- Cargamos el LOG
    hdlr = logging.FileHandler(str(dicDBvariables['logPath'])+'/'+str(procID)+'.log')
    log.addHandler(hdlr)
    if dicDBvariables['tracesLevel'] == 'DEBUG':
        log.setLevel(logging.DEBUG)
    elif dicDBvariables['tracesLevel'] == 'INFO':
        log.setLevel(logging.INFO)
    elif dicDBvariables['tracesLevel'] == 'WARNING':
        log.setLevel(logging.WARNING)
    elif dicDBvariables['tracesLevel'] == 'ERROR':
        log.setLevel(logging.ERROR)
    elif dicDBvariables['tracesLevel'] == 'CRITICAL':
        log.setLevel(logging.CRITICAL)
    # ---- Cargamos todas la variables del PROCESO necesarias de la BD
    dicDBvarProc={}
    usefulLibrary.usedVariablesProc(dicDBvarProc)
    client.loadConfigurationProc(keyspace,dicDBvarProc,procID) 
    # ---- Registramos en la BD el histórico para este proceso
    insertHist,valuesList,historyID=usefulLibrary.createRecordHistory(procID,keyspace)
    client.executeDBinsert(insertHist,valuesList)
    # ---- EXECUTION LOOP
    while True:
        # ---- Comprobamos si ha habido algún cambio en la configuración de la BD
        dicDBvariables2={}
        usedVariablesCom(dicDBvariables2)
        client.loadConfiguration(keyspace,dicDBvariables2)
        # --
        dicDBvarProc2={}
        usefulLibrary.usedVariablesProc(dicDBvarProc2)
        client.loadConfigurationProc(keyspace,dicDBvarProc2,procID) 
        changes1=usefulLibrary.checkVariables(dicDBvariables,dicDBvariables2)
        changes2=usefulLibrary.checkVariables(dicDBvarProc,dicDBvarProc2)
        if changes1:
            # Re-reading:
            dicDBvariables=dicDBvariables2
        if changes2:
            # Re-reading:
            dicDBvarProc=dicDBvarProc2
        if changes1 or changes2:        
            lastReading=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            valuesList[5]=lastReading
            client.executeDBinsert(insertHist,valuesList)
            if dicDBvarProc['stoppedflag']:
                log.info('Proccess stopped by DB configuration.')
                sys.exit(0)
                break
        listChil={}
        listChil=client.searchDBChildren(keyspace)
        usefulLibrary.searchOSChildren(listChil)
        # NOTA: El procedimiento anterior debería poder buscar el proceso en máquinas diferentes
        #       dejamos esta parte para mas adelante.
        # NOTA: también tendría que tener en cuenta la posibilidad de que un proceso tenga mas de 
        #       una instancia.
        usefulLibrary.startOSChildren(listChil,dbHost,keyspace)
        log.info("Applied variable monitorWait, sleeping:"+str(dicDBvariables['monitorWait'])+" s")
        time.sleep(float(dicDBvariables['monitorWait']))
                            
if __name__ == "__main__":
    main()