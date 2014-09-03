#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cassandraDBconnection # Librería propia del APP
import usefulLibraryFiles    # Libreria propia del APP
import usefulLibrary         # Libreria propia del APP

import logging, time, subprocess
import sys, traceback
from datetime import datetime

from cassandra.cluster import Cluster

log = logging.getLogger()
log.setLevel('INFO')

# -- CONSTANTES
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
    dic['filesWait']=''
    dic['connection1']=''
    dic['connection2']=''
    dic['tracesLevel']=''
    dic['logPath']=''
    # Las varianbles relacionadas con las rutas son comunes a todos los procesos ya que posteriormente se le unirá el prombre del
    # proceso.
    dic['initialPath']=''
    dic['finalPath']=''
    dic['backupPath']=''
    dic['errorPath']=''
    # Otras
    dic['logstashAgentCnf']=''
    dic['maxLinesAgentFile']=''
    dic['minLinesAgentFile']=''
    dic['commandAgentStart']=''
    dic['algorithmDis']=''
    dic['redisHost']=''
    dic['redisKey']=''

def main():
    logging.basicConfig()
    dicConfig={}
    # ---- Sacamos las variables mínimas necesarias del fichero de los parámetros pasados al arrancar el proceso
    dicConfig,procID = usefulLibrary.loadArguments()
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
    # ----
    dicAgentProcess={}    # es un diccionario de objetos subprocess
    dicClientsClassification={}
    client.extractClientsFeatures(keyspace,dicClientsClassification)
    usefulLibrary.applyAlgorithmDistribution(dicDBvariables,dicAgentProcess,dicClientsClassification.keys(),procID)
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
                if len(dicAgentProcess.keys()) > 0: # significa que hemos arrancando agentes de logstash
                    usefulLibrary.killAgents(dicAgentProcess,procID)
                sys.exit(0)
                break
        # Añadimos a la variable de configuración que fija el directorio de entrasada el nombre del proceso
        # para dar lugar a la ruta completa: ej -> /Users/jorgelozano/Monmalev1.0/initial + /SNT000
        # La carpeta final para este proceso es la carpeta de inicial del siguiente
        pathF=dicDBvariables['finalPath']
        pathB=dicDBvariables["backupPath"]+"/"+procID
        pathE=dicDBvariables["errorPath"]+"/"+procID
        for CL in dicClientsClassification.keys():
            pathI=dicDBvariables["initialPath"]+"/"+procID+"/"+str(CL)+"/"
            newFile=""
            exist=False
            newFile,exist=usefulLibraryFiles.searchFilesIN(pathI,"*.json",procID)
            if exist:
                error=False
                lines=0
                if "_Alarms.json" in newFile: # significa que es un JSON de Alarmas creado por el proceso MINER
                    destFile=str(CL)+"_Alarms.log"
                elif "_Dataset.json" in newFile:
                    destFile=str(CL)+"_DataSet.log"
                error,lines=usefulLibraryFiles.checkAgentOUTfile(pathF,destFile,procID)
                # Si se dio algún error al leer la cabecera descartaremos el fichero
                if error:
                    # Si hay error movemos el fichero
                    er=usefulLibraryFiles.moveFile(pathI,pathE,newFile)
                    if er:
                        # Si al intentar mover el fichero tenemos error salimos de la ejecución.
                        sys.exit(1)
                    # Si no tenemos error al mover el fichero continuamos la ejecución buscando nuevo ficheros.                    
                else:
                    log.info(procID+ " <main> Agent OUT file has: "+str(lines)+" lines")
                    # Continuamos tratando el fichero
                    if lines >= dicDBvariables["maxLinesAgentFile"]:
                        log.info(procID+ " <main> Agent OUT file big.")
                        e=False
                        e = usefulLibraryFiles.maintenanceLogFile(pathF,destFile,lines-int(dicDBvariables["maxLinesAgentFile"]),procID)
                        if e:
                            E=usefulLibraryFiles.moveFile(pathI,pathE,newFile)
                            if E:
                                sys.exit(1)
                    e=False
                    log.info(procID+ " <main> Adding new Dataset....")
                    e = usefulLibraryFiles.addNewJSONInFile(pathI,newFile,pathF,destFile,procID)
                    log.info(procID+ " <main> Moving in file to backup.")
                    usefulLibraryFiles.moveFile(pathI,pathB,newFile)
            else:
                log.info("Applied variable filesWait, sleeping:"+str(dicDBvariables['filesWait'])+" s")
                time.sleep(float(dicDBvariables['filesWait']))
                            
if __name__ == "__main__":
    main()