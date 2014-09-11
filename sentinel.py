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
        # Añadimos a la variable de configuración que fija el directorio de entrasada el nombre del proceso
        # para dar lugar a la ruta completa: ej -> /Users/jorgelozano/Monmalev1.0/initial + /SNT000
        pathI=dicDBvariables["initialPath"]+"/"+procID
        # La carpeta final para este proceso es la carpeta de inicial del siguiente
        pathF=dicDBvariables["initialPath"]+"/CLN000"
        pathB=dicDBvariables["backupPath"]+"/"+procID
        pathE=dicDBvariables["errorPath"]+"/"+procID
        newFile=""
        exist=False
        newFile,exist=usefulLibraryFiles.searchFilesIN(pathI,"*.txt",procID)
        if exist:
            dicHeader={}
            error=False
            dicHeader,error=usefulLibraryFiles.readHeaderFile(pathI,newFile)
            # Si se dio algún error al leer la cabecera descartaremos el fichero
            if error:
                # Si hay error movemos el fichero
                er=usefulLibraryFiles.moveFile(pathI,pathE,newFile)
                if er:
                    # Si al intentar mover el fichero tenemos error salimos de la ejecución.
                    sys.exit(1)
                # Si no tenemos error al mover el fichero continuamos la ejecución buscando nuevo ficheros.                    
            else:
                # Continuamos tratando el fichero
                exists=False
                exists=client.searchClient(keyspace,dicHeader["clientID"])
                # Buscamos el ID del cliente de la cabecera en la BD
                if exists:
                    # Si el cleinte ID existe sacamos el resto de elementos de fichero de entrada para
                    # validar que estos pertenecen realmente al cliente especificado.
                    # No se aplicará ningún algoritmo de MA, se aplicarán tres comprobaciones para "clasificar" el fichero de entrada
                    # mas una extra: que el cliente ID de la cabecera corresponda con el "calculado"
                    dicClientsClassification={}
                    client.extractClientsFeatures(keyspace,dicClientsClassification)
                    e=False
                    decoded={}
                    e,decoded=usefulLibraryFiles.readNomFieldFile(pathI,newFile)
                    if e:
                        # si al leer los campos del fichero hemos tenido algún error
                        sys.exit(1)
                    clientID=""
                    classify,clientID=usefulLibrary.applyClassification(dicClientsClassification,decoded)
                    if classify==False:
                        # Si hay error movemos el fichero
                        pathE=dicDBvariables["errorPath"]+"/"+procID
                        er=usefulLibraryFiles.moveFile(pathI,pathE,newFile)
                        if er:
                            # Si al intentar mover el fichero tenemos error salimos de la ejecución.
                            sys.exit(1)
                        # Si no tenemos error al mover el fichero continuamos la ejecución buscando nuevo ficheros. 
                    else:
                        log.info("ClientID of file -->"+str(dicHeader["clientID"]))
                        log.info("ClientID of clas -->"+str(clientID))
                        if dicHeader["clientID"] == clientID:
                            #significa que el clientID del fichero de entrada corresponde con el mismo calculado por la clasificación
                            # Si la clasificación fue bien continuamos
                            # en el diseño original llegados a este punto existía una función que dado el cliente buscaba el último dataSet
                            # almacenado para incrementar en 1 su ID y usarlo para el nuevo, esto se susstituyo por un UUID aleatorio con el
                            # fin de agilizar la operación.
                            # 1.Creara un nuevo DataSetID
                            # 2.Copiará el fichero de entrada
                            # 3.Insertará el nuevo DataSetID en el fichero copiado.
                            # 4. Moverá los ficheros:
                            #    - Original --> a la ruta de backup
                            #    - Copiado  --> a la ruta de salida
                            newDataSetID=usefulLibrary.createUUID()
                            extension=usefulLibrary.extensionDate()
                            # NEW: 09.09.14
                            newFinalPath = client.searchMLFinalpathForClient(keyspace,clientID,procID)
                            log.info(procID+" <main> search new final path for clientTrack -->"+str(newFinalPath))
                            # -----
                            E,fileWithExt=usefulLibraryFiles.createCopyFile(pathI,newFile,extension)
                            if E:
                                # Ha habido algún error al crear el nuevo fichero
                                sys.exit(1)
                            else:
                                if usefulLibraryFiles.addNewDataID(pathI,fileWithExt,newDataSetID):
                                    # Ha habido algún error al editar el fichero
                                    sys.exit(1)
                                else:
                                    if usefulLibraryFiles.moveFile(pathI,pathB,newFile)==True:
                                        # ha habido un error
                                        sys.exit(1)
                                    if usefulLibraryFiles.moveFile(pathI,newFinalPath,fileWithExt)==True:
                                        # ha habido un error
                                        sys.exit(1)
                        else:
                            #significa que el clientID que viene en el fichero no es el correcto
                            pathE=dicDBvariables["errorPath"]+"/"+procID
                            er=usefulLibraryFiles.moveFile(pathI,pathE,newFile)
                            if er:
                                # Si al intentar mover el fichero tenemos error salimos de la ejecución.
                                sys.exit(1)
                            # Si no tenemos error al mover el fichero continuamos la ejecución buscando nuevo ficheros.   
                else:
                    pathE=dicDBvariables["errorPath"]+"/"+procID
                    er=usefulLibraryFiles.moveFile(pathI,pathE,newFile)
                    if er:
                        # Si al intentar mover el fichero tenemos error salimos de la ejecución.
                        sys.exit(1)
                    # Si no tenemos error al mover el fichero continuamos la ejecución buscando nuevo ficheros.   
            #sys.exit(0) -> lo marco porque no se que hace aquí
        else:
            log.info("Applied variable filesWait, sleeping:"+str(dicDBvariables['filesWait'])+" s")
            time.sleep(float(dicDBvariables['filesWait']))
                            
if __name__ == "__main__":
    main()