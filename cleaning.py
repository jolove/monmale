#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cassandraDBconnection # Librería propia del APP
import usefulLibraryFiles    # Libreria propia del APP
import usefulLibrary         # Libreria propia del APP
import machineLearningLibrary# Libreria propia del APP

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
    dic['recentAlarm']=''
    dic['alarmWait']=''
    dic['tracesLevel']=''
    dic['logPath']=''
    # Las varianbles relacionadas con las rutas son comunes a todos los procesos ya que posteriormente se le unirá el prombre del
    # proceso.
    dic['initialPath']=''
    dic['finalPath']=''
    dic['backupPath']=''
    dic['errorPath']=''
    dic['maxTrainDataSet']=''
    dic['minTrainDataSet']=''
    dic['coefVaration']=''    
    dic['test_size']=''    


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
                log.info(procID+'- Proccess stopped by DB configuration.')
                sys.exit(0)
                break
        # Añadimos a la variable de configuración que fija el directorio de entrasada el nombre del proceso
        # para dar lugar a la ruta completa: ej -> /Users/jorgelozano/Monmalev1.0/initial + /SNT000
        pathI=dicDBvariables["initialPath"]+"/"+procID
        # La carpeta final para este proceso es la carpeta de inicial del siguiente
        pathF=dicDBvariables["initialPath"]+"/ACT000"
        pathB=dicDBvariables["backupPath"]+"/"+procID
        pathE=dicDBvariables["errorPath"]+"/"+procID
        newFile=""
        exist=False
        newFile,exist=usefulLibraryFiles.searchFilesIN(pathI,"*.txt",procID)
        if exist:
            dateInitial=datetime.now()
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
                # Hemos quitado la comprobación de buscar al cliente en la BD, ya que esta comprobación se hace en la fase anterior
                # y hacerlo nuevamente es repetir.
                lisAlarm=[]
                client.searchRecentAlarm(keyspace,dicHeader['clientID'],dicDBvariables['recentAlarm'],lisAlarm,procID)
                if len(lisAlarm)==0:
                    # No hay alarma reciente
                    log.info(procID+" - There is not recent alarm.")
                #
                # Después de insertar el nuevo DataSet en la BD tendremos que generar el fichero JSON en el cual será introducida
                # la lista del punto anterior con la posible alarma previa o en caso contrario la lista nula
                #
                dicClientsClassif={}
                client.extractClientsFeatures(keyspace,dicClientsClassif)
                #
                # La función anterior nos devuelve un diccionario con todos los clientes y sus características
                # Llamaremos a la siguiente función con las caracterísricas especificas de nuestro cliente
                #
                L1,L2,dicL,dicWrongValues = usefulLibrary.cleanNewDataSet(dicClientsClassif[dicHeader['clientID']],pathI,newFile,procID)
                #
                # Una vez tenemos el nuevo dataSet limpio de strings debemos cargar desde la BD los 'maxTrainDataSet' dataSet anteriores para 
                # usarlos como set de entrenamiento, por otro lado usaremos el nuevo dataSet como set de testeo quitando en cada momento la 
                # muestra que queremos "validar" (predecir)
                #
                L_train=[]         # En esta variable cargaremos los valores de los dataSet de entrenamiento
                features_train=[]  # En esta variable cargaremos los nombres de las columnas del array de entrenamiento.
                order=[]
                for index in dicL['L2'].keys():
                    order.append(int(index))
                for o in sorted(order):
                    features_train.append("col"+str(o))   # en la BD las columnas en la tabla de dataSet no tienen nombre, son "col1",....
                log.info(procID+" - Features Train: "+str(features_train))
                client.loadTrainDataSet(keyspace,dicDBvariables['maxTrainDataSet'],dicDBvariables['minTrainDataSet'],L_train,features_train,dicHeader['clientID'],"",False,False,procID)
                # En este punto ya tenemos:
                #   - L2         --> array que usaremos como test y a predecir
                #   - L_train    --> array que usaremos como train
                # por lo que ya podemos aplicar los algoritmos de ML.
                #
                dicAlarmsRegr={}
                score=0
                log.info(procID+" <main> L_train is --> "+str((len(L_train)*100)/(len(L_train)+len(L2)))+"%")
                if len(L_train) == 0: #or (len(L_train)*100)/(len(L_train)+len(L2)) < int(dicDBvariables['test_size']): # no hay suficientes dataset anteriores en la BD
                    log.info(procID+" - There are not enough datasets in DB, the regression execution will not be executed.")
                else:
                    machineLearningLibrary.applyRegression(dicWrongValues,L2,dicL["L2"],L_train,dicDBvariables['coefVaration'],dicAlarmsRegr,score,dicDBvariables['test_size'],procID)
                    # IMPORTANTE: el diccionario dicAlarmsRegr nos marca los registros con alarma sobre el array L2, por lo que antes de insertarlas
                    # en la BD debemos de encontrar las coordenadas correctas.
                    # De la llamada anterior obtenemos:
                    #      - L2 --> con los valores wrong rectificados
                    #      - dicAlarmsRegr --> diccionatio con las coordenadas de las muestras con alarma.
                    # Para insertar en la BD además de estos elementos necesitamos:
                    #      - L1
                    #      - dicL
                    #      - dicHeader
                L_union=[]
                dicAlarmReal={}
                dicAlarmReal=usefulLibrary.realCoord(dicAlarmsRegr,dicL,procID)
                L_union = usefulLibrary.unionArrays(L1,L2,dicL,procID)
                log.info(procID+" - Num columns L_union: "+str(len(L_union[0])))
                log.info(procID+" - Num rows L_union: "+str(len(L_union)))
                # NOTA: habría que comprobar el número de columna que hay, puede ser necesario crear alguna mas
                dateFinal=datetime.now()
                timeProcessing=dateFinal-dateInitial
                log.info(procID+" - Time processing: "+str(timeProcessing.seconds)+" s")
                # CHANGE: 04.09.14 --> client.insertNewDataSet(keyspace,dicHeader,L_union,dicL["L0"],dicAlarmsRegr,procID) 
                client.insertNewDataSetCheck(keyspace,dicHeader,L_union,dicL["L0"],dicAlarmsRegr,procID) 
                client.insertResult(keyspace,dicHeader,timeProcessing.seconds,score,procID)
                if len(dicAlarmsRegr.keys()) >0:
                    # En este proceso sólo se puede dar un tipo de alarma que es la de valor incorrecto mediante REGRESIÓN
                    #  Tipo Alarma 1
                    # la variable "alarmed" hace referendia a las posibles alarmas que puede tener un mismo registro, como en el punto en el que estamos sabemos que la alarma es nueva
                    # sólo puede terner como máximo la alarma detectada en este punto. cuando el proceso "miner" trabaje puede encontrar alguna alarma mas
                    alarmed=[]
                    alarmed.append(1)
                    client.insertAlarmS(keyspace,dicHeader,dicAlarmReal,dicL["L0"],alarmed,procID)
                # CHANGE: 03.09.14 --> usefulLibraryFiles.createJSONDataset(dicAlarmReal,dicHeader,dicL["L0"],L_union,pathF,lisAlarm,procID)
                usefulLibraryFiles.createJSONDatasetWithoutAlarm(dicAlarmReal,dicHeader,dicL["L0"],L_union,pathF,lisAlarm,procID)
                usefulLibraryFiles.moveFile(pathI,pathB,newFile)
        else:
            log.info(procID+" - Applied variable filesWait, sleeping:"+str(dicDBvariables['filesWait'])+" s")
            time.sleep(float(dicDBvariables['filesWait']))
                            
if __name__ == "__main__":
    main()