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
    dic['searchWait']=''
    dic['tracesLevel']=''
    dic['logPath']=''
    # Las varianbles relacionadas con las rutas son comunes a todos los procesos ya que posteriormente se le unirá el prombre del
    # proceso.
    dic['finalPath']=''
    dic['initialPath']=''
    dic['maxTrainDataSet']=''
    dic['minTrainDataSet']=''
    dic['clustGroup']=''
    dic['proofGroup']=''
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
        # La carpeta final para este proceso es la carpeta de inicial del siguiente
        pathF=dicDBvariables["initialPath"]+"/ACT000"
        # Continuamos tratando el fichero
        dicPendingData={} # será un diccionario del tipo:
        # datasetID: 2323-sdsdsd-2323
        # clientID: 3
        client.searchDatasetNoAnalysed(keyspace,dicPendingData,procID)
        if len(dicPendingData.keys())!= 0:
            log.info(procID+" <main> There are recent datasets pending for analyze.")
            for datasetID in dicPendingData.keys():
                clientID=dicPendingData[datasetID]
                dicClientsClassifOrder={}
                client.extractClientsFeaturesOrder(keyspace,dicClientsClassifOrder)
                dicLspecial={} # a diferencia del dicL usado en el CLN este sólo será un diccionario del mismo tipo donde las keys
                # que representaban el orden no tendrán el orden correcto
                usefulLibrary.createDicL(dicClientsClassifOrder,clientID,dicLspecial,procID)
                #
                # La función anterior nos devuelve un diccionario con todos los clientes y sus características
                # Llamaremos a la siguiente función con las caracterísricas especificas de nuestro cliente
                #
                order,features=[],[]
                for index in dicLspecial['L2'].keys():
                    order.append(int(index))
                for o in sorted(order):
                    features.append("col"+str(o))   # en la BD las columnas en la tabla de dataSet no tienen nombre, son "col1",....
                L_predict=[]
                L_train=[]         # En esta variable cargaremos los valores de los dataSet de entrenamiento
                features_predict=features
                features_train=features
                #backup 11.09.14 --> client.loadTrainDataSet(keyspace,dicDBvariables['maxTrainDataSet'],0,L_predict,features_predict,clientID,datasetID,True,True,procID)                
                client.loadTrainDataSetv2(keyspace,dicDBvariables['maxTrainDataSet'],0,L_predict,features_predict,clientID,datasetID,True,True,procID)                
                log.info(procID+" <main> Features Train: "+str(features_train))
                #backup 11.09.14 --> client.loadTrainDataSet(keyspace,dicDBvariables['maxTrainDataSet'],dicDBvariables['minTrainDataSet'],L_train,features_train,clientID,datasetID,False,True,procID)
                client.loadTrainDataSetv2(keyspace,dicDBvariables['maxTrainDataSet'],dicDBvariables['minTrainDataSet'],L_train,features_train,clientID,datasetID,False,True,procID)
                # En este punto ya tenemos:
                #   - L_predict  --> array que usaremos como a predecir
                #   - L_train    --> array que usaremos como train y test
                # por lo que ya podemos aplicar los algoritmos de ML.
                #
                # Tenemos que realizar la validación contra la BD de conocimiento externo para las alarmas de la Regresión Lineal
                # ---- NEW : 05.09.14
                lisInformationDB = []
                lisInformationDB = client.extractDatasetInformation(keyspace,clientID,datasetID,dicLspecial,procID) 
                listAlarmsDB = []
                listAlarmsDB = client.searchAlarmsByDataSet(keyspace,clientID,datasetID,procID) 
                dicAlarmsLinealRegresion={}
                usefulLibrary.convertListToDicAlarm(lisInformationDB,listAlarmsDB,dicAlarmsLinealRegresion,dicLspecial,procID)  
                dicExternalKnow={}
                if len(dicAlarmsLinealRegresion.keys()) > 0:
                    client.searchExternalKnow(keyspace,dicExternalKnow,clientID,procID)
                    if len(dicExternalKnow.keys()) > 0:
                        dicAlarmsLRePRec={}
                        usefulLibrary.validateExternalKnow(L_predict,dicLspecial,dicAlarmsLinealRegresion,dicExternalKnow,dicAlarmsLRePRec,procID)
                        client.updateAlarm(keyspace,clientID,datasetID,dicAlarmsLRePRec,dicLspecial['L2'],procID)
                # ---- NEW : 05.09.14
                dicAlarmsClusT={}
                dicAlarmsClusP={}
                score=0
                analysis=False
                # El array de entrenamiento puede ser demasiado pequeño comparado con el que deseamos predecir, en tal caso no podremos aplicar
                # el algoritmo de clustering, como norma general, el array de entrenamiento debe suponer el 75% del total
                log.info(procID+" <main> L_train is --> "+str((len(L_train)*100)/(len(L_train)+len(L_predict)))+"%")
                if (len(L_train)) == 0 or (len(L_train)*100)/(len(L_train)+len(L_predict)) < int(dicDBvariables['test_size']): # no hay suficientes dataset anteriores en la BD
                    log.info(procID+" <main> There are not enough datasets in DB, the clustering execution will not be executed.")
                else:
                    machineLearningLibrary.applyClusteringTotal(L_predict,features,L_train,dicDBvariables,dicAlarmsClusT,score,procID)
                    machineLearningLibrary.applyClusteringPartial(L_predict,features,L_train,dicDBvariables,dicAlarmsClusP,score,procID)
                    # dicAlarmsClusP es un diccionario por columna, es decir contiene X subdiccionarios del tipo dicAlarmsClusT (X=num columnas)                
                    analysis=True
                    if len(dicAlarmsClusT.keys()) > 0 or len(dicAlarmsClusP.keys()) > 0: # Hay alarmas
                        log.info(procID+" <main> There are new alarms.")
                        dicAlarmsClusPRec={} # diccionario con las alarmas rectificadas por el conocimiento externo
                        if len(dicAlarmsClusP.keys()) > 0: # Solo en el caso de el CLustering parcial (por columna) se usará la BD de conocimiento externo
                            # NEW --> 05.09.14 -- > ya lo hemos ejecutado antes --> client.searchExternalKnow(keyspace,dicExternalKnow,clientID,procID)
                            if len(dicExternalKnow.keys()) > 0:
                                usefulLibrary.validateExternalKnow(L_predict,dicLspecial,dicAlarmsClusP,dicExternalKnow,dicAlarmsClusPRec,procID) 
                            else:
                                log.info(procID+" <main> There are not external knowedge for this client.")
                            client.insertAlarmsClusCol(keyspace,clientID,datasetID,dicAlarmsClusP,dicAlarmsClusPRec,dicLspecial['L2'],procID)
                        client.insertAlarmsClusRow(keyspace,clientID,datasetID,dicAlarmsClusT,dicLspecial['L2'],procID)
                    else:
                        log.info(procID+" <main> There are not new alarms.")   
                # Actualizar la tabla DATASET con las alarmas.
                listAlarmsDB = client.searchAlarmsByDataSet(keyspace,clientID,datasetID,procID) 
                client.updateDatasetWithAlarms(keyspace,clientID,datasetID,listAlarmsDB,procID)                    
                #OLD --> usefulLibraryFiles.createJSONAlarms(datasetID,clientID,listAlarmsDB,pathF,procID)
                #
                # NEW: 04.09.14 -- Se cambia la generación de los ficheros JSON, ya no tendremos dos diferentes: 
                #                        * uno con el dataset generado por el proceso CLN
                #                        * otro con las alarmas de cada dataset generaro por el MNR
                #                  Ahora el proceso MNR creará un único fichero JSON en el que incluirá tanto el dataset como las alarmas asociadas a el 
                # 
                # Generamos los ficheros
                lisInformationDB = client.extractDatasetInformation(keyspace,clientID,datasetID,dicLspecial,procID) 
                # backup --> usefulLibraryFiles.createJSONAlarmsANDdataset(datasetID,clientID,listAlarmsDB,pathF,lisInformationDB,analysis,procID)
                usefulLibraryFiles.createJSONAlarmsANDdatasetv2(datasetID,clientID,listAlarmsDB,pathF,lisInformationDB,analysis,procID)
                log.info(procID+" <main> File Alarms&Dataset JSON created.")
                client.insertAnalysisInResult(keyspace,clientID,datasetID,analysis,procID)
                log.info(procID+" <main> Dataset updated as analyzed.")
        else:
            # No hay dataSet reciente reciente
            log.info(procID+" <main> There aren't recent dataset for analyzed.")
            log.info(procID+" <main> Applied variable searchWait, sleeping:"+str(dicDBvariables['searchWait'])+" s")
            time.sleep(float(dicDBvariables['searchWait']))
                            
if __name__ == "__main__":
    main()