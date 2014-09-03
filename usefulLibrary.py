#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys, traceback, os, subprocess
import uuid
import psutil
import getpass

import usefulLibraryFiles         # Libreria propia del APP
import numpy as np

from math import sqrt


from datetime import datetime

log = logging.getLogger()
log.setLevel('INFO')

def createUUID():
    return uuid.uuid4()

def extensionDate():
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")

def updateRecordResult(keyspace,clientID,datasetID,procID):
    valuesList=[]
    valuesList.insert(0,datasetID)
    valuesList.insert(1,clientID)
    valuesList.insert(2,False)
    insert="INSERT INTO "+keyspace+".Result (datasetID,clientID,pending) VALUES (%s,%s,%s)"
    return insert,valuesList  

def updateRecordDataset(keyspace,clientID,datasetID,recordn,procID):
    valuesList=[]
    valuesList.insert(0,datasetID)
    valuesList.insert(1,clientID)
    valuesList.insert(2,recordn)
    valuesList.insert(3,True)
    insert="INSERT INTO "+keyspace+".Dataset (datasetID,clientID,recordn,alarm) VALUES (%s,%s,%s,%s)"
    log.info(procID+' <updateRecordDataset> insert --> '+insert)
    log.info(procID+' <updateRecordDataset> values --> '+str(valuesList))
    return insert,valuesList        

def createRecordResult(keyspace,dicHeader,timeProcessing,score,procID):
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    datasetID=uuid.UUID(str(dicHeader["datasetID"]))
    clientID=dicHeader["clientID"]
    valuesList=[]
    valuesList.insert(0,datasetID)
    valuesList.insert(1,clientID)
    valuesList.insert(2,date)
    valuesList.insert(3,timeProcessing)
    valuesList.insert(4,score)
    valuesList.insert(5,True) # NEW: 30.08.14
    insert="INSERT INTO "+keyspace+".Result (datasetID,clientID,date,processingTime,regressionScore,pending) VALUES (%s,%s,%s,%s,%s,%s)"
    log.info(procID+' <createRecordResult> insert --> '+insert)
    log.info(procID+' <createRecordResult> values --> '+str(valuesList))
    return insert,valuesList

def createRecordAlarm(keyspace,data,clientID,recordN,column,alarmed,level,extRec,procID):
    # NEW: 31.08.14 --> cambios en el esquema de la tabla
    datasetID=uuid.UUID(str(data))
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #method viene en la llamada
    #alarmed viene en la llamada, es de tipo list<int>  
    valuesList=[]
    valuesList.insert(0,datasetID)
    valuesList.insert(1,clientID)
    valuesList.insert(2,recordN)
    valuesList.insert(3,column)
    valuesList.insert(4,date) 
    valuesList.insert(5,alarmed)
    valuesList.insert(6,level)  
    valuesList.insert(7,extRec) 
    insert="INSERT INTO "+keyspace+".Alarm (datasetID,clientID,recordN,column,date,alarmed,level,externalRectification) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    log.info(procID+' <createRecordAlarm> insert --> '+insert)
    log.info(procID+' <createRecordAlarm> values --> '+str(valuesList))
    return insert,valuesList
    
def createRecordHistory(procID,keyspace):
    historyID=createUUID()
    #procID=<vendrá como parámetro>
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    PID=os.getpid()
    hostname=os.uname()[1]
    lastReading=date
    valuesList=[]
    valuesList.insert(0,historyID)
    valuesList.insert(1,procID)
    valuesList.insert(2,date)
    valuesList.insert(3,PID)
    valuesList.insert(4,hostname)
    valuesList.insert(5,lastReading)
    insert="INSERT INTO "+keyspace+".History (historyID,procID,date,PID,hostname,lastReading) VALUES (%s,%s,%s,%s,%s,%s)"
    log.debug(procID+' <createRecordHistory> insert --> '+insert)
    log.debug(procID+' <createRecordHistory> values --> '+str(valuesList))
    return insert,valuesList,historyID

def createRecordDataset(procID,keyspace,dicL0,dicHeader,alarm,numRecords,recordN,row):
    # PASOS:
    #   1. Asignamos los valores fijos.
    #   2. Asignamos los valores dinámicos (colx)
    #
    # Valores fijos:
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    datasetID=uuid.UUID(str(dicHeader["datasetID"]))
    clientID=dicHeader["clientID"]
    # recordN nos lo pasan en la llamada
    # numRecords nos lo pasan en la llamada
    valuesList=[]
    valuesList.insert(0,datasetID)
    valuesList.insert(1,recordN)
    valuesList.insert(2,clientID)
    valuesList.insert(3,date)
    valuesList.insert(4,numRecords)
    valuesList.insert(5,alarm)
    insert_p1="INSERT INTO "+keyspace+".DataSet (datasetID,recordN,clientID,date,numRecords,alarm"
    # Valores dinámicos:
    dinamicV=""
    values=""
    for value in range(len(dicL0)):
        valuesList.insert(6+value,str(row[value]))
        dinamicV=dinamicV+",col"+str(value)
        values=values+","+"%s"
    # INSERT INTO DataSet (datasetID,recordN,clientID,date,numRecords,alarm,col0,col1,col2,col3,col4,col5,col6,col7)
    insert_p2=dinamicV
    insert_p3=") VALUES (%s,%s,%s,%s,%s,%s"
    insert_p4=values
    insert_p5=")"
    insert=insert_p1+insert_p2+insert_p3+insert_p4+insert_p5
    log.info(procID+' <createRecordDataset> insert --> '+insert)
    log.info(procID+' <createRecordDataset> values --> '+str(valuesList))
    return insert,valuesList

def checkVariables(oldDict,newDict):
        if oldDict == newDict:
            log.info('Both dictionaries are equal.')
            return False
        else:
            log.info('There are changes in DB configuration.')
            return True

def usedVariablesProc(dic):
    # Crearemos un diccionario con todas las variables del PROCESO (monmale.Process) necesarias que están definidas en la BD
    dic['numinstances']=''
    dic['clusterused']=''
    dic['clusterid']=''
    dic['stoppedflag']=''

def searchOSChildren(listChil):
    currentUser=getpass.getuser()
    try:
        for seq in listChil.keys():
            log.info("Searching process "+str(listChil[seq]["procID"])+" in machine.")
            continu=True
            for pid in psutil.get_pid_list():
                if continu:
                    proc=psutil.Process(pid)
                    log.info('Checked process of SO '+proc.name)
                    try:
                        if (proc.username == currentUser) and (listChil[seq]["command"] in proc.cmdline) and (proc != 1):
                        # Buscamos en la lista de procesos todos los procesos que cumplan con lo siguiente:
                        #       1. Tengan el mismo usuario que el nuestro
                        #       2. Tengan en su lines de ejecución el comando de nuestro proceso
                        #       3. Y cuyo pid no sea el init
                            log.info("Process "+listChil[seq]["procID"]+" already running with PID:"+str(proc))
                            del listChil[seq]
                            continu=False
                            # Si el proceso hijo ya está corriendo lo eliminamos del diccionario
                    except:
                        log.error("Error happend to search process PID in comparation.")
    except:
        log.error("Error happend to search process PID.")

def startOSChildren(listChil,dbHost,keyspace):
    orderKeys=listChil.keys()
    orderKeys.sort()
    log.info(orderKeys)
    for i in orderKeys:
        log.info("Boot sequence: "+str(i)+" | Process: "+listChil[i]["procID"])
        try:
            os.system('nohup '+listChil[i]["command"]+' '+listChil[i]["procID"]+' '+keyspace+' '+dbHost+' &')
            # Ejecutamos el comando en segundo plano
            log.info('Commnad executed: '+listChil[i]["command"])
        except:
            log.error('Error happend to execution start children command: '+listChil[i]["command"])

def loadArguments():
    if len(sys.argv) != 4:
        log.error("Wrong number of arguments in initial call.")
        log.error(sys.argv)
        sys.exit(1)
    else:
        # 1er Argumento: procID
        # 2do Argumento: keyspace
        # 3er Argumento: dbHost
        log.info("Right number of arguments in initial call:")
        dic={}
        dic["dbHost"]=sys.argv[3]
        dic["keyspace"]=sys.argv[2]
        procID=sys.argv[1]
        log.info("    -1st Arg -> "+sys.argv[1])
        log.info("    -2sn Arg -> "+sys.argv[2])
        log.info("    -3st Arg -> "+sys.argv[3])
        return dic,procID

def convertString(stri):
    # Por el momento vamos a trabajar con estas conversiones:
    # string --> int
    # string --> float
    # string --> string (string y date)
    try:
        return int(stri),"int"
    except ValueError:
        try:
            stri=stri.replace(",",".")
            return float(stri),"float"
        except ValueError:
            return stri,"str"

def applyClassification(dicClientsClassification,decodedDict):
    # dicClientsClassification --> diccionario de diccionarios con cada cliente y sus características principales para clasificar el data set
    # decodedDict --> diccionario con las características principales del fichero a clasificar
    equal=True
    clientsID=[]
    for i in dicClientsClassification.keys():
        # en cada interación estamos cogiendo a cada cliente
        log.info("Searching in DIC client "+str(i))
        if sorted(dicClientsClassification[i].keys()) !=  sorted(decodedDict.keys()):
            equal=False
            log.info("    - Features client don't same that file (features names).")
        else:
            for j in dicClientsClassification[i].keys():
                if dicClientsClassification[i][j] != decodedDict[j]:
                    equal=False
                    log.info("    - Features client don't same that file (features type).")
        if equal:
            clientsID.append(i)
    if len(clientsID)>1:
        log.info("Error, there are several clients with the same features for new File")
        return False,""
    if len(clientsID)==0:
        log.info("Error, there are not clients with the same features that the new File")
        return False,""
    if len(clientsID)==1:
        log.info("Client found with the same features that the new File")
        ID=clientsID[0]
        return True,ID

def cleaningDataSet(dicFeaturesClient,normalValues_X,normalFeature_names,procID):
    # Esta función eliminará del array de NumPy todas las columnas en formato string para que posteriormente pueda ser usado por los
    # algoritmos de ML.
    # Devolverá tres objetos:
    #   L1 --> array normal con las columnas strings "descartadas"
    #   L2 --> NumPy array con las columnas "seleccionadas"
    #   dicL --> diccionario:
    #               L0 --> {0=columna0 , 1=columna1, 2=columna2 , 3=columna3 , 4=columna4} // posicion_original=nombre_columna
    #               L1 --> {1=columna1 , 4=columna4}
    #               L2 --> {0=columna0 , 2=columna2 , 3=columna3}
    # Con estos tres objetos seremos capaces de "montar" nuevamente el array original para insertarlo en la BD con los nuevos valores calculados 
    print ""
    des=[]
    sel=[]
    dicL={}
    L1 , L2 = [] , []
    for i in dicFeaturesClient.keys():
        if dicFeaturesClient[i] == "str":
            # Tenemos que descartarla
            des.append(i)
        else:
            # Tenemos que seleccionarla
            sel.append(i)
    log.info(procID+" - Features discard (strings columns): "+str(des))
    log.info(procID+" - Features select: "+str(sel))
    # Creamos el diccionario que nos ayudará a montar de nuevo el array completo
    # --- L0
    subDic={}
    for col in range(len(normalFeature_names)):
        subDic[col]=normalFeature_names[col]
    dicL["L0"]=subDic
    #--- L1
    subDic={}
    for col in range(len(normalFeature_names)):
        if normalFeature_names[col] in des:
            subDic[col]=normalFeature_names[col]
    dicL["L1"]=subDic
    #--- L2 
    subDic={}
    for col in range(len(normalFeature_names)):
        if normalFeature_names[col] in sel:
            subDic[col]=normalFeature_names[col]
    dicL["L2"]=subDic 
    log.info(procID+" - Dictionay dicL[L0]")
    log.info(procID+"        > columns position: "+str(dicL["L0"].keys()))
    log.info(procID+"        > columns names: "+str(dicL["L0"].values()))
    log.info(procID+" - Dictionay dicL[L1]")
    log.info(procID+"        > columns position: "+str(dicL["L1"].keys()))
    log.info(procID+"        > columns names: "+str(dicL["L1"].values()))
    log.info(procID+" - Dictionay dicL[L2]")
    log.info(procID+"        > columns position: "+str(dicL["L2"].keys()))
    log.info(procID+"        > columns names: "+str(dicL["L2"].values()))
    # Ahora tenemos que recorrer el array
    for row in normalValues_X:
        subLisL1=[]
        subLisL2=[]
        for col in dicL["L1"].keys():
            subLisL1.append(row[col])
        L1.append(subLisL1)
        for col in dicL["L2"].keys():
            subLisL2.append(row[col])
        L2.append(subLisL2)
    log.info(procID+" - L1:")
    log.info(procID+str(L1))
    log.info(procID+" - L2:")
    log.info(procID+str(L2))
    return L1,L2,dicL

def searchANDremplaceStr(L,dicWrongValues,procID):
    for x in range(len(L)):
        for y in range(len(L[x])):
            v , t = convertString(L[x][y])
            if t == "str":
                L[x][y]=0
                coord={}
                coord["x"]=x
                log.info(procID+" - X coord wrong value: "+str(x))
                coord["y"]=y
                log.info(procID+" - Y coord wrong value: "+str(y))
                dicWrongValues[len(dicWrongValues.keys())]=coord
    
    log.info(procID+" - Number of coordinates by wrong values: "+str(dicWrongValues.keys()))
    
def cleanNewDataSet(dicFeaturesClient,path,csvFile,procID):
    # En esta función se llevarán a cabo las tareas de:
    #     - selección --> se seleccionaran las linéas del fichero como entradas del dataSet
    #     - limpieza  --> al ir introduciendo los valores sustituiremos aquellos que no existan o que
    #                     no sean del tipo adecuado por el correspondiente valor nulo (especificaremos)
    #                     previamente cual es el valor nulo de cada tipo.
    #     - transformación --> a media que vamos leyendo el fichero iremos transformando en el tipo adecuado
    #                          cada valor.
    # ETAPA de Selección:
    #
    normalValues_X,normalFeature_names=usefulLibraryFiles.readCVSfile(path,csvFile,procID) # Llamaremos para que se habra el fichero y se descomponga
    #
    # ETAPA de limpieza y transformación:
    # NOTA: en este punto estaría bien revisar los posibles valores de una columna string y en el caso de ser pocos asignarles un número
    #       y de esta forma que los algoritmos de scikit-learning puedan trabajar con esta columna. Por el momento lo dejamos como posible
    #       mejora.
    #   Quitar las columnas con string pero guardarlas para cuando tengamos que insertarlas en la BD
    #
    L1,L2,dicL = cleaningDataSet(dicFeaturesClient,normalValues_X,normalFeature_names,procID)
    #
    #   Antes de crear el array de NumPy debemos verificar que no se nos ha "colado" ningún valor string dentro de alguna de las columnas que no
    #   lo son (dará un error al crear el array NumPy si tiene algún valor no numérico). Como en el caso de que no tenga un tipo correcto no
    #   queremos ni eliminar la muestra completa (la línea) ni queremos ponerle un valor por defecto lo que haremos será:
    #       1. Cambiaremos el valor por "0"
    #       2. Guardaremos en un diccionario cada coordenada de los valores extraños.
    #       3. Cuando apliquemos la regresión lineal tendremos debería saltarnos una alarma para esta coordenada, con lo que la verificaremos en
    #          nuestro diccionario y si es de las "controladas" pondremos el valor "predecido" y NO generaremos alarma.
    #
    dicWrongValues={}
    searchANDremplaceStr(L2,dicWrongValues,procID)
    return L1,L2,dicL,dicWrongValues

def unionLists(L2,L_train,procID):
    L_full=L_train
    for row in range(len(L2)):
        L_full.append(L2[row])
    return L_full
    
def unionListsWrong(dicWrongValues,L2,L_train,procID):
    L_full=L_train
    wrong = bool
    for row in range(len(L2)):
        wrong = False
        for reg in dicWrongValues.keys():
            if dicWrongValues[reg]["x"] == row:
                log.info(procID+" - X coord wrong value founds in row: "+str(row)+" , this row will not be add to L full")
                wrong = True
        if wrong == False:
            L_full.append(L2[row])
            log.info(procID+" - "+str(row)+" , this row has been add to L full")
    return L_full

def extractColumnArray(L_full,col,procID):
    L_1col, L_restCol = [], []
    for row in range(len(L_full)):
        subLre = []
        log.debug(procID+" <extractColumnArray> ROWn: "+str(row))
        for colum in range(len(L_full[row])):
            log.debug(procID+" <extractColumnArray> COLUMNn: "+str(colum))
            if colum == col: # esta es la columna que debemos sacar
                L_1col.append(L_full[row][colum])
                log.debug(procID+" <extractColumnArray> This column has been removed: "+str(colum))
            else:
                subLre.append(L_full[row][colum])
                log.debug(procID+" <extractColumnArray> This column has been add the rest list: "+str(colum))
        L_restCol.append(subLre)
    return L_1col, L_restCol
          
def extractColumnList(L,col,procID):
    # De una lista (row) nos quedaremos con todas las columnas excepto con la "col"
    newLis=[]
    for colum in range(len(L)):
        if colum != col: # esta es la columna que debemos sacar
            newLis.append(L[colum])
            log.info(procID+" <extractColumnList> This column has been extracted: "+str(colum))
    return newLis

def desviacionTipica(num1,num2,procID):
    if num1 == 0 and num2 == 0:
        log.info(procID+" <desviacionTipica> Both numbers are 0.")
        coefVariacion=0
    else:
        avg=(num1+num2)/2
        varianza=((avg-num1)**2+(avg-num2)**2)/2
        desviacion=sqrt(varianza)
        coefVariacion=(desviacion/avg)*100
    return coefVariacion

def unionArrays(L1,L2,dicL,procID):
    L_union=[]
    for row in range(len(L1)):
        subLunion=[]
        contL1=0
        contL2=0
        log.debug(procID+" <unionArrays> ROWn: "+str(row))
        for col in range(len(dicL['L0'])):
            log.debug(procID+" <unionArrays> Coln: "+str(col))
            if dicL['L0'][col] in dicL['L1'].values():
                subLunion.append(L1[row][contL1])
                log.debug(procID+" <unionArrays> Column in L1")
                contL1=contL1+1
            else:
                if dicL['L0'][col] in dicL['L2'].values():
                    subLunion.append(L2[row][contL2])
                    log.debug(procID+" <unionArrays> Column in L2")
                    contL2=contL2+1
                else:
                    log.error(procID+" <unionArrays> Column doesn't exist into L1 and L2")
        L_union.append(subLunion)
    return L_union
    
def realCoord(dicAlarmsRegr,dicL,procID):
    log.info(procID+" <realCoord> Num of alarms: "+str(len(dicAlarmsRegr.keys())))
    newDic=dicAlarmsRegr
    for alarm in dicAlarmsRegr.keys():
        indexL2=0
        log.info(procID+" <realCoord> Alarm num: "+str(alarm))
        log.info(procID+" <realCoord> Old coord Y= "+str(dicAlarmsRegr[alarm]["y"]))
        log.info(procID+" <realCoord> Current coord X= "+str(dicAlarmsRegr[alarm]["x"]))
        for colL2 in sorted(dicL["L2"].keys()):
            if indexL2 == dicAlarmsRegr[alarm]["y"]:
                columOrder=colL2
            indexL2=indexL2+1
        indexL0=0
        for colL0 in sorted(dicL["L0"].keys()):
            if colL0 == columOrder:
                log.info(procID+" <realCoord> New coord Y= "+str(indexL0))
                newDic[alarm]["y"]=indexL0
            indexL0=indexL0+1
    return newDic 

def startAgent(nameCnfFile,dicDBvariables,client,procID): 
    # commandAgentStart --> /usr/bin/java -jar /opt/logstash/logstash.jar
    # logstashAgentCnf  --> /etc/logstash/shipper_<clientID>.cnf
    # logPath           --> /opt/logs/shipper_<clientID>.log
    # nohup /usr/bin/java -jar /opt/logstash/logstash.jar agent -v -f ${logstash_conf} --log ${logstash_log} > /dev/null 2>&1 &
    comm=dicDBvariables['commandAgentStart']+" agent -v -f "+nameCnfFile+" --log "+dicDBvariables['logPath']+"/shipper_"+str(client)+".log"
    log.info(procID+" <startAgent> "+str(comm))
    listArg =  comm.split(" ")
    try:
        proc = subprocess.Popen(listArg)
        # Ejecutamos el comando en segundo plano, no es necesario con nohup, creamos un objeto del tipo subprocess
        log.info(procID+" <startAgent> Commnad executed.")
    except:
        log.error(procID+" <startAgent> Error happend to execution start agent command.")
    return proc
    

def applyOIFA(dicDBvariables,dicAgentProcess,clientsList,procID): 
    nameCnfFile=""
    nameCnfFile = usefulLibraryFiles.createCnfAgentSFile(dicDBvariables,clientsList,procID)
    proc = startAgent(nameCnfFile,dicDBvariables,"allClients",procID)
    dicAgentProcess[proc.pid]=proc

def applyOIFE(dicDBvariables,dicAgentProcess,clientsList,procID):
    # dicAgentProcess --> es un diccionario de objetos del tipo "subprocess"
    for client in clientsList:
        nameCnfFile=""
        nameCnfFile = usefulLibraryFiles.createCnfAgentFile(dicDBvariables,client,procID)
        proc = startAgent(nameCnfFile,dicDBvariables,client,procID)
        dicAgentProcess[proc.pid]=proc
        
def applyAlgorithmDistribution(dicDBvariables,dicAgentProcess,clientsList,procID):
    if dicDBvariables["algorithmDis"]=="oneInstanceForAll":
        log.info(procID+" <applyAlgorithmDistribution> Algorithm distibution: oneInstanceForAll")
        applyOIFA(dicDBvariables,dicAgentProcess,clientsList,procID)
    else:
        if dicDBvariables["algorithmDis"]=="oneInstanceForEach":
            log.info(procID+" <applyAlgorithmDistribution> Algorithm distibution: oneInstanceForEach")
            applyOIFE(dicDBvariables,dicAgentProcess,clientsList,procID)
    
    
def killAgents(dicAgentProcess,procID):
    for procPID in dicAgentProcess.keys():
        log.info(procID+" <killAgents> Agent "+str(procPID)+" terminated.")
        dicAgentProcess[procPID].terminate()
        
def createDicL(dicClientsClassifOrder,clientID,dicLspecial,procID): 
    sub={}
    subL1={}
    subL2={}
    for order in range(len(dicClientsClassifOrder[clientID])):
        log.debug(procID+" <createDicL> Creating L0 ...")
        log.debug(procID+" <createDicL>     -> "+str(order)+" = "+str(dicClientsClassifOrder[clientID][order]['name']))
        sub[order]=dicClientsClassifOrder[clientID][order]['name']
    dicLspecial['L0']=sub
    for columnPos in range(len(dicLspecial['L0'])):  
        if str(dicClientsClassifOrder[clientID][columnPos]['type']) == 'str':
            subL1[columnPos]=dicLspecial['L0'][columnPos]
        else:
            subL2[columnPos]=dicLspecial['L0'][columnPos]
    dicLspecial['L1']=subL1
    dicLspecial['L2']=subL2
    for L in sorted(dicLspecial.keys()): 
        log.debug(procID+" <createDicL> "+L+" :")
        for col in sorted(dicLspecial[L].keys()):
            log.debug(procID+" <createDicL>     "+str(col)+"-->"+str(dicLspecial[L][col]))
             
def saveResult(y_pred,dicResultClusSample,dicResultClusGroup,itera,procID):
    for i in range(len(y_pred)):
        if i in dicResultClusSample.keys():
            if len(dicResultClusSample[i].keys()) == 0:
                subDic={}
                subDic[itera]=y_pred[i]
                dicResultClusSample[i]=subDic
            else:
                subDic=dicResultClusSample[i]
                subDic[itera]=y_pred[i]
                dicResultClusSample[i]=subDic
        else:
            subDic={}
            subDic[itera]=y_pred[i]
            dicResultClusSample[i]=subDic
        log.debug(procID+" <saveResult> Iter:"+str(itera)+"--row:"+str(i)+"--group:"+str(y_pred[i]))
        if itera in dicResultClusGroup.keys():
            if y_pred[i] in dicResultClusGroup[itera].keys():
                dicResultClusGroup[itera][y_pred[i]]=dicResultClusGroup[itera][y_pred[i]]+1
                log.debug(procID+" <saveResult> Group "+str(y_pred[i])+" exists in dicResultClusGroup")
            else:
                log.debug(procID+" <saveResult> Group "+str(y_pred[i])+" doesn't exist in dicResultClusGroup")
                dicResultClusGroup[itera][y_pred[i]]=1
        else:
            subDic={}
            subDic[y_pred[i]]=1
            dicResultClusGroup[itera]=subDic

def frecuencyGroup(total,elemsGroup,procID):
    fr=(elemsGroup*100)/total
    log.debug(procID+" <frecuencyGroup> "+str(fr))
    return fr

def searchRow(dicResultClusSample,itera,group,procID):
    rowsLis=[]
    for row in dicResultClusSample.keys():
        if dicResultClusSample[row][itera] == group:
            rowsLis.append(row)
    return rowsLis
        
    
def applyClusteringAlarm(dicResultClusSample,dicResultClusGroup,dicAlarmsClus,clustGroup,procID):
        # Este procedimiento tiene como objetivo devolver un diccionario con las filas alarmadas y la criticidad de la alarma. Para ello tiene
        # en cuenta los siguientes critérios:
        #   1.- Usará la variable clustGroup para determinar si un grupo está alarmado (ej: si un grupo tiene un 1% o menos de elementos que el
        #    resto producirá una alarma).
        #   2.- La criticidad de la alarma viene dada por em qué "profundidad" se da el grupo alarmado:
        #       CRITICAL --> se da en la 1ª iteración o en la 2ª y 3ª consecutivamente.
        #       HIGH     --> se da en la 2ª iteración o en la 3ª y 4ª consecutivamente.
        #       MEDIUM   --> se da en la 3ª iteración.
        #       LOW      --> se da en la 4ª iteración.
        # 
        for itera in sorted(dicResultClusGroup.keys()):
            for group in sorted(dicResultClusGroup[itera].keys()):
                if frecuencyGroup(len(dicResultClusSample.keys()),dicResultClusGroup[itera][group],procID) <= int(clustGroup):
                    log.info(procID+" <applyClusteringAlarm> "+str(itera)+" --G"+str(group)+" has alarm.")
                    # Vemos el nivel de la alarma
                    if itera == 'I0': # es la primera iteración
                        level="CRITICAL"
                    if itera == 'I1': # es la segunda iteración
                        level="HIGH"
                    if itera == 'I2': # es la tercera iteración
                        level="MEDIUM"
                    if itera == 'I3': # es la cuarta iteración
                        level="LOW"
                    rowsLis=searchRow(dicResultClusSample,itera,group,procID)
                    for row in rowsLis:
                        if row in dicAlarmsClus.keys(): #significa que ya tiene una alarma
                            if itera == 'I2' and dicAlarmsClus[row] == "HIGH" :
                                level="CRITICAL"
                            elif itera == 'I3' and dicAlarmsClus[row] == "MEDIUM" :
                                level="HIGH" 
                            else:
                                level=dicAlarmsClus[row] # Nos quedamos con el anterior que será mayor     
                                log.info(procID+" <applyClusteringAlarm> Row: "+str(row)+" has previous alarm with level: "+str(level))                       
                        dicAlarmsClus[row]=level
                        log.info(procID+" <applyClusteringAlarm> Row: "+str(row)+" has alarm with level: "+str(level))
                else:
                    log.info(procID+" <applyClusteringAlarm> "+str(itera)+" --G"+str(group)+" hasn't alarm.")

def removeAlarm(dicAlarmsClusP,dicAlarmsClusPRec,col,row,procID):
    sub={}
    sub['column']=col
    sub['row']=row
    sub['level']=dicAlarmsClusP[col][row]
    dicAlarmsClusPRec[len(dicAlarmsClusPRec)+1]=sub
    log.debug(procID+" <removeAlarm> len dicAlarmsClusP in col: "+str(col)+"-->"+str(len(dicAlarmsClusP[col])))
    del dicAlarmsClusP[col][row]

def searchColumnPosition(colName,dicL2special,procID):
    pos=0
    for col in sorted(dicL2special.keys()):
        if dicL2special[col] == colName:
            return pos
        else:
            pos=pos+1

def validateExternalKnow(L_predict,dicLspecial,dicAlarmsClusP,dicExternalKnow,dicAlarmsClusPRec,procID):
    # Es te procedimiento tendrá como objetivo validar cada una de las alarmas del diccionario dicAlarmsClusP (el cual las tiene definidas)
    # por columna contra los datos de conocimiemto esterno del cliente (representados por medio del diccionario dicExternalKnow). Como resultado
    # se "dividirá" el diccionario de alarmas en dos:
    #               - uno con las alarmas no corregidas por la información externa.
    #               - otro con las alarmas corregidas por la información externa.
    # NOTA: cuidado tenemos elementos con las columnas reales y otros con las columnas no strings:
    #            - Columnas reales: dicExternalKnow
    #            - Columnas no strings: L_predict,dicL2special,dicAlarmsClusP
    for extID in sorted(dicExternalKnow.keys()):
        if dicExternalKnow[extID]['column'] in dicLspecial['L2'].values(): # significa que la columna es una de las no strings
            posL2 = searchColumnPosition(dicExternalKnow[extID]['column'],dicLspecial['L2'],procID)
            if  posL2 in dicAlarmsClusP.keys(): # significa que la columna incluida en la BD de conocieminto externa, que tiene la posición posL2
                #                                 dentro de nuestros arrays no strings existe en el diccionario de alarmas
                log.debug(procID+" <validateExternalKnow> Column "+str(posL2))
                colName=dicLspecial['L2'][posL2]
                log.debug(procID+" <validateExternalKnow> Column name "+str(colName))
                log.debug(procID+" <validateExternalKnow> Column name "+str(colName)+" has external knowedge.")
                for row in sorted(dicAlarmsClusP[posL2].keys()):
                    log.debug(procID+" <validateExternalKnow> Row "+str(row))
                    if dicExternalKnow[extID]['equal']:
                        if dicExternalKnow[extID]['lower']:
                            if L_predict[row][posL2] <= dicExternalKnow[extID]['value']:
                                removeAlarm(dicAlarmsClusP,dicAlarmsClusPRec,posL2,row,procID)
                                log.debug(procID+" <validateExternalKnow> Alarm removed --> COL: "+str(posL2)+" ROW: "+str(row))
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" <= to external value: "+str(dicExternalKnow[extID]['value']))
                            else:
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" > to external value: "+str(dicExternalKnow[extID]['value'])+", alarm doesn't removed.")
                        else:
                            if L_predict[row][posL2] >= dicExternalKnow[extID]['value']:
                                removeAlarm(dicAlarmsClusP,dicAlarmsClusPRec,posL2,row,procID)
                                log.debug(procID+" <validateExternalKnow> Alarm removed --> COL: "+str(posL2)+" ROW: "+str(row))
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" >= to external value: "+str(dicExternalKnow[extID]['value']))
                            else:
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" < to external value: "+str(dicExternalKnow[extID]['value'])+", alarm doesn't removed.")
                    else:
                        if dicExternalKnow[extID]['lower']:
                            if L_predict[row][posL2] < dicExternalKnow[extID]['value']:
                                removeAlarm(dicAlarmsClusP,dicAlarmsClusPRec,posL2,row,procID)
                                log.debug(procID+" <validateExternalKnow> Alarm removed --> COL: "+str(posL2)+" ROW: "+str(row))
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" < to external value: "+str(dicExternalKnow[extID]['value']))
                            else:
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" > to external value: "+str(dicExternalKnow[extID]['value'])+", alarm doesn't removed.")
                        else:
                            if L_predict[row][posL2] > dicExternalKnow[extID]['value']:
                                removeAlarm(dicAlarmsClusP,dicAlarmsClusPRec,posL2,row,procID)
                                log.debug(procID+" <validateExternalKnow> Alarm removed --> COL: "+str(posL2)+" ROW: "+str(row))
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" > to external value: "+str(dicExternalKnow[extID]['value']))
                            else:
                                log.debug(procID+" <validateExternalKnow> Real value: "+str(L_predict[row][posL2])+" < to external value: "+str(dicExternalKnow[extID]['value'])+", alarm doesn't removed.")
        else: # al ser una columna string no la alarmamos en el aplicativo por lo que pasamos del registro de conocimiento externo
            log.debug(procID+" <validateExternalKnow> the column "++" is a string column, it doesn't analyzed by APP")
    log.debug(dicAlarmsClusP) 
    log.debug(dicAlarmsClusPRec)

# -- CLASES