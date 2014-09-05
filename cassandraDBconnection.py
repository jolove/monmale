#!/usr/bin/env python
# -*- coding: utf-8 -*-
import usefulLibrary         # Libreria propia del APP


from cassandra.cluster import Cluster
from cassandra.query import (PreparedStatement, BoundStatement, ValueSequence,
                             SimpleStatement, BatchStatement, BatchType,
                             dict_factory)

import logging
import sys, traceback
from datetime import datetime
from datetime import timedelta


log = logging.getLogger()
log.setLevel('INFO')

class connectionCassandra:
    session = None
    
    def connect(self, nodes):
        try:
            cluster = Cluster(nodes)
            metadata = cluster.metadata
            self.session = cluster.connect()
        except:
            log.error('DB connection problem¡¡.')
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)
        else:
            log.info('Connected to cluster: ' + metadata.cluster_name)
            for host in metadata.all_hosts():
                log.info('Datacenter: %s; Host: %s; Rack: %s',
                    host.datacenter, host.address, host.rack)
    
    def checkDBconnection(self,keyspace):
        try:
            query= " SELECT count(*) FROM "+keyspace+".Configuration ;"
            self.session.execute(query)
            log.info('Test queried.')
        except:
            log.error('DB query problem¡¡.')
            self.close()
            sys.exit(1)
                                        
    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')
    
    def loadConfiguration(self,keyspace,dicList):
        cad=''
        for i in dicList.keys():
            if (cad == ''):
                cad='\''+i+'\''
            else:
                cad=cad+','+'\''+i+'\''
        try:
            query= " SELECT variable,value FROM "+keyspace+".Configuration WHERE variable in ("+cad+");"
            R=self.session.execute(query)
            log.info('Configuration queried executed.')
        except:
            log.error('DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            records=len(R)
            if records != len(dicList):
                log.error('Do not exit all variables in DB.')
                # en este punto podríamos evitar abortar la ejecución poniendo un valor por defecto.
                self.close()
                sys.exit(1)
            else:
                log.info('Number of records recovered of DB: '+str(records))
                for j in range(records):
                    dicList[R[j][0]]=R[j][1]
                    
    def executeDBinsert(self,sentence,valuesList):
        try:
            self.session.execute(sentence,valuesList)
        except:
            log.error('DB insert problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info('Insert executed.')

    def loadConfigurationProc(self,keyspace,dicList,procID):
        for i in dicList.keys():
            try:
                query= " SELECT "+i+" FROM "+keyspace+".Process WHERE procID=\'"+procID+"\';"
                R=self.session.execute(query)
            except:
                log.error('DB query problem¡¡.')
                self.close()
                sys.exit(1)
            else:
                log.info('Configuration queried executed.')
                dicList[i]=R[0][0]
    
    def searchDBChildren(self,keyspace):
        listChil={}
        try:
            query= " SELECT bootseq,command,procid FROM "+keyspace+".Boot ;"
            R=self.session.execute(query)
        except:
            log.error('DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info('Boot queried executed.')
            log.info('Number of records found in DB= '+str(len(R)))
            for i in range(len(R)):
                log.info(' -Record '+str(i))
                log.info(' ---------------')
                log.info(R[i][0]) # Secuencia 
                log.info(R[i][1]) # comando
                log.info(R[i][2]) # ID del proceso
                log.info(' ---------------')
                # En la variable listChil almacenamos un diccionarios de diccionarios
                #    {{0{}},{1},{...}}
                #     
                # Ahora deberemos revisar si alguno de estos procesos tienen el flag
                # de stopped habilitado, en tal caso no deberemos incluirle en la 
                # lista de procesos a iniciar
                dicList={}
                dicList['stoppedflag']=''
                self.loadConfigurationProc(keyspace,dicList,R[i][2])
                log.info('Checking in DB for '+R[i][2]+' process, value for stoppedflag variable.')
                if dicList['stoppedflag'] == False:
                    log.info('    stoppedflag=False, new process added to list.')
                    interDic={}
                    interDic["command"]=R[i][1]
                    interDic["procID"]=R[i][2]
                    listChil[R[i][0]]=interDic
                    log.info('New child:')
                    log.info(listChil[R[i][0]])
            log.info('Children process found: ')
            for j in listChil.keys():
                log.info('    - '+listChil[j]["procID"])
            return listChil
    
    def searchClient(self,keyspace,clientID):
        try:
            query= "SELECT count(*) FROM "+keyspace+".Client WHERE clientID="+str(clientID)+";"
            R=self.session.execute(query)
        except:
            log.error('DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info('Client search queried executed.')
            if R[0][0] == 1:
                log.info("Client ID exists in DB.")
                return True
            else:
                if R[0][0] == 0:
                    log.info("Client ID does not exist in DB.")
                    return False
    
    def extractClientsFeatures(self,keyspace,dicClientsClassif):
        try:
            query= "SELECT clientid,nomcolumns,typecolumns FROM "+keyspace+".Client;"
            R=self.session.execute(query)
        except:
            log.error('DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info('Features client search queried executed.')
            log.info('Number of records found in DB= '+str(len(R)))
            for i in range(len(R)):
                log.info(' -Record '+str(i))
                log.info(' ---------------')
                log.info(R[i][0]) # clientID 
                log.info(R[i][1]) # nombres columnas -> es una lista
                log.info(R[i][2]) # tipo de las columnas -> es una lista
                log.info(' ---------------')
                subDic={}
                for j in range(len(R[i][1])):
                    subDic[R[i][1][j]]=R[i][2][j]
                dicClientsClassif[R[i][0]]=subDic
                log.info(dicClientsClassif[R[i][0]])
            log.info(dicClientsClassif)

    def searchRecentAlarm(self,keyspace,clientID,recentAlarm,lisAlarm,procID):
        initiDate=(datetime.now()-timedelta(0, int(recentAlarm))).strftime("%Y-%m-%d %H:%M:%S")
        finalDate=datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Ahora
        log.info(procID+" -select datasetid from "+keyspace+".Alarm where clientid="+str(clientID)+" and date >= '"+initiDate+"' and date <= '"+finalDate+"' ALLOW FILTERING;")
        try:
            query= "select datasetid from "+keyspace+".Alarm where clientid="+str(clientID)+" and date >= '"+initiDate+"' and date <= '"+finalDate+"' ALLOW FILTERING;"
            R=self.session.execute(query)
        except:
            log.error(procID+' - DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info(procID+' - Alarms search queried executed.')
            log.info(procID+' - Number of records found in DB= '+str(len(R)))
            for i in range(len(R)):
                log.info(procID+' - Record '+str(i))
                log.info(procID+' ----------------')
                log.info(R[i][0]) # dataSetID 
                log.info(procID+' ----------------')
            if len(R)>1:
                # significa que hay mas de un dataset en el intervalo de tiempo con alarma, por lo que nos quedaos con uno
                #ind=R.keys().index((sorted(R.keys(),reverse=True)[0]))
                lisAlarm.insert(0,R[0][0])
                log.info(procID+' DataSet return -> '+str(lisAlarm[0]))  
                
            else:
                if len(R)==1:
                    lisAlarm.insert(0,R[0][0])
                    log.info(procID+' DataSet return -> '+str(lisAlarm[0]))   
                # En caso contrario es que no hay alarma
    
    def loadTrainDataSet(self,keyspace,maxTrainDataSet,minTrainDataSet,L_train,features_train,clientID,dataUUID,only,alar,procID):
        # La busquerá se hará en dos partes:
        #   1ª. Sacaremos todos los dataset de un cliente --> 
        #                select datasetID,numRecords from monmale.DataSet WHERE clientID=X;
        #   2ª. Iremos uno a uno sacando los registros de cada dataset -->
        #               select alarm,col2,col3,col4,col5,col6,col7 FROM monmale.DataSet WHERE datasetID=xx-xx-xx order by recordN;
        #
        # CHANGE: 30.08.14 - Le añadimos dos parámetros mas a la llamada: "datasetUID" y "True/False" para poder hacer:
        #                    1.- Si el "datasetUID" está vacio ignorará el otro y en este caso no filtrará por datasetUID.
        #                    2.- Si el "datasetUID" no está vacio:
        #                       2.1.- Está a True => Sacará sólo las muestras de este dataset
        #                       2.2.- Está a False => Sacará todas las muestras de los dataset excepto las de este
        # CHANGE: 30.08.14 - Le añadimos un parámetro mas boolean para:
        #                    1.- Si es True sacará también las muestras alarmadas.
        #                    2.- Si es False no sacará las muestras alarmadas.
        include=True
        if dataUUID == "":
            query="select datasetID,numRecords from monmale.DataSet WHERE clientID="+str(clientID)+";"
        else:
            if only:
                query="select datasetID,numRecords from monmale.DataSet WHERE clientID="+str(clientID)+" AND datasetID="+str(dataUUID)+" ;"
            else:
                query="select datasetID,numRecords from monmale.DataSet WHERE clientID="+str(clientID)+";"
                include=False
        log.info(procID+" - "+query)
        try:
            J=self.session.execute(query)
            log.info(procID+' <loadTrainDataSet> Extraction train dataset queried executed step1.')
        except:
            log.error(procID+' <loadTrainDataSet> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else: 
            records=len(J)
            log.info(procID+' <loadTrainDataSet> DB records= '+str(records))
            log.info(procID+' <loadTrainDataSet> minTrainDataSet= '+str(minTrainDataSet))
            if records == 0 or records<int(minTrainDataSet):  # si no hay dataset para el cliente o no llega al mínimo
                # devolvemos la lista vacia, es el caso en el que sea nuevo
                log.info(procID+' <loadTrainDataSet> There are not data sets in DB.')
                del L_train[:]
            else:
                log.info(procID+' <loadTrainDataSet> Number of records recovered of DB: '+str(records))
                dicData={}
                cont=int(str(maxTrainDataSet))
                for row in range(records):
                    if (J[row][0] not in dicData.keys()) and cont>0:
                        log.info(procID+" - "+str(J[row][0])+" --> "+str(J[row][1]))
                        dicData[J[row][0]]=J[row][1]
                        cont=cont-J[row][1]
                log.info(procID+" <loadTrainDataSet> Number of samples to recover in DB:"+str(int(str(maxTrainDataSet))-cont))
                cad=''
                for i in features_train:
                    if (cad == ''):
                        cad=i
                    else:
                        cad=cad+','+i
                log.info(procID+' <loadTrainDataSet> DataUUID '+str(dataUUID))
                if include == False:
                    if dataUUID in dicData.keys(): # Ya que como tenemos un máximo de líneas en el dataset puede que no se haya cargado
                        del dicData[dataUUID]
                    log.info(procID+" <loadTrainDataSet> DatasetUUD "+str(dataUUID)+" deleted because it is excluded.")
                for dataset in dicData.keys():
                    query= "SELECT recordN,alarm,"+cad+" FROM "+keyspace+".DataSet WHERE datasetID="+str(dataset)+" ORDER BY recordN;"
                    log.info(procID+" - "+query)
                    try:
                        # No sacaremos las muestras alarmadas ya que estas presentan anomalías y queremos el set de entrenamiendo lo mas limpio posible,
                        # para aplicar el algoritmo de clustering en el proceso MNR si cargaremos todos.
                        # aunque sacamos las columnas datasetID,recordN,alarm en la query, a la hora de crear el array las quitaremos
                        #
                        R=self.session.execute(query)
                        log.info(procID+' - Extraction train dataset queried executed step2')
                    except:
                        log.error(procID+' - DB query problem¡¡.')
                        self.close()
                        sys.exit(1)
                    else:
                        rec=len(R)
                        log.info(procID+' - Number of records recovered of DB: '+str(rec))
                        for row in range(rec):
                            log.info(procID+" - Row:"+str(row))
                            sublis=[]
                            if R[row][1] == False or alar == True: # Si alar es True sacamos las muestras alarmadas
                                for col in range(len(features_train)): # cuidado que tenemos que quitar las 2 primeras
                                    log.info(procID+" - "+str(features_train[col])+" --> "+str(R[row][col+2]))
                                    v, t = usefulLibrary.convertString(R[row][col+2])
                                    sublis.append(v)
                                L_train.append(sublis)
                            else:
                                log.info(procID+" - "+str(row)+" does not load, it is alarmed.")
                log.info(procID+' - Number of samples recovered: '+str(len(L_train)))

    def insertNewDataSet(self,keyspace,dicHeader,L_union,dicL0,dicAlarmsRegr,procID):
        # Iremos recorriendo el array insertando cada valor (en la BD son siempre strings)
        # usaremos el procedimiento antes definido executeDBinsert(self,sentence,valuesList)
        # y la función usefulLibrary.createRecordHistory(procID,keyspace) que devuelve insertHist,valuesList,historyID
        # El orden será:
        #   1. createRecordHistory(procID,keyspace)
        #   2. executeDBinsert(self,sentence,valuesList)
        cont=0    
        for row in L_union:
            a=False
            for alarm in dicAlarmsRegr.keys():
                if cont == dicAlarmsRegr[alarm]["x"]: # existe una alarma en esta muestra
                    a=True
            inserData,valuesList = usefulLibrary.createRecordDataset(procID,keyspace,dicL0,dicHeader,a,len(L_union),cont,row)
            self.executeDBinsert(inserData,valuesList)
            cont=cont+1

    def enoughColumns(self,keyspace,columns,procID):
        try:
            query= "select column_name from system.schema_columns where columnfamily_name='dataset' ALLOW FILTERING;"
            R=self.session.execute(query)
        except:
            log.error(procID+' <enoughColumns> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info(procID+' <enoughColumns> Columns number query executed.')
            log.debug(procID+' <enoughColumns> Number of records found in DB= '+str(len(R)))
            if len(R) >= (columns+6): # Hay 6 columnas fijas
                return True, 0, ""
            else:
                listCols=[]
                for i in range(len(R)):
                    log.debug(procID+' <enoughColumns> Record '+str(i))
                    log.debug(procID+' <enoughColumns> ---------------')
                    log.debug(R[i][0]) # column name 
                    log.debug(procID+' <enoughColumns> ---------------')
                    if "col" in str(R[i][0]):
                        listCols.append(int(str(R[i][0]).split("col")[1]))
                log.debug(sorted(listCols))
                needCol=columns-len(listCols)
                log.debug(procID+' <enoughColumns> Columns nedded: '+str(needCol))
                return False, needCol, sorted(listCols)[len(listCols)-1]
                
    def createNewColDataset(self,keyspace,colName,procID):
        alter= "ALTER TABLE "+keyspace+".dataset ADD "+str(colName)+" varchar;"
        try:
            R=self.session.execute(alter)
        except:
            log.error(procID+' <createNewColDataset> DB alter problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.info(procID+' <createNewColDataset> New column created '+str(colName))
                
    def insertNewDataSetCheck(self,keyspace,dicHeader,L_union,dicL0,dicAlarmsRegr,procID):
        # Iremos recorriendo el array insertando cada valor (en la BD son siempre strings)
        # usaremos el procedimiento antes definido executeDBinsert(self,sentence,valuesList)
        # y la función usefulLibrary.createRecordHistory(procID,keyspace) que devuelve insertHist,valuesList,historyID
        # El orden será:
        #   1. createRecordHistory(procID,keyspace)
        #   2. executeDBinsert(self,sentence,valuesList)
        # NEW: 03.09.14 - Creamos este procedimiento como evolución del "insertNewDataSet", para que en el caso de que tengamos un número
        #                 menor de columnas en la tabla creemos las correspondientes
        enough, needCol,lastCol = self.enoughColumns(keyspace,len(L_union[0]),procID)
        if enough == False:
            cont=lastCol
            for col in range(needCol):
                cont=cont+1 
                colName="col"+str(cont)
                self.createNewColDataset(keyspace,colName,procID)
        cont=0    
        for row in L_union:
            a=False
            for alarm in dicAlarmsRegr.keys():
                if cont == dicAlarmsRegr[alarm]["x"]: # existe una alarma en esta muestra
                    a=True
            inserData,valuesList = usefulLibrary.createRecordDataset(procID,keyspace,dicL0,dicHeader,a,len(L_union),cont,row)
            self.executeDBinsert(inserData,valuesList)
            cont=cont+1
   
    def insertResult(self,keyspace,dicHeader,timeProcessing,score,procID): 
        log.info(procID+" <insertResult> Creating sentence ....")
        inserData,valuesList = usefulLibrary.createRecordResult(keyspace,dicHeader,timeProcessing,score,procID)
        log.info(procID+" <insertResult> Inserting ....")
        self.executeDBinsert(inserData,valuesList)
    
    def insertAlarmS(self,keyspace,dicHeader,dicAlarmReal,dicL0,alarmed,procID):
        for alarm in dicAlarmReal.keys():
            inserData,valuesList = usefulLibrary.createRecordAlarm(keyspace,dicHeader['datasetID'],dicHeader['clientID'],dicAlarmReal[alarm]['x'],dicL0[dicAlarmReal[alarm]['y']],alarmed,'LOW',False,procID)
            self.executeDBinsert(inserData,valuesList)
    
    def searchDatasetNoAnalysed(self,keyspace,dicPendingData,procID):
        log.info(procID+" <searchDatasetNoAnalysed> select datasetid,clientid from "+keyspace+".Result where pending=True;")
        try:
            query= "select datasetid,clientid from "+keyspace+".Result where pending=True;"
            R=self.session.execute(query)
        except:
            log.error(procID+' <searchDatasetNoAnalysed> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.debug(procID+' <searchDatasetNoAnalysed> Pendnig result search queried executed.')
            log.debug(procID+' <searchDatasetNoAnalysed> Number of records found in DB= '+str(len(R)))
            for i in range(len(R)):
                log.debug(procID+' <searchDatasetNoAnalysed> Record '+str(i))
                log.debug(procID+' <searchDatasetNoAnalysed> ----------------')
                log.debug(R[i][0]) # dataSetID 
                log.debug(R[i][1]) # clientID 
                log.debug(procID+' <searchDatasetNoAnalysed> ----------------')
                dicPendingData[R[i][0]]=R[i][1]

    def extractClientsFeaturesOrder(self,keyspace,dicClientsClassifOrder):
        try:
            query= "SELECT clientid,nomcolumns,typecolumns FROM "+keyspace+".Client;"
            R=self.session.execute(query)
        except:
            log.error(' <extractClientsFeaturesOrder> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.debug(' <extractClientsFeaturesOrder> Features client search queried executed.')
            log.debug(' <extractClientsFeaturesOrder> Number of records found in DB= '+str(len(R)))
            for i in range(len(R)):
                log.debug(' <extractClientsFeaturesOrder> Record '+str(i))
                log.debug(' <extractClientsFeaturesOrder> ---------------')
                log.debug(R[i][0]) # clientID 
                log.debug(R[i][1]) # nombres columnas -> es una lista
                log.debug(R[i][2]) # tipo de las columnas -> es una lista
                log.debug(' <extractClientsFeaturesOrder> ---------------')
                sub=[]
                for j in range(len(R[i][1])): # Recorro la lista de los nombres de las columnas
                    subDic={}
                    subDic['name']=R[i][1][j]
                    subDic['type']=R[i][2][j]
                    sub.insert(j,subDic)
                dicClientsClassifOrder[R[i][0]]=sub
                log.debug(dicClientsClassifOrder[R[i][0]])
            log.debug(dicClientsClassifOrder)
    
    def searchExternalKnow(self,keyspace,dicExternalKnow,clientID,procID):
        # Este procedimiento devolverá todas las entradas de conocimiento externo para un determinado cliente en un diccionario.
        try:
            query= "SELECT externalID,columnID,value,lower,equal FROM "+keyspace+".ExternalKnowedge WHERE clientID="+str(clientID)+";"
            R=self.session.execute(query)
        except:
            log.error(' <searchExternalKnow> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.debug(' <searchExternalKnow> External  search queried executed.')
            log.debug(' <searchExternalKnow> Number of records found in DB= '+str(len(R)))
            for i in range(len(R)):
                log.debug(' <searchExternalKnow> Record '+str(i))
                log.debug(' <searchExternalKnow> ---------------')
                log.debug(R[i][0]) # externalID 
                log.debug(R[i][1]) # columnID
                log.debug(R[i][2]) # value
                log.debug(R[i][3]) # lower
                log.debug(R[i][4]) # equal
                log.debug(' <searchExternalKnow> ---------------')
                sub={}
                sub['column']=R[i][1]
                sub['value']=R[i][2]
                sub['lower']=R[i][3]
                sub['equal']=R[i][4]
                dicExternalKnow[R[i][0]]=sub
                log.debug(dicExternalKnow[R[i][0]])
            log.debug(dicExternalKnow)
    
    def searchExactAlarm(self,keyspace,clientID,datasetID,row,columnName):
        query= "SELECT alarmed,externalRectification,level FROM "+keyspace+".Alarm WHERE datasetID="+str(datasetID)+" AND clientID="+str(clientID)+" AND recordN="+str(row)+" AND column=\'"+str(columnName)+"\';"
        log.info(' <searchExactAlarm> '+str(query))
        try:
            R=self.session.execute(query)
        except:
            log.error(' <searchExactAlarm> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.debug(' <searchExactAlarm> Exact alarm search queried executed.')
            log.debug(' <searchExactAlarm> Number of records found in DB= '+str(len(R))) # Como mucho será 1
            if len(R) > 0:
                log.debug(' <searchExactAlarm> Record 1')
                log.debug(' <searchExactAlarm> ---------------')
                log.debug(R[0][0]) # alarmed --> es una lista 
                log.debug(R[0][1]) # externalRectification
                log.debug(R[0][2]) # level
                log.debug(' <searchExactAlarm> ---------------')
                log.debug(R[0][1])
                log.debug(R[0][2])
                alarmed=[]
                if str(R[0][0]) != 'None':
                    for j in range(len(R[0][0])):
                        alarmed.insert(j,R[0][0][j])
                return alarmed,R[0][1],R[0][2]
            else:
                log.debug(' <searchExactAlarm> There is not alarm.')
                return "","",""
                
    def insertAlarmsClusRow(self,keyspace,clientID,datasetID,dicAlarmsClusT,dicL2special,procID):
        for row in dicAlarmsClusT.keys():
            columnName='all_columns' # así nos referiremos a una alarma sonbre una muestra completa
            alarmed,extRec,level = self.searchExactAlarm(keyspace,clientID,datasetID,row,columnName)
            if alarmed == "" and extRec == "" and level =="": # No había alarma anterior
                alarmed=[]
                alarmed.append(2)
                inserData,valuesList = usefulLibrary.createRecordAlarm(keyspace,datasetID,clientID,row,columnName,alarmed,dicAlarmsClusT[row],False,procID)
                self.executeDBinsert(inserData,valuesList)
            else: # había alarma anterior
                if 2 not in alarmed: # Si ya está en la lista este tipo de alarma no la metemos otra vez (se habrá ejecutado dos veces el proc)
                    alarmed.append(2) # Nuevo tipo de alarma
                if level == 'LOW':
                    newLevel = dicAlarmsClusT[row]
                elif level == 'MEDIUM' and dicAlarmsClusT[row] in ['CRITICAL','HIGH']:
                    newLevel = dicAlarmsClusT[row]
                elif level == 'HIGH' and dicAlarmsClusT[row] == 'CRITICAL':
                    newLevel = dicAlarmsClusT[row]
                elif level == 'CRITICAL':
                    newLevel = level
                else:
                    newLevel = level
                inserData,valuesList = usefulLibrary.createRecordAlarm(keyspace,datasetID,clientID,row,columnName,alarmed,newLevel,False,procID)
                self.executeDBinsert(inserData,valuesList)
        
    def insertAlarmsClusCol(self,keyspace,clientID,datasetID,dicAlarmsClusP,dicAlarmsClusPRec,dicL2special,procID):
        for col in dicAlarmsClusP.keys():
            log.debug(" <insertAlarmsClusCol> Column: "+str(col))
            log.debug(" <insertAlarmsClusCol> Column name: "+str(dicL2special[sorted(dicL2special.keys())[col]]))
            columnName=dicL2special[sorted(dicL2special.keys())[col]]
            for row in dicAlarmsClusP[col].keys():
                alarmed,extRec,level = self.searchExactAlarm(keyspace,clientID,datasetID,row,columnName)
                if alarmed == "" and extRec == "" and level =="": # No había alarma anterior
                    alarmed=[]
                    alarmed.append(3)
                    inserData,valuesList = usefulLibrary.createRecordAlarm(keyspace,datasetID,clientID,row,columnName,alarmed,dicAlarmsClusP[col][row],False,procID)
                    self.executeDBinsert(inserData,valuesList)
                else: # había alarma anterior
                    if 3 not in alarmed: # Si ya está en la lista este tipo de alarma no la metemos otra vez (se habrá ejecutado dos veces el proc)
                        alarmed.append(3) # Nuevo tipo de alarma
                    if level == 'LOW':
                        newLevel = dicAlarmsClusP[col][row]
                    elif level == 'MEDIUM' and dicAlarmsClusP[col][row] in ['CRITICAL','HIGH']:
                        newLevel = dicAlarmsClusP[col][row]
                    elif level == 'HIGH' and dicAlarmsClusP[col][row] == 'CRITICAL':
                        newLevel = dicAlarmsClusP[col][row]
                    elif level == 'CRITICAL':
                        newLevel = level
                    else:
                        newLevel = level
                    inserData,valuesList = usefulLibrary.createRecordAlarm(keyspace,datasetID,clientID,row,columnName,alarmed,newLevel,False,procID)
                    self.executeDBinsert(inserData,valuesList)
        if len(dicAlarmsClusPRec.keys()) > 0: #puede darse el caso de que el diccionario esté vacio porque no hay registros para el cliente en la
            # BD de conocimiento esterno.
            for alarm in dicAlarmsClusPRec.keys():
                columnName=str(dicL2special[sorted(dicL2special.keys())[dicAlarmsClusPRec[alarm]['column']]])
                log.debug(" <insertAlarmsClusCol> Column: "+str(col))
                log.debug(" <insertAlarmsClusCol> Column name: "+str(columnName))  
                log.debug(" <insertAlarmsClusCol> Row: "+str(dicAlarmsClusPRec[alarm]['row'])) 
                alarmed,extRec,level = self.searchExactAlarm(keyspace,clientID,datasetID,dicAlarmsClusPRec[alarm]['row'],columnName.decode('unicode-escape'))
                if alarmed == "" and extRec == "" and level =="": # No había alarma anterior
                    alarmed=[]
                    alarmed.append(3)
                    inserData,valuesList = usefulLibrary.createRecordAlarm(keyspace,datasetID,clientID,dicAlarmsClusPRec[alarm]['row'],columnName.decode('unicode-escape'),alarmed,dicAlarmsClusPRec[alarm]['level'],True,procID)
                    self.executeDBinsert(inserData,valuesList)
                else: # había alarma anterior, en este caso no debemos modificarla ya que la anterior no ha sido rectificada
                    log.debug(" <insertAlarmsClusCol> Rectification doesn't , there is previous alarm.")
    
    def searchAlarmsByDataSet(self,keyspace,clientID,datasetID,procID):
        query= "SELECT recordn,column,alarmed,date,level,externalrectification FROM "+keyspace+".Alarm WHERE datasetID="+str(datasetID)+" AND clientID="+str(clientID)+";"
        log.info(procID+' <searchAlarmsByDataSet> '+str(query))
        try:
            R=self.session.execute(query)
        except:
            log.error(procID+' <searchAlarmsByDataSet> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            log.debug(procID+' <searchAlarmsByDataSet> Extract alarm search queried executed.')
            log.debug(procID+' <searchAlarmsByDataSet> Number of records found in DB= '+str(len(R))) # Como mucho será 1
            for index in range(len(R)):
                log.debug(procID+' <searchAlarmsByDataSet> Record '+str(index))
                log.debug(procID+' <searchAlarmsByDataSet> ---------------')
                log.debug(R[index][0]) # recordn
                log.debug(R[index][1]) # column
                log.debug(R[index][2]) # alarmed --> es una lista 
                log.debug(R[index][3]) # date 
                log.debug(R[index][4]) # level  
                log.debug(R[index][5]) # externalRectification
                log.debug(procID+' <searchAlarmsByDataSet> ---------------')
            else:
                log.debug(procID+' <searchAlarmsByDataSet> There is not alarm.')
            return R
    
    def insertAnalysisInResult(self,keyspace,clientID,datasetID,procID):
        inserData,valuesList = usefulLibrary.updateRecordResult(keyspace,clientID,datasetID,procID)
        self.executeDBinsert(inserData,valuesList)
    
    def updateDatasetWithAlarms(self,keyspace,clientID,datasetID,listAlarmsDB,procID):
        for i in range(len(listAlarmsDB)):
            recordn=listAlarmsDB[i][0]
            inserData,valuesList = usefulLibrary.updateRecordDataset(keyspace,clientID,datasetID,recordn,procID)
            self.executeDBinsert(inserData,valuesList)
    
    def extractDatasetInformation(self,keyspace,clientID,datasetID,dicLspecial,procID):
        col="col"
        for index in range(len(dicLspecial['L0'])):
            if col == "col":
                col=str(col)+str(index)
            else:
                col=str(col)+",col"+str(index)
        log.info(procID+' <extractDatasetInformation> Dinamic columns --> '+str(col))        
        query= "SELECT recordn,date,numRecords,alarm,"+str(col)+" FROM "+keyspace+".dataset WHERE datasetID="+str(datasetID)+" AND clientID="+str(clientID)+";"
        log.info(procID+' <extractDatasetInformation> '+str(query))
        try:
            R=self.session.execute(query)
        except:
            log.error(procID+' <extractDatasetInformation> DB query problem¡¡.')
            self.close()
            sys.exit(1)
        else:
            lis=[]
            log.debug(procID+' <extractDatasetInformation> Extract search queried executed.')
            log.debug(procID+' <extractDatasetInformation> Number of records found in DB= '+str(len(R))) 
            for index in range(len(R)):
                subDic={}
                log.debug(procID+' <extractDatasetInformation> Record '+str(index))
                log.debug(procID+' <extractDatasetInformation> ---------------')
                log.debug(R[index][0]) # recordn
                subDic['recordn']=R[index][0]
                log.debug(R[index][1]) # date
                subDic['times']=R[index][1]
                log.debug(R[index][2]) # numRecords
                subDic['numRecords']=R[index][2]
                log.debug(R[index][3]) # alarm
                subDic['alarm']=R[index][3]
                for i in range(len(dicLspecial['L0'])):
                    log.debug(procID+' <extractDatasetInformation> Value: '+str(R[index][4+i]))
                    log.debug(procID+' <extractDatasetInformation> Key: '+str(dicLspecial['L0'][i]))
                    subDic[dicLspecial['L0'][i]]=R[index][4+i]
                log.debug(procID+' <extractDatasetInformation> ---------------')
                lis.append(subDic)
            return lis