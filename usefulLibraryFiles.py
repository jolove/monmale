#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys, traceback, os
import json, csv
import glob
import usefulLibrary            # APP library
from datetime import datetime


log = logging.getLogger()
log.setLevel('INFO')

def readJSONfile(pathFile):
    try:
        dic=json.load(open(pathFile))
    except:
        log.error('Error to load JSON file: '+pathFile)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    else:
        log.info('JSON file'+pathFile+' loads succesfuly.')
        return dic

def searchFilesIN(path,extension,procID):
    try:
        os.access(path, os.F_OK)
        log.info("Path -> "+path+" is accessible.")
    except:
        log.error("Path -> "+path+" is not accessible.")
    else:
        os.chdir(path)
        text_files=glob.glob(extension)
        text_files.sort()
        if len(text_files)>0:
            log.info("New file found: "+text_files[0])
            return text_files[0],True
        else:
            return " ",False

def readHeaderFile(path,newFile):
    # Debemos definir el formato de la cabecera:
    #    * Formato json
    #    * Número de líneas limitado -> 4
    #    * Campos:
    #    *    - clientID
    #    *    - datasetID -----> este campo vendrá vacio al principio pero luego será insertado
    txtFile=open(path+"/"+newFile,'r')
    index=0
    jsonStr=""
    while index<4:
        jsonStr=jsonStr+" "+txtFile.readline()
        index=index+1
    txtFile.close()
    try:
        decoded = json.loads(jsonStr)
        # pretty printing of json-formatted string
        log.info(json.dumps(decoded, sort_keys=False, indent=4))
    except (ValueError, KeyError, TypeError):
        log.error("JSON format error")
        return decoded,True
    else:
        return decoded,False

def moveFile(pathI,pathE,newFile):
    try:
        os.access(pathE, os.F_OK)
        log.info("Path -> "+pathE+" is accessible.")
    except:
        log.error("Path -> "+pathE+" is not accessible.")
        return True
    else:
        try:
            os.system("mv "+pathI+"/"+newFile+" "+pathE+"/")
        except:
            log.error("Error to move file to path -> "+newFile)
            return True
        else:
            log.info("File moved to path -> "+newFile)
            return False

def createCopyFile(pathI,newFile,extension):
    try:
        os.access(pathI, os.F_OK)
        log.info("Path -> "+pathI+" is accessible.")
    except:
        log.error("Path -> "+pathI+" is not accessible.")
        return True
    else:
        try:
            os.system("cp "+pathI+"/"+newFile+" "+pathI+"/"+newFile.replace(".txt","")+extension+".txt")
            log.info("cp "+pathI+"/"+newFile+" "+pathI+"/"+newFile.replace(".txt","")+extension+".txt")
        except:
            log.error("Error to copy file to path -> "+newFile)
            return True,""
        else:
            log.info("File copied -> "+newFile)
            return False,newFile.replace(".txt","")+extension+".txt"

def addNewDataID(pathI,fileWithExt,dataSet):
    try:
        os.access(pathI, os.F_OK)
        log.info("Path -> "+pathI+" is accessible.")
    except:
        log.error("Path -> "+pathI+" is not accessible.")
        return True
    else:
        log.info("sed -e 's/\"datasetID\": \"\"/\"datasetID\": \""+str(dataSet)+"\"/g' "+pathI+"/"+fileWithExt+" > "+pathI+"/"+fileWithExt+"_tmp")
        try:
            os.system("sed -e 's/\"datasetID\": \"\"/\"datasetID\": \""+str(dataSet)+"\"/g' "+pathI+"/"+fileWithExt+" > "+pathI+"/"+fileWithExt+"_tmp")
            os.system("mv "+pathI+"/"+fileWithExt+"_tmp "+pathI+"/"+fileWithExt)
        except:
            log.error("Error to edit file -> "+fileWithExt)
            return True
        else:
            log.info("File edited -> "+fileWithExt)
            return False   
            
def readNomFieldFile(path,newFile):
    txtFile=open(path+"/"+newFile,'r')
    index=0
    nomFieldStr=""
    valuesLine=""
    # Leeremos tanto la cabecera como la primera línea de valores
    while index<6:
        if index==4:
            nomFieldStr=txtFile.readline()
        else:
            if index==5:
                valuesLine=txtFile.readline()
            else:     
                txtFile.readline()
        index=index+1
    txtFile.close()
    try:
        decodedField = nomFieldStr.strip().split("|")
        log.info("<readNomFieldFile> "+str(decodedField))
        decodedValues = valuesLine.strip().split("|")
        # strip --> eliminar el retorno de carro y los posibles espacios
    except:
        log.error("<readNomFieldFile> Text file in format error")
        return True,decodedField
    else:
        # Vamos a convertir las dós líneas en un diccionario cambiando los valores de los campos por el tipo apropiado
        decoded={}
        for i in range(len(decodedValues)):
            value,typ=usefulLibrary.convertString(decodedValues[i])
            decoded[decodedField[i]]=typ
        log.info("<readNomFieldFile> "+str(decoded))
        return False,decoded

def readCVSfile(path,csvFile,procID):
    # Esta función pasará a variables el contenido de un fichero CSV:
    #   - Header
    #   - Values
    # Se ignorarán las primeras 4 líneas, ya que en ellas están en formato json variables que ya conocemos
    # Devolveremos una lista de listas y otra lista con los nombres de las columnas, mas tarde usaremos los arrays de Numpy 
    with open(path+"/"+csvFile, 'rb') as f:
        reader = csv.reader(f,delimiter='|')
        rownum = 0
        # Load dataset, and target classes
        values_X = []
        for row in reader:
            if rownum == 4:
                header=row
            else:
                if rownum > 4:
                    row2=[]
                    for elem in row:
                        v,t=usefulLibrary.convertString(elem)
                        row2.append(v)
                    values_X.append(row2)
            rownum += 1
        feature_names = header
        log.info(procID+" - Features loaded: "+str(feature_names))
        return values_X,feature_names

def createJSONDataset(dicAlarmReal,dicHeader,dicL0,L_union,pathF,lisAlarm,procID):
    # Formato de nuestro JSON
    # {clientID:3,datasetID:b37acfbc-cb1c-4b80-9826-5c957e8ed9d1,timestamp:2014-08-23 12:53:59+0200,recentAlarm:False,currentAlarm:True,
    # col0: value0,col1: value1,col2: value2, alarms: [col1,col2]}
    # 1º. Creamos el diccionario con los valores.
    # 2º. Lo pasamos a json.
    # 3º. Lo pasamos a string
    # 4º. Sustinuimos las \n por ""
    # -- Ya tendríamos un string por muestra listo para insertar en archivo
    # NEW: los ficheros json los crearemos en la carpeta de cada cliente
    log.info(procID+" <createJSONDataset> Creating JSON file ...")
    datasetID=str(dicHeader["datasetID"])
    clientID=dicHeader["clientID"]
    #if len(dicAlarmReal.keys()) > 0:
    #    currentAlarm = True
    #else:
    #    currentAlarm = False
    if len(lisAlarm) > 0:
        recentAlarm = True
    else:
        recentAlarm = False
    jsonFile=open(pathF+'/'+str(clientID)+'/'+str(datasetID)+'_Dataset.json', 'w')
    for sample in range(len(L_union)):
        interData={}
        timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        interData['clientID']=clientID
        interData['datasetID']=datasetID
        interData['timestamp']=timestamp
        interData['recentAlarm']=recentAlarm
        interData['recordn']=sample
        interData['featureAlarmed']=""
        interData['currentAlarm']=False
        interData['level']=""
        for a in dicAlarmReal.keys():
            if (dicAlarmReal[a]['x'] == sample):
                interData['currentAlarm']=True   
                interData['level']="LOW"
                # Aunque tuviera mas de una alarma la misma fila el valore de currentAlarm sería el mismo, se irá machacando
                if interData['featureAlarmed'] == "":
                    interData['featureAlarmed']=str(dicL0[dicAlarmReal[a]['y']])
                else:
                    interData['featureAlarmed']=str(interData['featureAlarmed'])+","+str(dicL0[dicAlarmReal[a]['y']])
                log.debug(procID+" <createJSONDataset> Feature alarmed: "+str(dicL0[dicAlarmReal[a]['y']]))
        for column in range(len(L_union[sample])):
            interData[dicL0[column]]=L_union[sample][column]
        jsonFile.write(str(json.dumps(interData,ensure_ascii=False,indent=7,sort_keys=True)).replace("\n","")+"\n")
    jsonFile.close()
    
def createJSONDatasetWithoutAlarm(dicAlarmReal,dicHeader,dicL0,L_union,pathF,lisAlarm,procID):
    # Formato de nuestro JSON
    # {clientID:3,datasetID:b37acfbc-cb1c-4b80-9826-5c957e8ed9d1,timestamp:2014-08-23 12:53:59+0200,recentAlarm:False,currentAlarm:True,
    # col0: value0,col1: value1,col2: value2, alarms: [col1,col2]}
    # 1º. Creamos el diccionario con los valores.
    # 2º. Lo pasamos a json.
    # 3º. Lo pasamos a string
    # 4º. Sustinuimos las \n por ""
    # -- Ya tendríamos un string por muestra listo para insertar en archivo
    # NEW: los ficheros json los crearemos en la carpeta de cada cliente
    # NEW: 03.09.2014 - Creamos este procedimiento para generar los JSON de Dataset sin las alarmas, ya que estas las saca mas tarde el proceso
    #                   MNR, y si no estaríamos duplicando
    log.info(procID+" <createJSONDatasetWithoutAlarm> Creating JSON file ...")
    datasetID=str(dicHeader["datasetID"])
    clientID=dicHeader["clientID"]
    if len(lisAlarm) > 0:
        recentAlarm = True
    else:
        recentAlarm = False
    jsonFile=open(pathF+'/'+str(clientID)+'/'+str(datasetID)+'_Dataset.json', 'w')
    for sample in range(len(L_union)):
        interData={}
        timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        interData['clientID']=clientID
        interData['datasetID']=datasetID
        interData['timestamp']=timestamp
        interData['recentAlarm']=recentAlarm
        interData['recordn']=sample
        #interData['featureAlarmed']=""
        interData['currentAlarm']=False
        interData['level']="none"
        for a in dicAlarmReal.keys():
            if (dicAlarmReal[a]['x'] == sample):
                interData['currentAlarm']=True   
        for column in range(len(L_union[sample])):
            interData[dicL0[column]]=L_union[sample][column]
        jsonFile.write(str(json.dumps(interData,ensure_ascii=False,indent=7,sort_keys=True)).replace("\n","")+"\n")
    jsonFile.close()

def createJSONAlarms(datasetID,clientID,listAlarmsDB,pathF,procID):
    # Formato de nuestro JSON
    #  {clientID:3,datasetID:b37acfbc-cb1c-4b80-9826,recordn: 14,column: %idle,timestamp:2014-08-23 12:53:59+0200,alarmType:[1,2],level: LOW,externalRectification: True}
    jsonFile=open(pathF+'/'+str(clientID)+'/'+str(datasetID)+'_Alarms.json', 'w')
    for index in range(len(listAlarmsDB)):
        dicAlarms={}
        dicAlarms['datasetID']=str(datasetID)
        dicAlarms['clientID']=clientID
        dicAlarms['recordn']=listAlarmsDB[index][0]
        dicAlarms['featureAlarmed']=str(listAlarmsDB[index][1])
        kind=""
        for k in range(len(listAlarmsDB[index][2])):
            if kind == "":
                kind=str(listAlarmsDB[index][2][k])
            else:
                kind=kind+","+str(listAlarmsDB[index][2][k])
        dicAlarms['alarmType']=kind
        date,hour = str(listAlarmsDB[index][3]).split(" ") 
        log.debug(procID+" <createJSONAlarms> date: "+str(date)+" hour: "+str(hour))
        # IN DB   --> 2014-09-01 16:37:45+0200
        # in file --> 2014-08-28T18:01:48.000Z
        dicAlarms['timestamp']=str(date)+"T"+str(hour)+".000Z"
        dicAlarms['level']=str(listAlarmsDB[index][4])
        dicAlarms['externalRectification']=listAlarmsDB[index][5]
        log.debug(dicAlarms)
        jsonFile.write(str(json.dumps(dicAlarms,ensure_ascii=False,indent=7,sort_keys=True)).replace("\n","")+"\n")
    jsonFile.close()
    
def createCnfAgentFile(dicDBvariables,client,procID):
    # NEW: 01.09.14: añadimos un fichero nuevo por cliente con los logs creador por el proceso MNR con las alarmas
    # Formato:
    # input {
    # file {
    #	    type => "clientID_DS"
    #       path => ["/home/jorge/sar/clientID_DataSet.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_AL"
    #       path => ["/home/jorge/sar/clientID_Alarms.log"]
    #       codec => "json"
    #	  }
    # }
    # output {
    #    stdout { }
    #    redis {
    #        host => "192.168.56.101"
    #        data_type => "list"
    #        key => "logstash"
    #    } 
    # }
    dicDBvariables['logstashAgentCnf']
    dicDBvariables['commandAgentStart']
    dicDBvariables['redisHost']
    dicDBvariables['redisKey']
    dicDBvariables['finalPath'] # Lo modificaremos y lo usaremos para los ficheros de los agentes
    try:
        cnfFile=open(str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf', 'w')
    except:
        log.error(procID+" <createCnfAgentFile> Problem to create cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf')
    else:
        log.info(procID+" <createCnfAgentFile> Creating cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf')
        cnfFile.write("input {\n")
        cnfFile.write("    file {\n")
        cnfFile.write("          type => \""+str(client)+"_DS\" \n")
        cnfFile.write("          path => [\""+str(dicDBvariables['finalPath'])+"/"+str(client)+"_DataSet.log\"] \n")
        cnfFile.write("          codec => \"json\" \n")
        cnfFile.write("         }\n")
        cnfFile.write("    file {\n")
        cnfFile.write("          type => \""+str(client)+"_AL\" \n")
        cnfFile.write("          path => [\""+str(dicDBvariables['finalPath'])+"/"+str(client)+"_Alarms.log\"] \n")
        cnfFile.write("          codec => \"json\" \n")
        cnfFile.write("         }\n")        
        cnfFile.write("      }\n")
        cnfFile.write("output {\n")
        cnfFile.write("    stdout { } \n")
        cnfFile.write("    redis {\n")
        cnfFile.write("          host => \""+str(dicDBvariables['redisHost'])+"\" \n")
        cnfFile.write("          data_type => \"list\" \n")
        cnfFile.write("          key => \""+str(dicDBvariables['redisKey'])+"\" \n")
        cnfFile.write("         }\n")
        cnfFile.write("      }\n")
        cnfFile.close()
    nameCnfFile=str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf'
    return nameCnfFile

def createCnfAgentFilev2(dicDBvariables,client,procID):
    # NEW: 01.09.14: añadimos un fichero nuevo por cliente con los logs creador por el proceso MNR con las alarmas
    # Formato:
    # input {
    # file {
    #	    type => "clientID_DS"
    #       path => ["/home/jorge/sar/clientID_DataSet.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_AL"
    #       path => ["/home/jorge/sar/clientID_Alarms.log"]
    #       codec => "json"
    #	  }
    # }
    # output {
    #    stdout { }
    #    redis {
    #        host => "192.168.56.101"
    #        data_type => "list"
    #        key => "logstash"
    #    } 
    # }
    dicDBvariables['logstashAgentCnf']
    dicDBvariables['commandAgentStart']
    dicDBvariables['redisHost']
    dicDBvariables['redisKey']
    dicDBvariables['finalPath'] # Lo modificaremos y lo usaremos para los ficheros de los agentes
    try:
        cnfFile=open(str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf', 'w')
    except:
        log.error(procID+" <createCnfAgentFilev2> Problem to create cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf')
    else:
        log.info(procID+" <createCnfAgentFilev2> Creating cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf')
        cnfFile.write("input {\n")
        cnfFile.write("    file {\n")
        cnfFile.write("          type => \""+str(client)+"_DS\" \n")
        cnfFile.write("          path => [\""+str(dicDBvariables['finalPath'])+"/"+str(client)+"_DataSet.log\"] \n")
        cnfFile.write("          codec => \"json\" \n")
        cnfFile.write("         }\n")     
        cnfFile.write("      }\n")
        cnfFile.write("output {\n")
        cnfFile.write("    stdout { } \n")
        cnfFile.write("    redis {\n")
        cnfFile.write("          host => \""+str(dicDBvariables['redisHost'])+"\" \n")
        cnfFile.write("          data_type => \"list\" \n")
        cnfFile.write("          key => \""+str(dicDBvariables['redisKey'])+"\" \n")
        cnfFile.write("         }\n")
        cnfFile.write("      }\n")
        cnfFile.close()
    nameCnfFile=str(dicDBvariables['logstashAgentCnf'])+'/shipper_'+str(client)+'.cnf'
    return nameCnfFile

def createCnfAgentSFile(dicDBvariables,clientsList,procID):
    # NEW: 01.09.14: añadimos un fichero nuevo por cliente con los logs creador por el proceso MNR con las alarmas
    # Formato:
    # input {
    # file {
    #	    type => "clientID_1_DS"
    #       path => ["/home/jorge/sar/clientID_1_DataSet.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_1_AL"
    #       path => ["/home/jorge/sar/clientID_1_Alarms.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_2_DS"
    #       path => ["/home/jorge/sar/clientID_2_DataSet.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_2_AL"
    #       path => ["/home/jorge/sar/clientID_2_Alarms.log"]
    #       codec => "json"
    #	  }
    # }
    # output {
    #    stdout { }
    #    redis {
    #        host => "192.168.56.101"
    #        data_type => "list"
    #        key => "logstash"
    #    } 
    # }
    dicDBvariables['logstashAgentCnf']
    dicDBvariables['commandAgentStart']
    dicDBvariables['redisHost']
    dicDBvariables['redisKey']
    dicDBvariables['finalPath'] # Lo modificaremos y lo usaremos para los ficheros de los agentes
    try:
        cnfFile=open(str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf', 'w')
    except:
        log.error(procID+" <createCnfAgentSFile> Problem to create cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf')
    else:
        log.info(procID+" <createCnfAgentSFile> Creating cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf')
        cnfFile.write("input {\n")
        for client in clientsList:
            cnfFile.write("    file {\n")
            cnfFile.write("          type => \""+str(client)+"_DS\" \n")
            cnfFile.write("          path => [\""+str(dicDBvariables['finalPath'])+"/"+str(client)+"_DataSet.log\"] \n")
            cnfFile.write("          codec => \"json\" \n")
            cnfFile.write("         }\n")
            cnfFile.write("    file {\n")
            cnfFile.write("          type => \""+str(client)+"_AL\" \n")
            cnfFile.write("          path => [\""+str(dicDBvariables['finalPath'])+"/"+str(client)+"_Alarms.log\"] \n")
            cnfFile.write("          codec => \"json\" \n")
            cnfFile.write("         }\n")
        # esta parte es compartida por todos
        cnfFile.write("      }\n")
        cnfFile.write("output {\n")
        cnfFile.write("    stdout { } \n")
        cnfFile.write("    redis {\n")
        cnfFile.write("          host => \""+str(dicDBvariables['redisHost'])+"\" \n")
        cnfFile.write("          data_type => \"list\" \n")
        cnfFile.write("          key => \""+str(dicDBvariables['redisKey'])+"\" \n")
        cnfFile.write("         }\n")
        cnfFile.write("      }\n")
        cnfFile.close()
    nameCnfFile=str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf'
    return nameCnfFile

def createCnfAgentSFilev2(dicDBvariables,clientsList,procID):
    # NEW: 01.09.14: añadimos un fichero nuevo por cliente con los logs creador por el proceso MNR con las alarmas
    # Formato:
    # input {
    # file {
    #	    type => "clientID_1_DS"
    #       path => ["/home/jorge/sar/clientID_1_DataSet.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_1_AL"
    #       path => ["/home/jorge/sar/clientID_1_Alarms.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_2_DS"
    #       path => ["/home/jorge/sar/clientID_2_DataSet.log"]
    #       codec => "json"
    #	  }
    # file {
    #	    type => "clientID_2_AL"
    #       path => ["/home/jorge/sar/clientID_2_Alarms.log"]
    #       codec => "json"
    #	  }
    # }
    # output {
    #    stdout { }
    #    redis {
    #        host => "192.168.56.101"
    #        data_type => "list"
    #        key => "logstash"
    #    } 
    # }
    dicDBvariables['logstashAgentCnf']
    dicDBvariables['commandAgentStart']
    dicDBvariables['redisHost']
    dicDBvariables['redisKey']
    dicDBvariables['finalPath'] # Lo modificaremos y lo usaremos para los ficheros de los agentes
    try:
        cnfFile=open(str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf', 'w')
    except:
        log.error(procID+" <createCnfAgentSFile> Problem to create cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf')
    else:
        log.info(procID+" <createCnfAgentSFile> Creating cnf file -->"+str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf')
        cnfFile.write("input {\n")
        for client in clientsList:
            cnfFile.write("    file {\n")
            cnfFile.write("          type => \""+str(client)+"_DS\" \n")
            cnfFile.write("          path => [\""+str(dicDBvariables['finalPath'])+"/"+str(client)+"_DataSet.log\"] \n")
            cnfFile.write("          codec => \"json\" \n")
            cnfFile.write("         }\n")
        # esta parte es compartida por todos
        cnfFile.write("      }\n")
        cnfFile.write("output {\n")
        cnfFile.write("    stdout { } \n")
        cnfFile.write("    redis {\n")
        cnfFile.write("          host => \""+str(dicDBvariables['redisHost'])+"\" \n")
        cnfFile.write("          data_type => \"list\" \n")
        cnfFile.write("          key => \""+str(dicDBvariables['redisKey'])+"\" \n")
        cnfFile.write("         }\n")
        cnfFile.write("      }\n")
        cnfFile.close()
    nameCnfFile=str(dicDBvariables['logstashAgentCnf'])+'/shipper_allClients.cnf'
    return nameCnfFile


def checkAgentOUTfile(pathF,destFile,procID):
    try:
        os.access(pathF, os.F_OK)
        log.info(procID+ " <checkAgentOUTfile> Path -> "+pathF+" is accessible.")
    except:
        log.error(procID+ " <checkAgentOUTfile> Path -> "+pathF+" is not accessible.")
        return True,0
    else:
        try:
            os.access(pathF+"/"+str(destFile), os.F_OK)
            count=os.system("wc -l "+pathF+"/"+str(destFile)+"|awk '{print$1}")
        except:
            log.error(procID+ " <checkAgentOUTfile> Error to count file -> "+str(destFile))
            return False,0
        else:
            log.info(procID+ " <checkAgentOUTfile> Count file -> "+str(destFile))
            return False,count

def maintenanceLogFile(pathF,destFile,cleanLines,procID):
    try:
        os.access(pathF, os.F_OK)
        log.info(procID+ " <maintenanceLogFile> Path -> "+pathF+" is accessible.")
    except:
        log.error(procID+ " <maintenanceLogFile> Path -> "+pathF+" is not accessible.")
        return True,0
    else:
        try:
            os.system("tail -"+str(cleanLines)+" "+pathF+"/"+destFile+" > /tmp/maintenanceLogFile_$$.tmp")
            os.system("mv /tmp/maintenanceLogFile_$$.tmp "+pathF+"/"+destFile)
        except:
            log.error(procID+ " <maintenanceLogFile> Error to maintenance file -> "+destFile)
            return True
        else:
            log.info(procID+ " <maintenanceLogFile> Maintenance file -> ok")
            return False 

def addNewJSONInFile(pathI,newFile,pathF,destFile,procID):
    # NEW: 01.09.14 - Modificamos el nombre del fichero .log
    try:
        os.access(pathI, os.F_OK)
        log.info(procID+ " <addNewDataSetInFile> Path -> "+pathI+" is accessible.")
    except:
        log.error(procID+ " <addNewDataSetInFile> Path -> "+pathI+" is not accessible.")
        return True,0
    else:
        try:
            os.access(pathF, os.F_OK)
            log.info(procID+ " <addNewDataSetInFile> Path -> "+pathF+" is accessible.")
        except:
            log.error(procID+ " <addNewDataSetInFile> Path -> "+pathF+" is not accessible.")
            return True,0
        else:
            try:
                os.system("cat "+pathI+"/"+newFile+" >> "+pathF+"/"+str(destFile))
            except:
                log.error(procID+ " <addNewDataSetInFile> Error to add lines to file : "+newFile+" --> "+str(destFile))
                return True
            else:
                log.info(procID+ " <addNewDataSetInFile> Added lines to file: "+newFile+" --> "+str(destFile))
                return False 

def createJSONAlarmsANDdatasetv2(datasetID,clientID,listAlarmsDB,pathF,lisInformationDB,analysis,procID):
    # Formato de nuestro JSON
    #  {
    #    * clientID:3,
    #    * datasetID:b37acfbc-cb1c-4b80-9826,
    #    * recordn: 14,
    #    * numRecords: 100,
    #    * featureAlarmed: %idle,
    #    * timestamp:2014-08-23 12:53:59+0200,
    #    * alarmType:[1,2],
    #    * level: LOW,
    #    * externalRectification: True
    #    * col0: asas,
    #    * col1: asas,
    #    * col2: asas,
    #    * analysis: True
    # }
    jsonFile=open(pathF+'/'+str(clientID)+'/'+str(datasetID)+'_Dataset.json', 'w')
    for index in range(len(lisInformationDB)):
        dicDataset={}
        dicDataset['datasetID']=str(datasetID)
        dicDataset['clientID']=clientID
        dicDataset['recordn']=lisInformationDB[index]['recordn']
        dicDataset['featureAlarmed']="none"
        dicDataset['alarmType']="none"
        dicDataset['level']="none"
        dicDataset['externalRectification']=False
        dicDataset['analysis']=analysis
        lisFeatures = []
        lisType = []
        for i in range(len(listAlarmsDB)): # Cuidado, un recordn puede tener mas de una alarma
            if lisInformationDB[index]['recordn'] == listAlarmsDB[i][0]: # significa que la muestra tiene alarma 
                if str(listAlarmsDB[i][1]) not in lisFeatures:
                    lisFeatures.append(str(listAlarmsDB[i][1]))
                for k in range(len(listAlarmsDB[i][2])):
                    if str(listAlarmsDB[i][2][k]) not in lisType:
                        lisType.append(str(listAlarmsDB[i][2][k]))
                if dicDataset['level'] == "none" or dicDataset['level'] == "LOW":
                    dicDataset['level']=str(listAlarmsDB[i][4])
                else:
                    if dicDataset['level'] == "MEDIUM" and (str(listAlarmsDB[i][4]) == "HIGH" or str(listAlarmsDB[i][4]) == "CRITICAL"):
                        dicDataset['level']=str(listAlarmsDB[i][4])
                    elif dicDataset['level'] == "HIGH" and str(listAlarmsDB[i][4]) == "CRITICAL":
                        dicDataset['level']=str(listAlarmsDB[i][4])
                dicDataset['externalRectification']=listAlarmsDB[i][5]
        allElem=""
        for fea in sorted(lisFeatures):
            if allElem == "":
                allElem=fea
            else:
                allElem=str(allElem)+","+str(fea)
        if allElem == "":
            dicDataset['featureAlarmed']="none"
        else:
            dicDataset['featureAlarmed']=str(allElem)
        allElem=""
        for typ in sorted(lisType):
            if allElem == "":
                allElem=typ
            else:
                allElem=str(allElem)+","+str(typ)
        if allElem == "":
            dicDataset['alarmType']="none"
        else:
            dicDataset['alarmType']=str(allElem)        
        date,hour = str(lisInformationDB[index]['times']).split(" ") 
        log.debug(procID+" <createJSONAlarmsANDdataset> times: "+str(date)+" hour: "+str(hour))
        # IN DB   --> 2014-09-01 16:37:45+0200
        # in file --> 2014-08-28T18:01:48.000Z
        dicDataset['timestamp']=str(date)+"T"+str(hour)+".000Z"
        for j in lisInformationDB[index].keys():
            if j != "times" and j != "recordn" and j != "alarm" and j != "numRecords":
                v, t = usefulLibrary.convertString(lisInformationDB[index][j])
                dicDataset[j]=v
        log.debug(dicDataset)
        jsonFile.write(str(json.dumps(dicDataset,ensure_ascii=False,indent=7,sort_keys=True)).replace("\n","")+"\n")
    jsonFile.close()

def createJSONAlarmsANDdataset(datasetID,clientID,listAlarmsDB,pathF,lisInformationDB,analysis,procID):
    # Formato de nuestro JSON
    #  {
    #    * clientID:3,
    #    * datasetID:b37acfbc-cb1c-4b80-9826,
    #    * recordn: 14,
    #    * numRecords: 100,
    #    * featureAlarmed: %idle,
    #    * timestamp:2014-08-23 12:53:59+0200,
    #    * alarmType:[1,2],
    #    * level: LOW,
    #    * externalRectification: True
    #    * col0: asas,
    #    * col1: asas,
    #    * col2: asas,
    #    * analysis: True
    # }
    jsonFile=open(pathF+'/'+str(clientID)+'/'+str(datasetID)+'_Dataset.json', 'w')
    for index in range(len(lisInformationDB)):
        dicDataset={}
        dicDataset['datasetID']=str(datasetID)
        dicDataset['clientID']=clientID
        dicDataset['recordn']=lisInformationDB[index]['recordn']
        dicDataset['featureAlarmed']="none"
        dicDataset['alarmType']="none"
        dicDataset['level']="none"
        dicDataset['externalRectification']=False
        dicDataset['analysis']=analysis
        for i in range(len(listAlarmsDB)): # Cuidado, un recordn puede tener mas de una alarma
            if lisInformationDB[index]['recordn'] == listAlarmsDB[i][0]: # significa que la muestra tiene alarma 
                if dicDataset['featureAlarmed'] == "none": #significa que es la primera característica alarmada encontrada
                    dicDataset['featureAlarmed']=str(listAlarmsDB[i][1])
                else:
                    dicDataset['featureAlarmed']=str(dicDataset['featureAlarmed'])+","+str(listAlarmsDB[i][1])
                kind=""
                for k in range(len(listAlarmsDB[i][2])):
                    if kind == "":
                        kind=str(listAlarmsDB[i][2][k])
                    else:
                        kind=kind+","+str(listAlarmsDB[i][2][k])
                if dicDataset['alarmType'] == "none":
                    dicDataset['alarmType']=kind
                else:
                    if str(kind) not in str(dicDataset['alarmType']):
                        dicDataset['alarmType']=str(dicDataset['alarmType'])+","+kind
                if dicDataset['level'] == "none" or dicDataset['level'] == "LOW":
                    dicDataset['level']=str(listAlarmsDB[i][4])
                else:
                    if dicDataset['level'] == "MEDIUM" and (str(listAlarmsDB[i][4]) == "HIGH" or str(listAlarmsDB[i][4]) == "CRITICAL"):
                        dicDataset['level']=str(listAlarmsDB[i][4])
                    elif dicDataset['level'] == "HIGH" and str(listAlarmsDB[i][4]) == "CRITICAL":
                        dicDataset['level']=str(listAlarmsDB[i][4])
                dicDataset['externalRectification']=listAlarmsDB[i][5]
        date,hour = str(lisInformationDB[index]['times']).split(" ") 
        log.debug(procID+" <createJSONAlarmsANDdataset> times: "+str(date)+" hour: "+str(hour))
        # IN DB   --> 2014-09-01 16:37:45+0200
        # in file --> 2014-08-28T18:01:48.000Z
        dicDataset['timestamp']=str(date)+"T"+str(hour)+".000Z"
        for j in lisInformationDB[index].keys():
            if j != "times" and j != "recordn" and j != "alarm" and j != "numRecords":
                v, t = usefulLibrary.convertString(lisInformationDB[index][j])
                dicDataset[j]=v
        log.debug(dicDataset)
        jsonFile.write(str(json.dumps(dicDataset,ensure_ascii=False,indent=7,sort_keys=True)).replace("\n","")+"\n")
    jsonFile.close()