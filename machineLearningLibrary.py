#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sklearn.cross_validation import train_test_split
from sklearn import linear_model
from sklearn import mixture
from sklearn import metrics

import logging
import sys, traceback, os
import uuid
import psutil
import getpass

import usefulLibraryFiles         # Libreria propia del APP
import usefulLibrary             # Libreria propia del APP
import numpy as np


from datetime import datetime

log = logging.getLogger()
log.setLevel('INFO')

def applyRegression(dicWrongValues,L2,dicL,L_train,coefVaration,dicAlarmsRegr,score,testSize,analysis,procID):
    # Esta función tiene como objetivo:
    #   1º. Sustituir los "wrong" values de la lista L2 por valores predecidos según el resto de características.
    #       NOTA: cuidado cuando existan mas de una característica "wrong" para la misma muestra.
    #   2º. Validar uno a uno cada valor de la lista L2, es decir, comprobar que el valor real es el "mismo" que obtenemos al predecirlo. En
    #       el caso de que no sea (sea lo suficientemente diferente) generará una alarma.
    #
    #  Lo idóneo es unir el array sacado de la BD con el del data set a analizar (quitando las muestras cuyas coordenadas tienen valore raro),
    #  y ya con esta lista dividir la en train y test, esto es válido solo para el PASO 1 
    #  NOTA: se podría definir una variable "regressionMinScore" para sólo aplicar la regresión en caso de que el score sea mayor a este valor.
    features=[]
    for col in sorted(dicL.keys()):
        features.append(dicL[col])
    feature_names = np.array(features)
    log.info(procID+" <applyRegression> Features: "+str(feature_names))
    if (len(dicWrongValues.keys())>0):
        L_full = usefulLibrary.unionListsWrong(dicWrongValues,L2,L_train,procID) # L_full contiene un array de la unión del array de 
        # entrenamiento mas el del fichero sin los valores a 0 que tenían strings
        log.info(procID+" <applyRegression> Num columns L_full: "+str(len(L_full[0])))
        # Toca recorrer el array: PASO 1 --> en busca de los "0" que hemos puesto en los valores malos
        #   - la forma de recorrer esta vez no será registro a registro, lo haremos por columna ya que a la hora de aplicar el algoritmo  
        # Sólo podremos aplicar la regresión lineal si se cumple que el tamaño del array L_full es:
        #       1. Mayor que el valor definido por la variable test_size
        #       2. Si es mayor lo divideremos hasta tener un array con el valor justo de "test_size", cogiendo muestras aleatorias
        percentSizeTrain=(len(L_full)*100)/(len(L_full)+len(dicWrongValues.keys()))
        if  int(percentSizeTrain) >= int(testSize):
            log.info(procID+" <applyRegression> STEP 1: Train array is upper to test_size "+str(testSize)+", the Lineal Regression will be executed.")
            analysis=True
            values_X, values_Y = [], []
            columns=[]
            for wrong in sorted(dicWrongValues.keys()):
                log.info(procID+" <applyRegression> WrongDict key= "+str(wrong))
                if dicWrongValues[wrong]["y"] not in columns:
                    columns.append(dicWrongValues[wrong]["y"])
            log.info(procID+" <applyRegression> Num columns of wrong values= "+str(len(columns)))
            for col in columns:
                log.info(procID+" <applyRegression> Col= "+str(col))
                values_Y, values_X = usefulLibrary.extractColumnArray(L_full,col,procID)
                log.info(procID+" <applyRegression> Num rows values_Y: "+str(len(values_Y)))
                log.info(procID+" <applyRegression> "+str(values_Y))
                log.info(procID+" <applyRegression> Num columns values_X: "+str(len(values_X[0])))
                values_X = np.array(values_X)
                values_Y = np.array(values_Y)
                # Antes de dividir tenemos que calcular el % del tamaño del test_size
                #
                perCsplit=(percentSizeTrain-int(testSize))/100 # EJ: 91% - 80% = 11% / 100 --> 0.11 del array a usar como test y 0.89 para train
                # haciendo justo el 80% (definido por la variable)
                log.info(procID+" <applyRegression> test_size= "+str(perCsplit))
                X_train, X_test, y_train, y_test =train_test_split(values_X, values_Y, test_size=perCsplit,random_state=33)
                log.debug(procID+" <applyRegression> X_train size: before= "+str(len(values_X))+" | now= "+str(len(X_train)))    
                # Create linear regression object
                regr = linear_model.LinearRegression()
                # Train the model using the training sets
                regr.fit(X_train, y_train)
                # Explained variance score: 1 is perfect prediction
                log.info(procID+" <applyRegression> Variance score: "+str(regr.score(X_test,y_test)))
                # Ahora tocaría estimar y sustituir
                for reg in dicWrongValues.keys():
                    x=dicWrongValues[reg]["x"]
                    y=dicWrongValues[reg]["y"]
                    if L2[x][y] == 0 and y == col:  # nosotros le pusimos este valor, así nos aseguramosxº
                        y_pred=regr.predict(usefulLibrary.extractColumnList(L2[x],y,procID))
                        log.info(procID+" <applyRegression> Value predict for wrong value in coordinates "+str(x)+","+str(y)+": "+str(y_pred))
                        aprox=round(y_pred,4)
                        log.info(procID+" <applyRegression> Aproximate predict value: "+str(aprox))
                        # Ahora deberíamos sustituirlo en la Lista definitiva
                        L2[x][y]=aprox
        else:
            log.info(procID+" <applyRegression> STEP1: Train array is lower to test_size "+str(testSize)+", the Lineal Regression will not be executed.")
    # Para el PASO 2 no podemos unir la lista sacada de la BD con la del fichero ya que vamos a ir prediciendo cada valore del array del
    # fichero y no podemos usar los valores que ya vienen en el.
    log.info(procID+" <applyRegression> Num columns L_train: "+str(len(L_train[0])))
    percentSizeTrain=(len(L_train)*100)/(len(L_train)+len(L2))
    if  int(percentSizeTrain) >= int(testSize):
        log.info(procID+" <applyRegression> STEP 2: Train array is upper to test_size "+str(testSize)+", the Lineal Regression will be executed.")
        analysis=True
        values_X, values_Y = [], []
        # Nos toca recorre todo el array del fichero prediciendo uno a uno cada valor, por columna
        for colum in range(len(feature_names)):
            log.info(procID+" <applyRegression> Predict values of Colum= "+str(colum))
            values_Y, values_X = usefulLibrary.extractColumnArray(L_train,colum,procID)
            values_X = np.array(values_X)
            values_Y = np.array(values_Y)
            # Antes de dividir tenemos que calcular el % del tamaño del test_size
            #
            perCsplit=(percentSizeTrain-int(testSize))/100 # EJ: 91% - 80% = 11% / 100 --> 0.11 del array a usar como test y 0.89 para train
            # haciendo justo el 80% (definido por la variable)
            log.info(procID+" <applyRegression> test_size= "+str(perCsplit))
            X_train, X_test, y_train, y_test =train_test_split(values_X, values_Y, test_size=perCsplit,random_state=33)
            log.debug(procID+" <applyRegression> X_train size: before= "+str(len(values_X))+" | now= "+str(len(X_train)))
            # Create linear regression object
            regr = linear_model.LinearRegression()
            # Train the model using the training sets
            regr.fit(X_train, y_train)
            # Explained variance score: 1 is perfect prediction
            score=regr.score(X_test,y_test)
            log.info(procID+" - Variance score: "+str(score))
            # Una vez ya tenemos el estimador entrenado comenzamos a predecir
            for row in range(len(L2)):
                subDalarm={}
                newL = usefulLibrary.extractColumnList(L2[row],colum,procID)
                log.info(procID+" <applyRegression> List of features to predict: "+str(newL))
                y_pred=regr.predict(newL)
                log.info(procID+" <applyRegression> Value predict for coordinates row,colum "+str(row)+","+str(colum)+" -> REAL: "+str(L2[row][colum])+" PRED: "+str(y_pred))
                aprox=round(y_pred,4)
                log.info(procID+" <applyRegression> Aproximate predict value: "+str(aprox))
                coefV = usefulLibrary.desviacionTipica(L2[row][colum],aprox,procID)
                if coefV > int(coefVaration):
                    # Como el coeficiente de variación es mayor a lo permitido por variable generamos la alarma como posible anomalía
                    subDalarm["x"]=row
                    subDalarm["y"]=colum
                    dicAlarmsRegr[len(dicAlarmsRegr.keys())]=subDalarm
                    log.info(procID+" <applyRegression> Alarm generated...[coefV= "+str(coefV)+"]")
                else:
                    # Como el coeficiente de variación es menor a lo permitido por variable, consideramos que tanto el valor real como el predecido
                    # son semejantes por lo que no generaremos alarma
                    log.info(procID+" <applyRegression> Element with value between interval...[coefV= "+str(coefV)+"]")
    else:
        log.info(procID+" <applyRegression> STEP2: Train array is lower to test_size "+str(testSize)+", the Lineal Regression will not be executed.")
    # Una vez recorrido el array y obtenidas todas las posibles alarmas, hemos terminado.

def applyClusteringTotal(L_predict,features,L_train,dicDBvariables,dicAlarmsClus,score,procID):
    # EL objetivo de este procedimiento es aplicar un número de veces (según la variable proofGroup) el algoritmo de clustering 
    # Gaussian Mixture Models (GMM) aumentanto en cada una de las iteraciones el número de grupos a obtener (2^x). En cada iteración se obtendrá
    # el grupo al que pertence cada una de las muestras del array a predecir (L_predict)
    # Se aplicará por muestra (row)
    log.info(procID+" <applyClusteringTotal> Features: "+str(features))
    percentSizeTrain=(len(L_train)*100)/(len(L_train)+len(L_predict))
    # Antes de dividir tenemos que calcular el % del tamaño del test_size
    #
    perCsplit=(percentSizeTrain-int(dicDBvariables["test_size"]))/100 # EJ: 91% - 80% = 11% / 100 --> 0.11 del array a usar como test y 0.89 para train
    # haciendo justo el 80% (definido por la variable)
    log.debug(procID+" <applyClusteringTotal> test_size= "+str(perCsplit))
    X_train, X_test, y_train, y_test =train_test_split(L_train, L_train, test_size=perCsplit,random_state=33)
    log.debug(procID+" <applyClusteringTotal> X_train size: before= "+str(len(L_train))+" | now= "+str(len(X_train)))    
    nComp=2
    dicResultClusSample={}
    dicResultClusGroup={}
    for proof in range(int(dicDBvariables['proofGroup'])): 
        log.info(procID+" <applyClusteringTotal> Proof level:"+str(proof)+" - n_components: "+str(nComp))   
        gm = mixture.GMM(n_components=nComp,covariance_type='tied', random_state=42)
        gm.fit(X_train)
        y_pred = gm.predict(L_predict) 
        usefulLibrary.saveResult(y_pred,dicResultClusSample,dicResultClusGroup,'I'+str(proof),procID)
        nComp=nComp*2
    log.debug(dicResultClusSample)
    log.debug(dicResultClusGroup)
    usefulLibrary.applyClusteringAlarm(dicResultClusSample,dicResultClusGroup,dicAlarmsClus,dicDBvariables['clustGroup'],procID)
    for alarm in sorted(dicAlarmsClus.keys()):
        log.info(procID+" <applyClusteringTotal> Row:"+str(L_predict[alarm])+" - level: "+str(dicAlarmsClus[alarm])) 

def applyClusteringPartial(L_predict,features,L_train,dicDBvariables,dicAlarmsClusTotal,score,procID):
    # EL objetivo de este procedimiento es aplicar un número de veces (según la variable proofGroup) el algoritmo de clustering 
    # Gaussian Mixture Models (GMM) aumentanto en cada una de las iteraciones el número de grupos a obtener (2^x). En cada iteración se obtendrá
    # el grupo al que pertence cada una de las muestras del array a predecir (L_predict)
    # Se aplicará por columna (column)
    log.info(procID+" <applyClusteringPartial> Features: "+str(features))
    percentSizeTrain=(len(L_train)*100)/(len(L_train)+len(L_predict))
    # Antes de dividir tenemos que calcular el % del tamaño del test_size
    #
    perCsplit=(percentSizeTrain-int(dicDBvariables["test_size"]))/100 # EJ: 91% - 80% = 11% / 100 --> 0.11 del array a usar como test y 0.89 para train
    # haciendo justo el 80% (definido por la variable)
    log.debug(procID+" <applyClusteringPartial> test_size= "+str(perCsplit))
    X_train, X_test, y_train, y_test =train_test_split(L_train, L_train, test_size=perCsplit,random_state=33)
    log.debug(procID+" <applyClusteringPartial> X_train size: before= "+str(len(L_train))+" | now= "+str(len(X_train)))        
    for col in range(len(features)):
        dicAlarmsClus={}
        nComp=2
        dicResultClusSample={}
        dicResultClusGroup={}
        L_1colTR, L_restColTR = usefulLibrary.extractColumnArray(L_train,col,procID)
        L_1colPR, L_restColPR = usefulLibrary.extractColumnArray(L_predict,col,procID)
        for proof in range(int(dicDBvariables['proofGroup'])): 
            log.info(procID+" <applyClusteringPartial> Proof level:"+str(proof)+" - n_components: "+str(nComp)+" - COLUMN= "+str(col))   
            gm = mixture.GMM(n_components=nComp,covariance_type='tied', random_state=42)
            gm.fit(L_1colTR)
            y_pred = gm.predict(L_1colPR) 
            usefulLibrary.saveResult(y_pred,dicResultClusSample,dicResultClusGroup,'I'+str(proof),procID)
            nComp=nComp*2
        log.debug(dicResultClusSample)
        log.debug(dicResultClusGroup)
        usefulLibrary.applyClusteringAlarm(dicResultClusSample,dicResultClusGroup,dicAlarmsClus,dicDBvariables['clustGroup'],procID)
        dicAlarmsClusTotal[col]=dicAlarmsClus
        for alarm in sorted(dicAlarmsClus.keys()):
            log.info(procID+" <applyClusteringPartial> COLUMN= "+str(col)+" Value:"+str(L_predict[alarm][col])+" - level: "+str(dicAlarmsClus[alarm])) 