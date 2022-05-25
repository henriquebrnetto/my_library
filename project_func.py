# -*- coding: utf-8 -*-
"""
Created on Tue May 17 13:16:55 2022

@author: Henrique
"""

import math
import pandas as pd
import numpy as np
import mysql.connector as mysql
import os
import csv
from os import path

def find(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

#Time-Step between Measurements
def diff(data, interval):
	return [data[i] - data[i - interval] for i in range(interval, len(data))]


#Splitting X-axis Train/Test Data (from same dataframe)
def xtrain_test(df, feats = None):
    x = int(0.75*len(df))
    if feats == None:
        train = df.loc[:x-1]
        test = df.loc[x:]
    else:
        train = df.loc[:x, feats]
        test = df.loc[x:, feats]
    return train, test


def ytrain_test(df,col = None):
    y = int(0.75*len(df))
    if col == None:
        train = np.array(df.loc[:y-1])
        train = [float(x) for x in train]
        train = np.array(train).reshape(len(train),1)
        test = np.array(df.loc[y:])
        test = [float(x) for x in test]
        test = np.array(test).reshape(len(test),1)
        
    else:
        train = np.array(df.loc[:y-1, col])
        train = [float(x) for x in train]
        train = np.array(train).reshape(len(train),1) 
        test = np.array(df.loc[y:,col])
        test = [float(x) for x in test]
        test = np.array(test).reshape(len(test),1)
    return train, test

def sql_to_df(cursor, query, *col):
    cursor.execute(query)
    if len(col) == 1:
        return pd.DataFrame([x[0] for x in cursor], columns=col)
    elif len(col) == 0:
        return pd.DataFrame([x for x in cursor])
    else:
        return pd.DataFrame([x for x in cursor], columns=col)
    

def sql_to_csv(cursor, query, **kwargs):
    cursor.execute(query)
    col = kwargs.get('col', []) #def of col kwarg
    filename = kwargs.get('filename', None) # def of filename kwarg
    if filename != None:    #called function has filename argument
        if path.exists(f'{filename}.csv') == True:  #if the file exists
            with  open(f'{filename}.csv', 'a', newline='') as file:
                csv_out = csv.writer(file)
                [csv_out.writerow(line) for line in cursor]
            return
        else:   #if the file does not exist
            try:
                with open(f'{filename}.csv', 'w', newline='') as file:
                    csv_out = csv.writer(file)
                    if len(col) == 0:   #called function with no col argument
                        csv_out.writerow('0')
                        [csv_out.writerow(line) for line in cursor]
                    elif len(col) == 1:
                        csv_out.writerow(col)
                        [csv_out.writerow(line) for line in cursor]
                    else:
                        csv_out.writerow(col)
                        [csv_out.writerow(line) for line in cursor]
                    return
            except ValueError:
                raise TypeError("Column argument must be an iterable object")
                
    else:   #called function does not have filename argument
        try:
            if len(col) == 1:
                df = pd.DataFrame([x[0] for x in cursor], columns=[col])
            elif len(col) == 0:     #called function with no col argument
                df = pd.DataFrame([x for x in cursor])
            else:
                df = pd.DataFrame([x for x in cursor], columns=[col])
            return df
        except ValueError:
            raise TypeError("Column argument must be an iterable object")

def file_reader(*args):
        return [pd.read_csv(f'{x}.csv') for x in args]
    
def csv_append(cursor, col, table, **kwargs):
    filename = kwargs.get('filename', col)
    where = kwargs.get('where', '')
    query = kwargs.get('query', None)
    if filename.endswith('.csv') == True:
        df = pd.read_csv(filename)
    else:
        df = pd.read_csv(f'{filename}.csv')
    size = len(df.iloc[:,0])
    if query == None:
        if where == '':
            quer = f'SELECT {col} FROM {table};'
        else:
            quer = f'SELECT {col} FROM {table} WHERE {where};'
        cursor.execute(quer)
    else:
        cursor.execute(query)
    sql = [x for x in cursor]
    sql = sql[size:]
    if path.exists(f'{filename}.csv') == True:  #if the file exists
        with  open(f'{filename}.csv', 'a', newline='') as file:
            csv_out = csv.writer(file)
            [csv_out.writerow(line) for line in sql]
        return
    else:
        with open(f'{filename}.csv', 'w', newline='') as file:
            csv_out = csv.writer(file)
            df = [csv_out.writerow(line) for line in sql]
        return