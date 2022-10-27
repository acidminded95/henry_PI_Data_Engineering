# Empezamos importando las librerías que usaremos para la preparación de los datos
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sqlite3 as db
from datetime import datetime
from os import listdir
from random import sample
from helpers import *

# -------------------- E X T R A C C I Ó N -------------------- #

archivos_iniciales = ['precios_semana_20200413.csv', 'precios_semanas_20200419_20200426.xlsx', 'precios_semana_20200518.txt', 'precios_semana_20200503.json', 'sucursal.csv', 'producto.parquet']
archivos_incrementales = []

# Obtenemos una lista de los archivos dentro del directorio 'datasets'
try:
    file_names = listdir('.\datasets')
    os = "win"
except FileNotFoundError:
    file_names = listdir('./datasets')
    os = "unix"

# Creamos un diccionario con los nombres de cada archivo y su extensión para los datasets iniciales y uno para los incrementales
datasets_extensions = {}
datasets_incrementales_ext = {}
for x in file_names:
    if x not in archivos_iniciales:
        archivos_incrementales.append(x)
        datasets_incrementales_ext = x.split('.')
    else:
        datasets_extensions[x] = x.split('.')

# Definimos el path a usar para importar los archivos dependiendo del sistema operativo
if os == "win":
    path = f'datasets/{x}'
elif os == "unix":
    path = f'datasets/{x}'

# Creamos los diccionarios para los datasets 
# 1)Tabla-de-hecho-inicial (ps_2020)
# 2)Tabla de hecho incremental (ps_2020_i) y 
# 3)dimensión automatizamos la importación de los archivos contenidos en el directorio 'datasets'

ps_2020 = {}
ps_2020_i = {}
dims = {}
for x in datasets_extensions:
    if x[:7] == 'precios':
        extraccion_tabla_hecho(datasets_extensions[x], path, ps_2020)
        '''
        if datasets_extensions[x][1] in ['xlsx', 'xls']:
            xl_dict = pd.read_excel(path, sheet_name=None, date_parser=None)
            for sheet in xl_dict:
                name = f'{sheet[-8:-4]}-{sheet[-4:-2]}-{sheet[-2:]}'
                ps_2020[name] = pd.DataFrame(xl_dict[sheet])
        else:
            name = f'{datasets_extensions[x][0][-8:-4]}-{datasets_extensions[x][0][-4:-2]}-{datasets_extensions[x][0][-2:]}'
            if datasets_extensions[x][1] == 'csv':
                ps_2020[name] = pd.read_csv(path, encoding='UTF-16 LE')
            elif datasets_extensions[x][1] == 'json':
                ps_2020[name] = pd.read_json(path)
            elif datasets_extensions[x][1] == 'txt':
                ps_2020[name] = pd.read_csv(path, delimiter='|')
        '''
    else:
        name = datasets_extensions[x][0]
        if datasets_extensions[x][1] == 'csv':
            dims[name] = pd.read_csv(path)
        elif datasets_extensions[x][1] == 'parquet':
            dims[name] = pd.read_parquet(path)

for x in datasets_incrementales_ext:
    extraccion_tabla_hecho(datasets_incrementales_ext[x], path, ps_2020_i)
    '''
    if x[:7] == 'precios':
        if datasets_incrementales_ext[x][1] in ['xlsx', 'xls']:
            xl_dict = pd.read_excel(path, sheet_name=None, date_parser=None)
            for sheet in xl_dict:
                name = f'{sheet[-8:-4]}-{sheet[-4:-2]}-{sheet[-2:]}'
                ps_2020_i[name] = pd.DataFrame(xl_dict[sheet])
        else:
            name = f'{datasets_incrementales_ext[x][0][-8:-4]}-{datasets_incrementales_ext[x][0][-4:-2]}-{datasets_incrementales_ext[x][0][-2:]}'
            if datasets_incrementales_ext[x][1] == 'csv':
                ps_2020_i[name] = pd.read_csv(path, encoding='UTF-16 LE')
            elif datasets_extensions[x][1] == 'json':
                ps_2020[name] = pd.read_json(path)
            elif datasets_extensions[x][1] == 'txt':
                ps_2020[name] = pd.read_csv(path, delimiter='|')
    '''
for x in ps_2020:
    print(x)