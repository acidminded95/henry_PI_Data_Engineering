import pandas as pd
import numpy as np
import sqlite3 as db
import warnings
from datetime import datetime

def disable_pandas_warnings():
    warnings.resetwarnings()  # Maybe somebody else is messing with the warnings system?
    warnings.filterwarnings('ignore')  # Ignore everything
    # ignore everything does not work: ignore specific messages, using regex
    warnings.filterwarnings('ignore', '.*A value is trying to be set on a copy of a slice from a DataFrame.*')
    warnings.filterwarnings('ignore', '.*indexing past lexsort depth may impact performance*')



# --------- Funciones helper para el proceso de extracción


def extraccion_tabla_hecho(dataset, path, dict):
    if dataset[1] in ['xlsx', 'xls']:
            print(f'-- Extrayendo archivo excel:\n\t{dataset[0]}.{dataset[1]}\nEsto puede tardar unos segundos..')
            xl_dict = pd.read_excel(path, sheet_name=None, engine='openpyxl')
            for sheet in xl_dict:
                name = f'{sheet[-8:-4]}-{sheet[-4:-2]}-{sheet[-2:]}'
                dict[name] = pd.DataFrame(xl_dict[sheet])
            print('\tArchivo excel extraido!')
    else:
        name = f'{dataset[0][-8:-4]}-{dataset[0][-4:-2]}-{dataset[0][-2:]}'
        if dataset[1] == 'csv':
            print(f'-- Extrayendo archivo csv:\n\t{dataset[0]}.{dataset[1]}')
            dict[name] = pd.read_csv(path, encoding='UTF-16 LE')
            print('\tArchivo csv extraido!')
        elif dataset[1] == 'json':
            print(f'-- Extrayendo archivo json:\n\t{dataset[0]}.{dataset[1]}')
            dict[name] = pd.read_json(path)
            print('\tArchivo json extraido!')
        elif dataset[1] == 'txt':
            print(f'-- Extrayendo archivo txt:\n\t{dataset[0]}.{dataset[1]}')
            dict[name] = pd.read_csv(path, delimiter='|')
            print('\tArchivo txt extraido!')


# --------- Funciones helper para el proceso de transformación

# Definimos una función para obtener sólo la fecha de la carga de datos (sin horas, minutos ni segundos)
def to_date(dtime:datetime):
    return dtime.date()

# Creamos una función para cambiar los valores 'float' e 'int' a 'str'
def pid_num2str(val):
    if type(val) == float:
        return str(int(val))
    elif type(val) == int:
        return str(val)
    else:
        return val

# Creamos una función que nos regrese un valor de 13 caracteres con 0 a la izquierda si el argumento pasado (str) tiene menos de 13
def fill_id(str:str):
    if len(str)<13:
        return str.zfill(13)
    else:
        return str

# Creamos una función que cambie los registros problemáticos de las columnas 'product_id' por el código '1111111111111'
def nxcode2err111(registro:str):
    # Definimos los id's que no hacen referencia a un id en la tabla dimensión 'producto'
    pid_a_remplazar = ['10-1-2303809000003', '10-2-2304238000008', '2630399000008', '2920433200007', '7790513005687', '7798037563669', '7798037563683']
    if registro in pid_a_remplazar:
        return '1111111111111'
    else:
        return registro

# Creamos una función que se pueda aplicar a la columna 'sucursal_id' y nos regrese cada valor como un str en el formato deseado
def sucursal_id_2str(registro):
    if type(registro) == datetime:
        spl = registro.strftime('%d-%m-%Y').split('-')
        if spl[0][0] == '0':
            spl[0] = spl[0][1]
        if spl[1][0] == '0':
            spl[1] = spl[1][1]
        return str(f'{spl[0]}-{spl[1]}-{spl[2]}')
    elif type(registro) == float:
        return '0'
    else:
        return str(registro)

# Definimos la función para llevar a cabo la transformación de valores problemáticos en sucursal_id2
def suc_id_problem_2_zero(registro:str):
    sid_a_remplazar = ['10-1-2029', '9-2-1939', '25-1-2001', '13-1-1939', '13-1-1962',
                       '6-1-2009', '6-2-2021', '10-1-2006', '10-1-2018', '18-1-2005',
                       '5-1-2003', '7-1-1937', '7-1-1948', '10-1-1946', '10-1-2026',
                       '10-1-1933', '10-1-1948', '10-1-1953', '10-1-1954', '10-1-1944',
                       '13-1-1952', '14-1-2009', '6-1-2004', '6-2-2002', '10-1-1955',
                       '9-2-1950', '6-1-2026', '20-1-2001', '17-1-263', '29-1-2007',
                       '17-1-285', '22-1-2017', '3-1-1962', '7-1-1935', '12-1-1999',
                       '29-1-2005', '17-1-101', '12-1-1940', '65-1-315', '20-1-4',
                       '17-1-254', '17-1-46', '17-1-252', '17-1-198', '17-1-178', '17-1-7',
                       '22-1-11', '65-1-317', '19-1-01201', '19-1-02903', '19-1-03235',
                       '19-1-30977', '1-1-12', '17-1-165', '17-1-122', '22-1-23', '1-1-12',
                       '17-1-165', '17-1-122', '22-1-23']
    if registro in sid_a_remplazar:
        return '0'
    else:
        return registro

def transformador(dataset, name):
    cols = ['precio', 'producto_id', 'sucursal_id']
    # Reordenamos las columnas de los DataFrames
    dataset_ok = dataset[cols]
    # Creamos la columna 'fecha' que identifica a qué DataFrame pertenece cada registro para que no se confundan al concatenarse
    dataset_ok['fecha'] = datetime.strptime(name, '%Y-%m-%d')
    dataset_ok['fecha_ok'] = dataset_ok['fecha'].apply(to_date)
    # Borramos los registros duplicados
    dataset_ok.drop_duplicates(inplace=True)
    
    # Transformación de columna 'precio'
    
    # Dado que ningún DataFrame tiene el dato '0' en la columna 'precio', remplazamos los valores faltantes de esta columna con '0'
    dataset_ok['precio_ok'] = dataset_ok['precio'].replace('', np.nan).fillna(0)
    
    # Transformación de columna 'producto_id'

    # Dado que ningún DataFrame tiene el dato '0' en la columna 'producto_id', remplazamos los valores faltantes de esta columna con '0'
    dataset_ok['producto_id2'] = (dataset_ok['producto_id'].replace('', np.nan).fillna(0))
    # Aplicamos la función 'pid_num2str' a la columna sin valores nulos (producto_id2) y guardamos el resultado en otra columna (producto_id3)
    dataset_ok['producto_id3'] = dataset_ok['producto_id2'].apply(pid_num2str)
    # Aplicamos la función 'fill_id' a las columnas 'producto_id3' de los DataFrames de 'precios'
    dataset_ok['producto_id4'] = dataset_ok['producto_id3'].apply(fill_id)
    # Borramos los registros problemáticos de producto_id (7): los que no hacen referencia a un valor de la columna 'id' en la tabla dimensión 'producto'
    dataset_ok['producto_id_ok'] = dataset_ok['producto_id4'].apply(nxcode2err111)

    # Transformación de columna 'sucursal_id'

    # Aplicamos la función 'sucursal_id_2str' a las columnas de 'sucursal_id' y guardamos los valores en una nueva columna 'sucursal_id2'
    dataset_ok['sucursal_id2'] = dataset_ok.sucursal_id.apply(sucursal_id_2str)
    # Transformamos los datos problemáticos de sucursal_id2
    dataset_ok['sucursal_id_ok'] = dataset_ok['sucursal_id2'].apply(suc_id_problem_2_zero)

    return dataset_ok
    
def transformar_producto(dict):
    # Deshechamos las columnas innecesarias del DataFrame 'producto'
    producto_ok = dict['producto'].drop(['categoria1','categoria2','categoria3'], axis=1)

    # Llenamos valores faltantes
    producto_ok.marca.fillna('SIN MARCA', inplace=True)
    producto_ok.nombre.fillna('SIN NOMBRE', inplace=True)
    producto_ok.presentacion.fillna('SIN DATO', inplace=True)

    # Agregamos el registro que representa el id faltante en las tablas de hecho (producto_id='0')
    pid_err_val0 = ['0000000000000', 'SIN MARCA', 'SIN NOMBRE', 'SIN DATO']
    producto_ok.loc[len(producto_ok)] = pid_err_val0
    producto_ok.iloc[-1]
    
    return producto_ok

def transformar_sucursal(dict):
    # Agregamos los registros para que los valores faltantes y errados de la columna 'sucursal_id' tengan una referencia
    sid_err_val0 = ['0',0,0,'SIN DATO','SIN DATO','SIN DATO','SIN DATO','SIN DATO',0,0,'SIN NOMBRE','SIN DATO']
    sid_err_val1 = ['1',0,0,'SIN DATO','SIN DATO','SIN DATO','SIN DATO','SIN DATO',0,0,'SIN NOMBRE','SIN DATO']
    
    sucursal_ok = dict['sucursal'].copy()
    sucursal_ok.loc[len(sucursal_ok)] = sid_err_val0
    sucursal_ok.loc[len(sucursal_ok)] = sid_err_val1

    return sucursal_ok


# --------- Funciones helper para el proceso de carga

def get_dfs_to_concat(carga_inicial:bool,carga_incremental:bool, fact_tables:dict):    
    dfs_to_concat = []
    if carga_inicial:
        if carga_incremental:
            print('Se detecto que no hay un archivo .db creado, pero tambien que hay una carga de archivos diferente a la prevista para la carga inicial.\nSe procedera a cargar todos los registros de la carga en la base de datos.\n')
            for x in fact_tables:
                for df in fact_tables[x]:
                    # Concatenamos todas las tablas de hecho detectadas
                    dfs_to_concat.append(fact_tables[x][df])
        else:
            print('No se ha detectado una base de datos en el directorio.\nComenzando carga inicial de datos..\n')
            for x in fact_tables['inicial']:
                # Concatenamos todos las tablas de hecho detectadas como cargas iniciales
                dfs_to_concat.append(fact_tables['inicial'][x])
    else:
        if carga_incremental:
            print('Se detecto un archivo nuevo entre los datasets. Comenzando carga en base de datos...\n')
            for x in fact_tables['incremental']:
                print(x)
                # Concatenamos todos las tablas de hecho detectadas como cargas incrementales
                dfs_to_concat.append(fact_tables['incremental'][x])
        else:
            print('Se ha detectado una base de datos en el directorio pero no hay cargas incrementales.\nSe procedera a volver a cargar los archivos iniciales (esto sobreescribira las cargas anteriores)\nComenzando carga inicial de datos..\n')
            for x in fact_tables['inicial']:
                # Concatenamos todos las tablas de hecho detectadas como cargas iniciales
                dfs_to_concat.append(fact_tables['inicial'][x])
    return dfs_to_concat

def get_final_fact_table(dfs_to_concat:list):
    ps_2020_ok = pd.concat(dfs_to_concat, axis=0)
    ps_2020_ok.drop(['producto_id','producto_id2','producto_id3','producto_id4','sucursal_id','sucursal_id2','precio','fecha'], axis=1, inplace=True)
    ps_2020_ok.drop_duplicates(inplace=True)
    cols = ['precio_ok', 'producto_id_ok', 'sucursal_id_ok', 'fecha_ok']
    ps_2020_ok = ps_2020_ok[cols]
    ps_2020_ok.rename(columns = {'precio_ok':'precio','producto_id_ok':'producto_id','sucursal_id_ok':'sucursal_id', 'fecha_ok':'fecha'}, inplace = True)
    return ps_2020_ok

def create_connection():
    conn = db.connect("sqlite.db")
    cn = conn.cursor()
    return conn, cn

def create_tables(cn):
    cn.execute("CREATE TABLE IF NOT EXISTS producto (id VARCHAR(24) NOT NULL UNIQUE PRIMARY KEY, marca VARCHAR(24), nombre VARCHAR(24), presentacion VARCHAR(8));")
    cn.execute("CREATE TABLE IF NOT EXISTS sucursal (id VARCHAR(16) NOT NULL UNIQUE PRIMARY KEY, comercio_id SMALLINT, bandera_id SMALLINT, bandera_descripcion VARCHAR(128), comerio_razon_social VARCHAR(128), provincia VARCHAR(32), localidad VARCHAR(32), direccion VARCHAR(128), lat INT, lng INT, sucursal_nombre VARCHAR(48), sucursal_tipo VARCHAR(16));")
    cn.execute("PRAGMA foreign_keys = ON;")
    cn.execute("CREATE TABLE IF NOT EXISTS precio_semanal_2020 (precio FLOAT, producto_id VARCHAR(24), sucursal_id VARCHAR(16), fecha TEXT, FOREIGN KEY(producto_id) REFERENCES producto(id),FOREIGN KEY(sucursal_id) REFERENCES sucursal(id));")

def run_sqlite_cursor(conn, cn):
    print('A continuacion, podras realizar consultas a la base de datos si así lo deseas.\n\nPor ejemplo:')
    query = """
            SELECT   AVG(p.precio),
                     s.id 
            FROM     precio_semanal_2020 p 
            JOIN     sucursal s 
            ON       p.sucursal_id=s.id 
            GROUP BY s.id 
            HAVING   s.id='9-1-688';
        """
    print(query)
    print('Output:\n')
    print(pd.read_sql_query(query, conn))

    opciones = '''
    \n-- Por favor ingresa alguna de las opciones:
        A) Hacer consulta
        B) Commit y cerrar conexión
        C) Cerrar conexión sin commit
    Eleccion: '''
    choice = input(opciones).capitalize()
    querying = True
    while querying:
        if choice == 'A':
            query = input('-- Ingresa la consulta de SQLite que deseas hacer:')
            print('Output:\n')
            try:
                print(pd.read_sql_query(query, conn))
            except:
                print('La consulta no pudo precesarse.')
            choice = input(opciones).capitalize()
        elif choice == 'B':
            conn.commit()
            cn.close()
            conn.close()
            querying = False
        elif choice == 'C':
            cn.close()
            conn.close()
            querying = False
        else:
            print('La opcion seleccionada no es valida.')
            choice = input(opciones).capitalize()
        
        