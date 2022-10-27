# Empezamos importando las librerías que usaremos para la preparación de los datos
from os import listdir
from helpers import *

disable_pandas_warnings()

if __name__ == '__main__':


    # -------------------- E X T R A C C I Ó N -------------------- #


    # El propósito de esta sección es importar los datasets de diferentes formatos 
    # contenidos en el directorio 'datasets' a DataFrames de pandas, de manera 
    # automatizada, organizándolos en diccionarios para facilitar su acceso y limpieza.

    print('\n\n----------Inicializando extraccion de datos----------\n')

    archivos_iniciales = ['precios_semana_20200413.csv',
                        'precios_semanas_20200419_20200426.xlsx',
                        'precios_semana_20200518.txt',
                        'precios_semana_20200503.json',
                        'sucursal.csv',
                        'producto.parquet']
    archivos_incrementales = []

    # Obtenemos una lista de los archivos dentro del directorio 'datasets', hay dos paths posible dependiendo de si el script se corre desde windows o unix
    try:
        file_names = listdir('.\datasets')
    except FileNotFoundError:
        file_names = listdir('./datasets')

    # Creamos un diccionario con los nombres de cada archivo y su extensión para los datasets iniciales y uno para los incrementales
    datasets_extensions = {}
    datasets_incrementales_ext = {}
    for x in file_names:
        if x in archivos_iniciales:
            datasets_extensions[x] = x.split('.')
        else:
            archivos_incrementales.append(x)
            datasets_incrementales_ext[x] = x.split('.')

    # Se determina si hay o no carga incremental (este valor será útil al momento de la carga)
    if len(archivos_incrementales)>0:
        carga_incremental = True
        print('- Carga incremental detectada\n\n')
    else:
        carga_incremental = False
        print('- No se detecta carga incremental. Se llevara a cabo la carga inicial..\n\n')

    # Creamos los diccionarios para los datasets y los poblamos:
    # 1) Tabla de hecho inicial (ps_2020)
    # 2) Tabla de hecho incremental (ps_2020_i)
    # 3) Dimensión 
    # Automatizamos la importación de los archivos contenidos en el directorio 'datasets'

    ps_2020 = {}
    ps_2020_i = {}
    dims = {}
    for x in datasets_extensions:
        # Definimos el path a usar para importar los archivos dependiendo del sistema operativo
        path = f'datasets/{x}'
        if x[:7] == 'precios':
            extraccion_tabla_hecho(datasets_extensions[x], path, ps_2020)
        else:
            name = datasets_extensions[x][0]
            if datasets_extensions[x][1] == 'csv':
                dims[name] = pd.read_csv(path)
            elif datasets_extensions[x][1] == 'parquet':
                dims[name] = pd.read_parquet(path)

    for x in datasets_incrementales_ext:
        path = f'datasets/{x}'
        extraccion_tabla_hecho(datasets_incrementales_ext[x], path, ps_2020_i)


    # -------------------- T R A N S F O R M A C I Ó N -------------------- #


    # El propósito de esta sección es dejar los DataFrames listos para su carga en una
    # base de datos SQL. Esto se logrará cumpliendo con 2 tareas:
    #   1) Dejar todas las columnas de las tablas de 'precios' con el mismo tipo de dato
    #   y mismo formato de registros en las diferentes tablas para poder concatenarlas 
    #   después y subirlas a una base de datos SQL.
    #   2) Asegurarnos de que todos los valores de las columnas 'producto_id' y 'sucursal_id'
    #   puedan relacionarse a algún valor de las columnas 'id' en las dimensiones 'producto' 
    #   y 'sucursal'.

    print('\n\n----------Inicializando transformacion de datos----------\n\n')

    # Creamos un diccionario para facilitar el acceso a las tablas de hecho
    fact_tables = {'inicial': ps_2020, 'incremental': ps_2020_i}

    print('Transformando DataFrames de registros de precios...')
    for x in fact_tables:
        for df in fact_tables[x]:
            # Aplicamos las transformaciones necesarias a los DataFrames que pertenecerán a la tabla-de-hecho en la base de datos
            fact_tables[x][df] = transformador(fact_tables[x][df], df)
    print('DataFrames de registros de precios transformados!\n')

    # Realizamos las transformaciones necesarias a las tablas de dimensiones
    print('Transformando DataFrames de tablas de dimension...')
    dims['producto_ok'] = transformar_producto(dims)
    dims['sucursal_ok'] = transformar_sucursal(dims)
    print('DataFrames de tablas de dimension transformadas!\n')

    print('Transformacion de datos completada!')


    # ------------------- C A R G A   D E   D A T O S ------------------- #


    print('\n\n----------Inicializando carga de datos----------')

    # Determinamos los DataFrames a concatenar (en una sola tabla de hecho) para subir 
    # en la base de datos de acuerdo a:
    # 1) si hay o no un archivo .db en el directorio
    # 2) si se detecta una carga incremental en los archivos extraidos

    # Buscamos el archivo sqlite.db en el directorio
    cur_dir = listdir('.')
    if 'sqlite.db' in cur_dir:
        carga_inicial = False
    else:
        carga_inicial = True

    # Usamos la función 'get_dfs_to_concat' para obtener los DataFrames a concatenar
    dfs_to_concat = get_dfs_to_concat(carga_inicial,carga_incremental, fact_tables)

    # Usamos la función 'get_final_fact_table' para concatenar todos los DataFrames de 'precios'
    ps_2020_ok = get_final_fact_table(dfs_to_concat)


    # Guardamos los DataFrames a exportar en un diccionario
    df_2_export = {'precio_semanal_2020':ps_2020_ok, 'producto': dims['producto_ok'], 'sucursal':dims['sucursal']}

    # Creamos la conexión con SQLite
    conn, cn = create_connection()

    # Creamos las tablas, si es que no existen ya
    create_tables(cn)

    # Poblamos las tablas con los datos
    for x in df_2_export:
        df_2_export[x].to_sql(x, conn, if_exists="replace", index=False)

    separator = '-------------------'
    print(f'{separator} CARGA DE DATOS FINALIZADA {separator}')

    run_sqlite_cursor(conn, cn)