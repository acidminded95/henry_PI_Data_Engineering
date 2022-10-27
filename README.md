# Henry_PI: Data Engineering
Este repositorio contiene mi proyecto individual de Data Engineering de la carrera de Data Science de la escuela Henry.

## ¿Qué se esperaba del proyecto?

La consigna del proyecto fue planteada de la siguiente manera por la escuela:

>'En este primer proyecto proponemos realizar un proceso de ETL (extract, transform and load) a partir de un conjunto de datos que se enfocarán en una misma perspectiva de negocio. Los datos vienen de diversas fuentes de relevamiento de precios en distintos mercados. Deberán trabajar los diferentes tipos de archivos para llevarlos a una misma extensión y, una vez finalizada esta etapa, deberán crear los joins necesarios con el objetivo de crear un DER y dejarlos almacenados en un archivo con extensión .db. Por último, todo su trabajo deberá contemplar la carga incremental del archivo "precios_semana_20200518.txt".

>En esta etapa de la academia, verán siempre la palabra "Plus", que hará referencia a los puntos extra disponibles para cada trabajo. Es muy importante que estos sean obviados hasta completar los requerimientos mínimos (deben entregar siempre el producto mínimo viable, MVP, que hace lo mínimo que pedimos), pero siempre recomendamos tratar de completar los plus/adicionales aún después de que el trabajo haya sido entregado, ya que generarán valor agregado a su portfolio'

Dentro de los requerimientos mínimos se listaban: 

- Procesar los diferentes datasets.
- Crear un archivo DB con el motor de SQL que quieran. Pueden usar SQLAlchemy por ejemplo.
- Realizar en draw.io un diagrama de flujo de trabajo del ETL y explicarlo en vivo.
- Realizar una carga incremental de los archivos que se tienen durante el video.
- Realizar una query en el video, para comprobar el funcionamiento de todo su trabajo. La query a armar es la siguiente: Precio promedio de la sucursal 9-1-688.

Además se pedía como 'plus' lo siguiente:

>En este proyecto, será un plus trabajar con herramientas como Docker, Airflow, NiFi u otras herramientas que optimicen y automaticen el proceso de la consigna, programando las cargas incrementales para realizarse con cierta frecuencia. Recuerden que los plus siempre son totalmente opcionales, va a depender de ustedes si lo quieren realizar o no ¡Desafíense a lograrlo!


## Mi acercamiento para resolver el problema:

Desde el comienzo me pareció claro que la problemática de la carga incremental de datos en la base de datos iba a ser una cuestión determinante, puesto que no iba a poder solucionar los problemas específicos que tuviera cada una de las fuentes de los datos de manera directa. Tendría que implementar una solución que tomara automáticamente los archivos que se encontraran en el directorio de 'datasets' del proyecto y procesarlos de manera automática: reconociendo el tipo de tabla al que pertenecería cada archivo y haciéndole las transformaciones necesarias para luego subirlo a la base de datos.

Teniendo presente esto, mi enfoque fue centrarme en el uso de la librería Pandas para limpiar los datos y dejarlos listos para ser cargados en una base de datos sin problemas. Esto es, asegurándome de que todas las claves foráneas (foreign keys) de las tablas de hecho tuvieran un registro al cual hacer referencia en las tablas de dimension (su primary key, único además para cada registro de la tabla dimensión).

Una vez limpiadas las tablas se pudieron recabar los problemas que estas tenían y que no pudieron ser abordados en mi implementación del proceso de ETL, los cuales serán mencionados al final de esta documento. Pese a los problemas encontrados, se logró obtener un producto (sqlite.db) con las especificaciones solicitadas por el cliente (la escuela), sin valores nulos ni registros repetidos.

En el archivo .db se puede encontrar una única tabla de hecho (precios_semanales_2020) en la que se concatenan todos los datos que estaban separados en diferentes archivos inicialmente (claramente separados por la fecha a la que correspondía el dataset del cual fueron extraídos).

Además, allí mismo se encuentran dos tablas de dimensión (producto y sucursal) con claves primarias únicas y no-nulas para todos sus registros.

Cabe señalar, que dicho archivo .db se genera automáticamente al correr el script 'pipeline.py'. No fue posible subirlo al repositorio de GitHub debido a que pesaba más de 100MB.

![flowchart](https://imgur.com/a/ljcV0QD "Logo Title Text 1")

FLOWCHART: https://imgur.com/a/ljcV0QD 


## Resumen del proceso de ETL

### 1) Extracción

• Empecé por escanear el directorio 'datasets' contenido en el mismo directorio del proyecto y donde, se asume para el funcionamiento de todo el programa, deben estar cargados los archivos a procesar. Usando la librería 'os' de Python pude obtener los nombres y extensiones de los archivos que allí se eencontraban.

• Usando los datos obtenidos, se pudo importar los archivos de manera automática usando los métodos '.read_csv()', '.read_excel()' y '.read_json()' de Pandas.

•Una vez importados los DataFrames, estos fueron organizados en diccionarios, distinguiéndolos en 3 tipos de DataFrame según el papel que cada uno jugaría en la base de datos final:
1) Tabla de dimensión.
2) Tabla de hecho (carga inicial)
3) Tabla de hecho (carga incremental)

### 2) Transformación

Usando el archivo 'test.ipynb' se llevó a cabo una exploración de los DataFrames recién importados (usando métodos de pandas tales como '.info()', '.isnull().sum()' y '.duplicated().value_counts()'). Con esto, se lograron encontrar 3 problemas principales en las tablas de hecho (los cuales están debidamente registrados en dicho archivo):

    1) En uno de los DataFrames de precios (2020-05-03), 
	el tipo de data de la columna 'precio' no es *float64* 
	como en los demás.
	2) El formato de la columna 'producto_id' es diferente 
	en varios DataFrames.
	3) Algunos valores de la columna 'sucursal_id', de al 
	menos la tabla '2020-04-19', son interpretados en como 
	de tipo 'datetime'.
	
Para llevar a cabo la transformación se hicieron las siguientes transformaciones a todos los DataFrames:
    
    - Reorganizar las columnas para que en el mismo orden.
    - Agregar la columna 'fecha' obteniendo el dato del archivo del cual procedía cada DataFrame para poder distinguir su origen una vez concatenados.
    - Se borraron los registros duplicados

Así mismo, se realizaron las siguientes transformaciones por columna:
| precio        | producto_id           | sucursal_id  |
| ------------- |:-------------:| -----:|
| Reemplazo de str vacíos por  el valor numpy.nan | Reemplazo de nulos por 0 | Convertir todos los datos a str en el formato adecuado |
| Reemplazo de np.nan por 0 | Convertir valores numéricos a str |   Reemplazo de valores problemáticos* |
|  | Llenado de los str de menos de 13 caracteres con 0's a la izquierda |  |
|  | Reemplazar los valores problemáticos* |  |

\* Se entienden por *valores problemáticos* aquellos id de producto que no hacen referencia a ningún registro de las tablas dimensión.

En cuanto a la transformación de las tablas de dimensión, no fueron muchos los realizdos pero se eliminaron las columnas de 'categoria1', 'categoria2' y 'categoria3' de la tabla 'producto'. También se llenaron los valores faltantes de dicha tabla con valores del tipo 'SIN MARCA', 'SIN DATO', etc. Finalmente se agregó un registro cuyo 'id' es el código usado para remplazar los valores problemáticos de las tablas de hecho, a fin de garantizar que se haga la referencia.

En la tabla de sucursal sólo se llevó a cabo el último paso, agregando 2 registros para que los valores problemáticos de las tablas de hecho (los cuales eran de 2 tipos, como se desglosa en el archivo 'test.ipynb') tuvieran una referencia.

### 3) Carga

Para la carga de los archivos se usó la librería 'SQLite', la cual genera un archivo de extensión '.db' como fue solicitado. Se concatenaron todos los DataFrames de precios y en una sola tabla (precios_semanales_2020) y se subieron todas las tablas a la base de datos.

Como un pequeño extra, se desarrollo una pequeña interfaz para que, al correr el archivo 'pipeline.py' desde la terminal se puedan hacer consultas a la base de datos (algo sencillo pero bastante útil). En esta misma parte del código se pone como ejemplo para hacer un query aquel que fue solicitado por el cliente al definir la tarea: Precio promedio de la sucursal 9-1-688..
## Notas del ingeniero de datos para el correcto funcionamiento del programa:
• Todos los archivos a procesar deben de ser cargados en la carpeta 'datasets' del proyecto.
• Cualquier archivo que se meta a dicho directorio debe tener un nombre con el mismo formato de los archivos facilitados para la elaboración del proyecto. Esto es 'precio(s)_semanas_YYYYMMDD' y estar en alguno de los formatos para los cuales se diseñó el programa (csv, json, txt, xls o xlsx).
• La carga incremental se lleva a cabo de manera automática al ingresar un archivo adecuado a la carpeta 'datasets'.
• Las librerías usadas para la elaboración del script 'pipeline.py' y el módulo 'helpers.py' son:
    - pandas
    - numpy
    - sqlite3
    - warnings
    - datetime
    - os

## Comentarios para el cliente:
• Se encontró que las columnas de 'producto_id' y 'sucursal_id', así como las respectivas columnas de 'id' de las tablas a las que hacen referencia, tienen códigos de diferentes longitudes. Se sugiere definir un formato de código único para los códigos de cada columna a fin de evitar posibles confuciones en las referencias.
• Se encontró también que los valores de la columna 'precio' de la tabla correspondiente a la semana del 26 de abril del 2020 tiene valores corruptos debido a la ausencia de un separador decimal en los úmeros. Esto parece ser un error al momento de registrar los datos en el excel y provoca que los valores tengan de 10 a 100 veces el valor que deberían sin que haya un criterio fácil de implementar para solucionarlo en el tiempo propuesto para la finalización del producto. Se sugiere como posible solución comparar los precios de cada producto de esa tabla con los precios del mismo producto en las demás tbalas y tratar de hacer una corrección basado en la varianza de los datos. Esto claro está, asumiendo que en esa semana no hubo cambios inflacionarios en los precios que jsutificaran una alta varianza.
• El archivo .db resultante fue demasiado pesado para cargarse a github, sin embargo, se genera automáticamente al correr el script.