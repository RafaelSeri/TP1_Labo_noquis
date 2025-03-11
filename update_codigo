#%% Introducción

"""
GRUPO: ÑOQUIS DE LA ABUELA

INTEGRANTES:
    SERI RAFAEL (362/23)
    ANDINA SILVA AUGUSTO (1344/23)
    ARTUFFO TOMÁS (721/23)
    
EN ESTE CÓDGIO ENCONTRARÁN LAS SECCIONES DE PROCESAMIENTO DE DATOS
Y ANÁLISIS DE DATOS.
EN PROCESAMIENTO DE DATOS ESTAREMOS CREANDO Y LIMPIANDO LAS TABLAS DEL
MODELO RELACIONAL EN 3FN.
EN ANÁLISIS DE DATOS ESTAREMOS CREANDO LAS CONSULTAS SQL QUE NOS PIDEN
Y LOS MATERIALES DE VISUALIZACIÓN.

EN RELACION A LA CARGA DE LOS DATASETS:
    LO PENSAMOS EN BASE AL SISTEMA OPERATIVO WINDOWS PORQUE NO SABEMOS
    DESDE QUÉ DISPOSITIVO LO VAN A CORREGIR
    TENER INSTALADO OPENPYXL PARA LA CARGA DE LOS ARCHIVOS .XLSX
"""

#%% FUNCIONES
#codigo_area(x) toma un string y devuelve otro con los numeros que tiene el primero
def codigo_area(x):
    y=''
    for i in x:
        if i in ['0','1','2','3','4','5','6','7','8','9']:
            y+=i
    return y    

#separar_mails(x) toma un string y devuelve una lista con 
def separar_mails(x):
    return x.split()

#%% PROCESAMIENTO DE DATOS

"""
EN ESTA SECCIÓN ESTAREMOS CREANDO LAS TABLAS DEL MODELO RELACIONAL
ESTAS TABLAS SON:
    departamento, poblacion, provincia, centro_cult, mail_centros, usa_mail y est_edu
"""

#%% cargo los datasets y librerías

import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
from matplotlib import ticker
import numpy as np

carpeta='TP01-noquisdelaabuela'

centros=pd.read_csv('TablasOriginales/centros_culturales.csv')
padron_ee=pd.read_excel('TablasOriginales/2022_padron_oficial_establecimientos_educativos.xlsx',skiprows=6)
censo=pd.read_excel('TablasOriginales/padron_poblacion.xlsX')

#%% Creacion de la tabla provincia

pobl_limpio=censo[['Unnamed: 1', 'Unnamed: 2']] #elijo las columnas que quiero
pobl_limpio=pobl_limpio.dropna(how='all') #saco las filas con todos valores NaN
pobl_limpio['id_depto']='' #creo una nueva columna 'id_depto' 
pobl_limpio['depto']='' #creo una nueva columna 'depto'


#le asigno a la columna 'area' un nuevo valor correspdiente al código de área
skip=0
for i,e in pobl_limpio.iterrows():
    if skip>0:
        skip-=1
    if type(e['Unnamed: 1'])==str and 'AREA' in e['Unnamed: 1']:
        y=e['Unnamed: 1']
        z=e['Unnamed: 2']
        skip=2
    if skip==0:
        pobl_limpio.loc[i,'id_depto']=codigo_area(y)
        pobl_limpio.loc[i,'depto']=z

#saco las filas que no deben estar
for i,e in pobl_limpio.iterrows():
    if type(e['Unnamed: 1'])==str and (e['Unnamed: 1']=='Edad' or e['Unnamed: 1']=='Total' 
                                       or 'AREA' in e['Unnamed: 1'] or e['Unnamed: 1']=='RESUMEN'):
        pobl_limpio=pobl_limpio.drop(i,axis=0)


#renombro las columnas
pobl_limpio=pobl_limpio.rename(columns={'Unnamed: 1':'Edad','Unnamed: 2':'Casos'})

#Cambio el tipo de dato de 'id_depto' para que luego coincida con las otras tablas
for i,e in pobl_limpio.iterrows():
    pobl_limpio.loc[i,'id_depto']=int(pobl_limpio.loc[i,'id_depto'])

#elimino filas sobrantes
pobl_limpio=pobl_limpio.iloc[:53949]

#Asigno el nivel educativo a la fila correspondiente y de paso cambio valores
#de otras columnas por temas de consistencia con otras tablas
asign=dd.sql("""
SELECT (CASE WHEN depto LIKE '%Comuna%' THEN 'CABA' ELSE depto END) AS depto,
        (CASE WHEN depto LIKE '%Comuna%' THEN 2000 ELSE id_depto END) AS id_depto,
         Edad, Casos, (CASE WHEN Edad<=4 THEN 'Jardin'
                           WHEN Edad<=12 THEN 'Primaria'
                           WHEN Edad<=17 THEN 'Secundaria' END) AS nivel
FROM pobl_limpio                
                """).df()

#Calculo la población total de cada nivel educativo de cada depto
pobl_nivel_depto=dd.sql("""
SELECT depto, id_depto, nivel, SUM(Casos) AS sum
FROM asign
WHERE nivel IS NOT NULL
GROUP BY nivel, id_depto, depto
ORDER BY id_depto, nivel            
            """).df()

#creo una tabla que tiene por columnas la poblacion de los niveles educativos de cada depto
pobl_grupos=dd.sql("""
SELECT DISTINCT  p1.depto, p1.id_depto, p1.sum AS pobl_jardin,
                pobl_primaria, pobl_secundaria, pobl_total
FROM pobl_nivel_depto AS p1
INNER JOIN (SELECT DISTINCT depto, id_depto, sum AS pobl_primaria
            FROM pobl_nivel_depto
            WHERE nivel='Primaria') AS p2
ON p1.id_depto=p2.id_depto
INNER JOIN (SELECT DISTINCT depto, id_depto, sum AS pobl_secundaria
            FROM pobl_nivel_depto
            WHERE nivel='Secundaria') AS p3
ON p1.id_depto=p3.id_depto
INNER JOIN (SELECT DISTINCT depto, id_depto, SUM(casos) AS pobl_total
            FROM asign
            GROUP BY id_depto, depto) AS p4
ON p1.id_depto=p4.id_depto
WHERE p1.nivel='Jardin'
ORDER BY p1.id_depto
                   """).df()


#calculo y asigno id_prov a cada depto, además corrijo los id_depto de dos deptos de tiera del fuego        
departamento_prov=dd.sql("""
SELECT FLOOR(id_depto/1000) AS id_prov, depto, (CASE WHEN id_depto=94015 THEN 94014
                                                    WHEN id_depto=94008 THEN 94007
                                                    ELSE id_depto END) AS id_depto, pobl_jardin,
        pobl_primaria, pobl_secundaria, pobl_total                   
FROM pobl_grupos
                    """).df()                

#junto las provincias con sus respectivos id_prov
provincia=dd.sql("""
SELECT DISTINCT ID_PROV AS id_prov, (CASE WHEN Provincia LIKE '%Ciudad%' THEN 'CABA'
                          WHEN Provincia LIKE '%Tierra%'THEN 'Tierra del Fuego'
                          ELSE Provincia END) AS nombre_corto
FROM centros                
                """).df()

#%% Creación de la tabla departmanento

#creo la tabla departamento con el nombre de la provincia en vez del id_prov
departamento=dd.sql("""
SELECT p.id_prov, d.depto AS nombre_depto, d.id_depto
FROM departamento_prov AS d
INNER JOIN provincia AS p
ON p.id_prov=d.id_prov                     
                    """).df()

#%% Creación de la tabla poblacion

poblacion_aux=dd.sql("""
SELECT id_depto, pobl_jardin, pobl_primaria, pobl_secundaria, pobl_total
FROM departamento_prov              
                 """).df()

poblacion=dd.sql("""
SELECT p1.id_depto, '0-4' AS rango, p1.pobl_jardin  AS cantidad
FROM poblacion_aux AS p1
UNION 
SELECT p2.id_depto, '05-12' AS rango, p2.pobl_primaria AS cantidad               
FROM poblacion_aux AS p2
UNION 
SELECT p2.id_depto, '13-17' AS rango, p2.pobl_secundaria AS cantidad               
FROM poblacion_aux AS p2
UNION 
SELECT p2.id_depto, 'total' AS rango, p2.pobl_total AS cantidad               
FROM poblacion_aux AS p2
ORDER BY id_depto ASC, rango ASC 
                 """).df()

#%% Creacion de la tabla centro_cult

#me quedo con las columnas que me interesan
cc_uno=centros[['ID_DEPTO','Nombre', 'Mail ','Capacidad']]

#ID_cc será la clave de la tabla
cc_uno['ID_cc']=0 

#asignamos un valor a ID_cc
for i,e in cc_uno.iterrows():
        cc_uno.loc[i,'ID_cc']=i

#cambio el nombre de la columna para que sea más cómodo
cc_uno=cc_uno.rename(columns={'Mail ':'Mail'})
 
#me quedo con las columnas propias del modelo relacional           
centro_cult=cc_uno[['ID_cc','ID_DEPTO','Capacidad']]

#reemplzaco los NaN en 'Capacidad' por 0
centro_cult=centro_cult.fillna(value=0)

#%% creacion de la tabla mail_centros
        
#comienzo creando la tabla mail_cc
mail_cent=cc_uno[['ID_cc','Mail']]

#saco los valores null
mail_cent=mail_cent.dropna()

#creo un df que en donde cada fila tiene un ID_cc y un mail correspondiente 
filas=[]
for i,e in mail_cent.iterrows():
    mails=separar_mails(e['Mail'])
    for j in mails:
        filas+=[{'ID_cc':e['ID_cc'],'Mail':j}]
mail=pd.DataFrame(data=filas)

#mail_aux recupera los ID_cc que tenían '' en Mail
usa_mail_aux=dd.sql("""
SELECT m1.ID_CC, m2.Mail
FROM mail_cent AS m1
LEFT JOIN mail AS m2
ON m1.ID_cc=m2.ID_cc     
ORDER BY m1.ID_cc      
               """).df()
               
#creo la tabla para mail_centros
mail_centros=dd.sql("""
SELECT DISTINCT Mail
FROM usa_mail_aux
WHERE Mail LIKE '%@%'                   
                    """).df()


#%% Creacion de la tabla usa_mail
                    
#Creo la tabla usa_mail
usa_mail=dd.sql("""
SELECT u.*
FROM usa_mail_aux AS u
INNER JOIN mail_centros AS m
ON u.Mail=m.Mail
                """).df()                

#%% Ceracion de la tabla est_edu

#creo un df con las columnas que me interesan
ee_columns=padron_ee[['Cueanexo', 'Código de localidad','Departamento','Común','Nivel inicial - Jardín maternal',
                     'Nivel inicial - Jardín de infantes','Primario','Secundario',
                     'Secundario - INET']]

#convierto en string los valores de Común y de las columnas de niveles educativos
ee_columns[['Común','Nivel inicial - Jardín maternal','Nivel inicial - Jardín de infantes',
            'Primario','Secundario','Secundario - INET']] = ee_columns[['Común','Nivel inicial - Jardín maternal',
                     'Nivel inicial - Jardín de infantes','Primario','Secundario','Secundario - INET']].astype(str)
                                                                        
#calculo los id_depto y me quedo con los ee que tengan modalidad comun                                                                        
ee_comun=dd.sql("""
SELECT FLOOR("Código de localidad"/1000) AS id_depto, Cueanexo, Departamento,
        "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes",
        Primario, Secundario, "Secundario - INET"
FROM ee_columns
WHERE Común='1'               
                """).df()

#creo columnas que me indican qué nivel de educacion tiene cada ee
est_edu_aux=dd.sql("""
SELECT Cueanexo, (CASE WHEN Departamento LIKE '%Comuna%' THEN 2000 ELSE id_depto END) AS id_depto,
                (CASE WHEN "Nivel inicial - Jardín maternal"='1' OR "Nivel inicial - Jardín de infantes"='1' THEN '1' ELSE '0' END) AS jardin,
                (CASE WHEN Primario='1' THEN '2' ELSE '0' END) AS primario, (CASE WHEN Secundario='1' OR "Secundario - INET"='1' THEN '3' ELSE '0' END) AS secundario
FROM ee_comun
               """).df()

#cambio los tipos de dato de las columnas de niveles educativos a int               
est_edu_aux[['jardin','primario','secundario']]=est_edu_aux[['jardin','primario','secundario']].astype(int)

est_edu=est_edu_aux[['Cueanexo','id_depto']]

#%% Creación de la tabla nivel

dicc={'id_nivel':[1,2,3],'desc':['jardin','primaria','secundaria']}

nivel=pd.DataFrame(data=dicc)

#%% Creación de la tabla tiene_nivel

tiene_nivel=dd.sql("""
SELECT Cueanexo, id_nivel
FROM est_edu_aux
INNER JOIN nivel
ON jardin=id_nivel OR primario=id_nivel OR secundario=id_nivel                   
ORDER BY Cueanexo ASC, id_nivel ASC                   
                   """).df()

#%% ANALISIS DE DATOS - CONSULTAS SQL

"""
EN ESTA SECCIÓN ESTAREMOS HACIENDO EL PUNTO DE CONSULTAS SQL
"""                 
                 
#%% Creo el reporte 1

#junto datos de la tabla provincia y departamento
prov_depto=dd.sql("""
SELECT p.nombre_corto, d.id_depto, d.nombre_depto
FROM provincia AS p
INNER JOIN departamento AS d
ON p.id_prov=d.id_prov                  
                  """).df()

#junto datos de la tabla est_edu y tiene_nivel
est_nivel=dd.sql("""
SELECT e.id_depto, e.Cueanexo, t.id_nivel
FROM est_edu AS e
INNER JOIN tiene_nivel AS t
ON e.Cueanexo=t.Cueanexo
                 """).df()                  

#cuento cuántos establecimientos de cada nivel tiene cada depto
cant_nivel_depto=dd.sql("""
SELECT id_depto, id_nivel, COUNT(*) AS cant
FROM est_nivel
GROUP BY id_depto,id_nivel
ORDER BY id_depto ASC, id_nivel ASC                        
                        """).df()

#distingo los niveles como columnas
depto_nivel=dd.sql("""
SELECT id_depto, SUM(CASE WHEN id_nivel = 1 THEN cant ELSE 0 END) AS Jardines,
               SUM(CASE WHEN id_nivel = 2 THEN cant ELSE 0 END) AS Primarias,
               SUM(CASE WHEN id_nivel = 3 THEN cant ELSE 0 END) AS Secundarias
FROM cant_nivel_depto
GROUP BY id_depto
ORDER BY id_depto ASC           
            """).df()

#uno datos de depto_nivel y prov_depto
prov_depto_nivel=dd.sql("""
SELECT p.nombre_corto, p.id_depto, p.nombre_depto, (CASE WHEN d.Jardines IS NULL THEN 0 ELSE d.Jardines END) AS Jardines,
         (CASE WHEN d.Primarias IS NULL THEN 0 ELSE d.Primarias END) AS Primarias,
         (CASE WHEN d.Secundarias IS NULL THEN 0 ELSE d.Secundarias END) AS Secundarias
FROM prov_depto AS p
LEFT JOIN depto_nivel AS d
ON p.id_depto=d.id_depto
ORDER BY p.id_depto ASC
                   """).df()

#distingo los rangos como columnas
poblacion_rango=dd.sql("""
SELECT id_depto, SUM(CASE WHEN rango = '0-4' THEN cantidad ELSE 0 END) AS 'Poblacion Jardin',
               SUM(CASE WHEN rango = '05-12' THEN cantidad ELSE 0 END) AS 'Poblacion Primaria',
               SUM(CASE WHEN rango = '13-17' THEN cantidad ELSE 0 END) AS 'Poblacion Secundaria',
               SUM(CASE WHEN rango = 'total' THEN cantidad ELSE 0 END) AS 'Poblacion Total'
FROM poblacion
GROUP BY id_depto
ORDER BY id_depto ASC                                  
                       """).df()
                    
#creo el primer reporte
reporte_1=dd.sql("""
SELECT p.nombre_corto AS Provincia, p.nombre_depto AS Departamento, p.Jardines,
        r."Poblacion Jardin", p.Primarias, r."Poblacion Primaria", p.Secundarias,
        r."Poblacion Secundaria"
FROM prov_depto_nivel AS p
LEFT JOIN poblacion_rango AS r
ON p.id_depto=r.id_depto
ORDER BY Provincia ASC, Primarias DESC                
                """).df()

      
#%% Creo el reporte 2

#cuento para cada depto los cc con cap>100
cc_100=dd.sql("""
SELECT ID_DEPTO, COUNT(*) AS cant
FROM centro_cult
WHERE Capacidad>100
GROUP BY ID_DEPTO              
              """).df()

#creo el segundo reporte
reporte_2=dd.sql("""
SELECT p.nombre_corto AS Provincia, p.nombre_depto AS Departamento,
        (CASE WHEN c.cant IS NULL THEN 0 ELSE c.cant END) AS "Cantidad de CC con cap>100"
FROM prov_depto AS p
LEFT JOIN cc_100 AS c
ON p.id_depto=c.ID_DEPTO
ORDER BY Provincia ASC, "Cantidad de CC con cap>100" DESC                          
                 """).df()

#%% Creo el reporte 3

#cuento los ee por depto
ee_por_depto=dd.sql("""
SELECT id_depto, COUNT(*) AS Cant_ee
FROM est_edu
GROUP BY id_depto                    
                    """).df()
                    
cc_por_depto=dd.sql("""
SELECT ID_DEPTO, COUNT(*) AS Cant_cc
FROM centro_cult
GROUP BY ID_DEPTO                                        
                    """).df()

#creo el reporte 3
reporte_3=dd.sql("""
SELECT p.nombre_corto AS Provincia, p.nombre_depto AS Departamento, e.Cant_ee,
        c.cant_cc, r."Poblacion Total"
FROM prov_depto AS p
INNER JOIN poblacion_rango AS r
ON p.id_depto=r.id_depto
LEFT JOIN cc_por_depto AS c
ON c.ID_DEPTO=p.id_depto
LEFT JOIN ee_por_depto AS e
ON p.id_depto=e.id_depto
ORDER BY Cant_ee DESC, Cant_cc DESC, Provincia ASC, Departamento ASC                 
                 """).df()
                 
#%% Creo el reprote 4

#a cada centro cultural que tiene mail le asigno su dominio
usa_dominio=dd.sql("""
SELECT u.ID_cc, LOWER(REGEXP_EXTRACT(m.Mail, '@([^\.]+)', 1)) AS dominio
FROM usa_mail AS u
LEFT JOIN mail_centros AS m
ON u.Mail=m.Mail
           """).df()

#a cada departamento le asigno su dominio más usado
depto_dominio=dd.sql("""
SELECT c.ID_DEPTO, MAX(u.dominio) AS dominio
FROM centro_cult AS c
LEFT JOIN usa_dominio AS u
ON c.ID_cc=u.ID_cc 
GROUP BY ID_DEPTO           
           """).df()

#creo el reporte 4
consulta_4=dd.sql("""
SELECT d.nombre_corto AS Provincia, d.nombre_depto AS Departamento, dd.dominio AS "Dominio más frecuente en CC"
FROM prov_depto AS d
LEFT JOIN depto_dominio AS dd
ON dd.ID_DEPTO=d.id_depto
ORDER BY Provincia, Departamento                  
                  """).df()
                  
#%% ANALISI DE DATOS - VISUALIZACION

"""
EN ESTA SECCIÓN ESTAREMOS HACIENDO EL PUNTO DE VISUALIZACION
DE LA SECCIÓN DE ANÁLISIS DE DATOS
"""


#%% i)

#cuántos cc hay por provincia
cc_por_prov=dd.sql("""
SELECT Provincia, SUM(Cant_CC) AS cant_cc
FROM consulta_3
GROUP BY Provincia
ORDER BY cant_cc DESC                 
                   """).df()

fig,ax=plt.subplots()
plt.rcParams['font.family']='sans-serif'
ax.bar(data=cc_por_prov,x='Provincia',height='cant_cc')
ax.set_title('Cantidad de centros culturales por provincia')
ax.set_xlabel('')
ax.set_ylabel('Centros culturales',fontsize='medium')
plt.xticks(rotation=90)
ax.set_yticks([])
ax.bar_label(ax.containers[0],fontsize=8)
plt.show()

#%% ii)

#tendencia(x,y,c) toma dos columnas de un df (x,y) y un color (c)
#y devuelve un gráfico de una línea
def tendencia(x,y,c):
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    plt.plot(x,p(x),color=c,linestyle='--')

fig,ax=plt.subplots()
plt.rcParams['font.family']='sans-serif'
ax.scatter(data=consulta_1, x="Poblacion Jardin", y='Jardines',
           color='red', label='Jardines', s=5)
tendencia(consulta_1["Poblacion Jardin"],consulta_1['Jardines'],'red')
ax.scatter(data=consulta_1, x="Poblacion Primaria", y='Primarias',
           color='skyblue', label='Primarias', s=5)
tendencia(consulta_1["Poblacion Primaria"],consulta_1['Primarias'], 'skyblue')
ax.scatter(data=consulta_1, x="Poblacion Secundaria",
           y='Secundarias', color='orange', label='Secundarias', s=5)
tendencia(consulta_1["Poblacion Secundaria"],consulta_1['Secundarias'], 'orange')
plt.legend()
ax.set_xlabel('Poblacion',fontsize='medium')
ax.set_ylabel('Establecimientos educativos', fontsize='medium')
ax.set_title('Cantidad de establecimientos educativos por departamento\n en función de su población')
ax.set_xlim(0,100000)
plt.show()

#%% iii)

fig,ax=plt.subplots()
consulta_3.boxplot(by=['Provincia'], column=['Cant_EE'], ax=ax, grid=False, showmeans=True)
fig.suptitle('')
ax.set_title('Cantidad de establecimientos educativos\n de cada departamento por provincia')
ax.set_xlabel('')
ax.set_ylabel('Establecimientos educativos')
plt.xticks(rotation=90)
ax.set_ylim(0,450)
plt.show()

#%% iv)

#calculo para cada depto la cantidad de cc cada mil habitantes
#y la cantidad de ee cada mil habitantes
cc_ee_1000=dd.sql("""
SELECT Departamento, ((1000*"Cant_EE")/"Población total") AS ee,
        ((1000*"Cant_CC")/"Población total") AS cc
FROM consulta_3
                  """).df()

fig,ax=plt.subplots()
plt.rcParams['font.family']='sans-serif'
ax.scatter(data=cc_ee_1000, x='ee',y='cc',s=5)
ax.set_xlabel('Establecimientos educativos cada mil habitantes', fontsize='medium')
ax.set_ylabel('Centros culturales cada mil habitantes', fontsize='medium')
ax.set_title('Cantidad de centros culturales cada mil habitantes\n en función de establecimientos educativos cada mil habitantes')
plt.show()

