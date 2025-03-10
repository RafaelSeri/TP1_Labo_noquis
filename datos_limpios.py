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

"""

#%% PROCESAMIENTO DE DATOS

"""
EN ESTA SECCIÓN ESTAREMOS CREANDO LAS TABLAS DEL MODELO RELACIONAL
ESTAS TABLAS SON:
    departamento, poblacion, centro_cult, mail_centros, usa_mail y est_edu
"""

#%% cargo los datasets

import pandas as pd
import duckdb as dd

centros=pd.read_csv('centros_culturales.csv')
padron_ee=pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx',skiprows=6)
censo=pd.read_excel('padron_poblacion.xlsX')

#%% Creacion de la tabla departamento

pobl_limpio=censo[['Unnamed: 1', 'Unnamed: 2']] #elijo las columnas que quiero
pobl_limpio=pobl_limpio.dropna(how='all') #saco las filas con todos valores NaN
pobl_limpio['id_depto']='' #creo una nueva columna 'id_depto' 
pobl_limpio['depto']='' #creo una nueva columna 'depto'

#codigo_area(x) toma un string y devuelve otro con los numeros que tiene el primero
def codigo_area(x):
    y=''
    for i in x:
        if i in ['0','1','2','3','4','5','6','7','8','9']:
            y+=i
    return y    

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
id_prov=dd.sql("""
SELECT DISTINCT ID_PROV, (CASE WHEN Provincia LIKE '%Ciudad%' THEN 'CABA'
                          WHEN Provincia LIKE '%Tierra%'THEN 'Tierra del Fuego'
                          ELSE Provincia END) AS prov
FROM centros                
                """).df()

#creo la tabla departamento con el nombre de la provincia en vez del id_prov
departamento=dd.sql("""
SELECT p.prov, d.depto, d.id_depto, d.pobl_jardin, d.pobl_primaria,
        d.pobl_secundaria
FROM departamento_prov AS d
INNER JOIN id_prov AS p
ON p.ID_PROV=d.id_prov                     
                    """).df()

#%% Creación de la tabla poblacion

poblacion=dd.sql("""
SELECT pobl_jardin, pobl_primaria, pobl_secundaria, pobl_total
FROM departamento_prov              
                 """).df()


#%% Creacion de la tabla provincia

provincia=dd.sql("""
SELECT prov
FROM id_prov                 
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

#separar_mails(x) toma un string y devuelve una lista con 
def separar_mails(x):
    return x.split()
        
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

#solo_dominio(x) crea un string y devuelve la parte entre el '@' y el primer '.'
def solo_dominio(x):
    return x.split('@')[1].split('.')[0].lower()

#creo la columna 'dominio'
mail_centros['dominio']=''

#a cada mail le asigno su dominio
for i,e in mail_centros.iterrows():
    mail_centros.loc[i,'dominio']=solo_dominio(e['Mail'])


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
SELECT FLOOR("Código de localidad"/1000) AS id_depto, Cueanexo AS id_ee, Departamento,
        "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes",
        Primario, Secundario, "Secundario - INET"
FROM ee_columns
WHERE Común='1'               
                """).df()

#creo columnas que me indican qué nivel de educacion tiene cada ee
est_edu=dd.sql("""
SELECT id_ee, (CASE WHEN Departamento LIKE '%Comuna%' THEN 2000 ELSE id_depto END) AS id_depto,
                (CASE WHEN "Nivel inicial - Jardín maternal"='1' OR "Nivel inicial - Jardín de infantes"='1' THEN '1' ELSE '0' END) AS jardin,
                (CASE WHEN Primario='1' THEN '1' ELSE '0' END) AS primario, (CASE WHEN Secundario='1' OR "Secundario - INET"='1' THEN '1' ELSE '0' END) AS secundario
FROM ee_comun
               """).df()

#cambio los tipos de dato de las columnas de niveles educativos a int               
est_edu[['jardin','primario','secundario']]=est_edu[['jardin','primario','secundario']].astype(int)

#%% ANALISIS DE DATOS - CONSULTAS SQL

"""
EN ESTA SECCIÓN ESTAREMOS HACIENDO EL PUNTO DE CONSULTAS SQL
"""                 
                 
#%% Creo la consulta_1

consulta_1=dd.sql("""
SELECT d.prov AS Provincia, d.depto AS Departamento, (CASE WHEN e.Jardines IS NULL THEN 0 ELSE e.Jardines END) AS Jardines,
        d.pobl_jardin AS "Poblacion Jardin", (CASE WHEN e.Primarias IS NULL THEN 0 ELSE e.Primarias END) AS Primarias,
        d.pobl_primaria AS "Poblacion Primaria", (CASE WHEN e.Secundarias IS NULL THEN 0 ELSE e.Secundarias END) AS Secundarias,
        d.pobl_secundaria AS "Poblacion Secundaria"
FROM departamento AS d                    
LEFT JOIN (SELECT id_depto, SUM(jardin) AS Jardines, SUM(primario) AS Primarias,
           SUM(secundario) AS Secundarias
           FROM est_edu
           GROUP BY id_depto) AS e
ON d.id_depto=e.id_depto
ORDER BY Provincia ASC, Primarias DESC 
                   """).df()
 
consulta_1.to_csv()   
 
#%% Creo la consulta_2
                   
consulta_2=dd.sql("""
SELECT d.prov AS Provincia, d.depto AS Departamento, 
        (CASE WHEN c.cant IS NULL THEN 0 ELSE c.cant END) AS "Cantidad de CC con cap>100"
FROM departamento AS d
LEFT JOIN (SELECT ID_DEPTO, COUNT(*) AS cant
           FROM centro_cult
           WHERE Capacidad>100
           GROUP BY ID_DEPTO) AS c
ON d.id_depto=c.ID_DEPTO 
ORDER BY Provincia ASC, "Cantidad de CC con cap>100" DESC                
                  """).df()                   

#%% Creo la consulta_3

consulta_3=dd.sql("""
SELECT d.prov AS Provincia, d.depto AS Departamento,
        (CASE WHEN e.cant_ee IS NULL THEN 0 ELSE e.cant_ee END) AS Cant_EE,
        (CASE WHEN c.cant_cc IS NULL THEN 0 ELSE c.cant_cc END) AS Cant_CC,
        p.pobl_total AS "Población total"
FROM departamento AS d
INNER JOIN poblacion AS p
ON p.pobl_jardin=d.pobl_jardin AND p.pobl_primaria=d.pobl_primaria
    AND p.pobl_secundaria=d.pobl_secundaria
LEFT JOIN (SELECT id_depto, COUNT(*) AS cant_ee
           FROM est_edu
           GROUP BY id_depto) AS e
ON e.id_depto=d.id_depto
LEFT JOIN (SELECT ID_DEPTO, COUNT(*) AS cant_cc
           FROM centro_cult
           GROUP BY ID_DEPTO) AS c
ON c.ID_DEPTO=d.id_depto    
ORDER BY Cant_EE DESC, Cant_CC DESC, Provincia ASC, Departamento ASC         
                  """).df()


#%% Creo la consulta_4

#a cada centro cultural que tiene mail le asigno su dominio
usa_dominio=dd.sql("""
SELECT u.ID_cc, m.dominio
FROM usa_mail AS u
LEFT JOIN mail_centros AS m
ON m.Mail=u.Mail            
           """).df()

#a cada departamento le asigno su dominio más usado
depto_dominio=dd.sql("""
SELECT c.ID_DEPTO, MAX(u.dominio) AS dominio
FROM centro_cult AS c
LEFT JOIN usa_dominio AS u
ON c.ID_cc=u.ID_cc 
GROUP BY ID_DEPTO           
           """).df()

#a cada departamento le asigno su provincia
consulta_4=dd.sql("""
SELECT d.prov AS Provincia, d.depto AS Departamento, dd.dominio AS "Dominio más frecuente en CC"
FROM departamento AS d
LEFT JOIN depto_dominio AS dd
ON dd.ID_DEPTO=d.id_depto
ORDER BY Provincia, Departamento                  
                  """).df()

#%% ANALISI DE DATOS - VISUALIZACION

"""
EN ESTA SECCIÓN ESTAREMOS HACIENDO EL PUNTO DE VISUALIZACION
DE LA SECCIÓN DE ANÁLISIS DE DATOS
"""

import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import ticker
import numpy as np

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
WHERE Cant_CC>0
                  """).df()

fig,ax=plt.subplots()
plt.rcParams['font.family']='sans-serif'
ax.scatter(data=cc_ee_1000, x='ee',y='cc',s=5)
ax.set_xlabel('Establecimientos educativos cada mil habitantes', fontsize='medium')
ax.set_ylabel('Centros culturales cada mil habitantes', fontsize='medium')
ax.set_title('Cantidad de centros culturales cada mil habitantes\n en función de establecimientos educativos cada mil habitantes')
plt.show()
