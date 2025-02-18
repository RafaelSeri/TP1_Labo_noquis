import pandas as pd
import duckdb as dd
#%% cargo los datasets
centros_cult=pd.read_csv('centros_culturales.csv')
padron_ee=pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx',skiprows=6)
poblacion=pd.read_excel('padron_poblacion.xlsX')
#%% Para crear la tabla departamento, primero filtro poblacion juntando todos los datos en una tabla

pobl_limpio=poblacion[['Unnamed: 1', 'Unnamed: 2']] #elijo las columnas que quiero
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

#upper_y_sacar_tildes(x) toma un string y devuelve otro con todas las letras en
#mayúsculas y sin las tildes
def upper_y_sacar_tildes(x):
    x=x.upper()
    y=''
    for i in x:
        if i=='Á':
            y+='A'
        elif i=='É':
            y+='E'
        elif i=='Í':
            y+='I'
        elif i=='Ó':
            y+='O'
        elif i=='Ú':
            y+='U'
        else:
            y+=i
    return y

#para mayor comodidad y consistencia con otras tablas modifico los valores de depto
for i,e in pobl_limpio.iterrows():
    pobl_limpio.loc[i,'depto']= upper_y_sacar_tildes(pobl_limpio.loc[i,'depto'])

#elimino filas sobrantes
pobl_limpio=pobl_limpio.iloc[:53949]

#Asigno el nivel educativo a la fila correspondiente y de paso cambio valores
#de otras columnas por temas de consistencia con otras tablas
asign=dd.sql("""
SELECT (CASE WHEN depto LIKE '%COMUNA%' THEN 'CABA' ELSE depto END) AS depto,
        (CASE WHEN depto LIKE '%COMUNA%' THEN 2000 ELSE id_depto END) AS id_depto,
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


provs=dd.sql("""
SELECT DISTINCT p.id_depto,e.Cueanexo
FROM pobl_grupos AS p
LEFT JOIN ee_limpio AS e
ON p.id_depto=e.id_depto
             """).df()
             
consulta=dd.sql("""
SELECT g.*,p.Provincia
FROM pobl_grupos AS g
LEFT JOIN provs AS p
ON g.id_depto=p.ID_DEPTO                
                """).df()
                
consulta2=dd.sql("""
SELECT c1.*
                 
                 """)                

x=provs['id_depto'].value_counts()

departamento=dd.sql("""
SELECT DISTINCT p.depto, p.id_depto, e.prov, p.pobl_jardin, p.pobl_primaria,
                p.pobl_secundaria, p.pobl_total
FROM pobl_grupos AS p

ORDER BY p.id_depto                 
                """).df()

#%% filtro centros_cult dejando solo las columnas correspondietnes
cc_uno=centros_cult[['ID_DEPTO','Nombre', 'Mail ','Capacidad']]
cc_uno['ID_cc']=0

for i,e in cc_uno.iterrows():
        cc_uno.loc[i,'ID_cc']=i

cc_uno=cc_uno.rename(columns={'Mail ':'Mail'})
            
cc_limpio=cc_uno[['ID_cc','ID_DEPTO','Nombre','Capacidad']]

#%% para crear usa_mail uso cc_uno


#separar_mails(x) toma un string y devuelve una lista con 
def separar_mails(x):
    return x.split()
        
#comienzo creando la tabla mail_cc
mail_centro=cc_uno[['ID_cc','Mail']]

#saco los valores null
mail_centro=mail_centro.dropna()

#creo un df que en donde cada fila tiene un ID_cc y un mail correspondiente 
filas=[]
for i,e in mail_centro.iterrows():
    mails=separar_mails(e['Mail'])
    for j in mails:
        filas+=[{'ID_cc':e['ID_cc'],'Mail':j}]
mail=pd.DataFrame(filas)

#mail_aux recupera los ID_cc que tenían '' en Mail
usa_mail_aux=dd.sql("""
SELECT m1.ID_CC, m2.Mail
FROM mail_centro AS m1
LEFT JOIN mail AS m2
ON m1.ID_cc=m2.ID_cc     
ORDER BY m1.ID_cc      
               """).df()

usa_mail=dd.sql("""
SELECT u.*
FROM usa_mail_aux AS u
INNER JOIN mail_centros AS m
ON u.Mail=m.Mail
                
                """).df()
                
#%%creo la tabla para mail_centros

mail_centros=dd.sql("""
SELECT DISTINCT Mail
FROM usa_mail_aux
WHERE Mail LIKE '%@%'                   
                    """).df()


#%%
#%% filtro padron_ee dejando solo las columnas correspondientes

#creo un df con las columnas que me interesan
ee_columns=padron_ee[['Cueanexo','Departamento','Común','Nivel inicial - Jardín maternal',
                     'Nivel inicial - Jardín de infantes','Primario','Secundario',
                     'Secundario - INET']]

#convierto en string los valores de Común
ee_columns[['Cueanexo','Departamento','Común','Nivel inicial - Jardín maternal','Nivel inicial - Jardín de infantes',
            'Primario','Secundario','Secundario - INET']] = ee_columns[['Cueanexo','Departamento','Común','Nivel inicial - Jardín maternal',
                     'Nivel inicial - Jardín de infantes','Primario','Secundario','Secundario - INET']].astype(str)

ee_comun=dd.sql("""
SELECT Cueanexo, Departamento, "Nivel inicial - Jardín maternal",
       "Nivel inicial - Jardín de infantes", Primario, Secundario,
       "Secundario - INET"
FROM ee_columns
WHERE Común='1'               
                """).df()

ee_tipos=dd.sql("""
SELECT Cueanexo, (CASE WHEN Departamento LIKE '%Comuna%' THEN 'CABA' ELSE Departamento END) AS Departamento,
                (CASE WHEN "Nivel inicial - Jardín maternal"='1' OR "Nivel inicial - Jardín de infantes"='1' THEN 'J' ELSE '' END) AS jardin,
                (CASE WHEN Primario='1' THEN 'P' ELSE '' END) AS primario, (CASE WHEN Secundario='1' OR "Secundario - INET"='1' THEN 'S' ELSE '' END) AS secundario
FROM ee_comun
               """).df()
               
ee_tipos['tipo_edu']=ee_tipos['jardin']+ee_tipos['primario']+ee_tipos['secundario']



ee_tipos=ee_tipos[['Cueanexo','Departamento','tipo_edu']]         

ee_limpio=dd.sql("""
SELECT DISTINCT p.id_depto,e.Cueanexo,e.tipo_edu
FROM ee_tipos AS e
INNER JOIN pobl_grupos AS p
ON p.depto=e.Departamento                 
                 """).df()
                 
                 
                 
