import pandas as pd

#%% cargo los datasets
centros_cult=pd.read_csv('centros_culturales.csv')
padron_ee=pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx',skiprows=6)
poblacion=pd.read_excel('padron_poblacion.xlsX')

#%% filtro poblacion juntando todos los datos en una tabla

poblacion=poblacion[['Unnamed: 1', 'Unnamed: 2','Unnamed: 3', 'Unnamed: 4']] #elijo las columnas que quiero
poblacion=poblacion.dropna(how='all') #saco las filas con todos valores NaN
poblacion['area']='' #creo una nueva columna 'area' 


def codigo_area(x):
    y=''
    for i in x:
        if i in ['0','1','2','3','4','5','6','7','8','9']:
            y+=i
    return y

#codigo_area(x) toma un string y devuelve otro con los numeros que tiene el primero

skip=0
for i,e in poblacion.iterrows():
    if skip>0:
        skip-=1
    if type(e['Unnamed: 1'])==str and 'AREA' in e['Unnamed: 1']:
        y=e['Unnamed: 1']
        skip=2
    if skip==0:
        poblacion.loc[i,'area']=codigo_area(y)

#le asigno a la columna 'area' un nuevo valor correspdiente al código de área

for i,e in poblacion.iterrows():
    if type(e['Unnamed: 1'])==str and (e['Unnamed: 1']=='Edad' or e['Unnamed: 1']=='Total' 
                                       or 'AREA' in e['Unnamed: 1'] or e['Unnamed: 1']=='RESUMEN'):
        poblacion=poblacion.drop(i,axis=0)

#saco las filas que no deben estar

poblacion=poblacion.rename(columns={'Unnamed: 1':'Edad','Unnamed: 2':'Casos','Unnamed: 3':'%','Unnamed: 4':'Acumulado %'})

#renombro las columnas

#%% filtro centros_cult dejando solo las columnas correspondietnes

centros_cult=centros_cult[['Cod_Loc','ID_PROV','ID_DEPTO','Provincia','Departamento',
                           'Localidad','Nombre','Domicilio','Telefóno','Mail ','Web','Capacidad']]

#%% filtro padron_ee dejando solo las columnas correspondientes

padron_ee.columns

padron_ee=padron_ee[['Jurisdicción','Cueanexo','Nombre','Sector','Domicilio','Teléfono','Código de localidad',
                     'Localidad','Departamento','Mail','Común','Nivel inicial - Jardín maternal',
                     'Nivel inicial - Jardín de infantes','Primario','Secundario',
                     'Secundario - INET','SNU','SNU - INET']]



