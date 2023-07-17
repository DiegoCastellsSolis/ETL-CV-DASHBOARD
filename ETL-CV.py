import pandas as pd
import numpy as np
import PyPDF2
import os
import csv

#THIS FUNCTION TRANSFORM PDF TO TEXT PLAIN
def extraer_texto_pdf(nombre_archivo):
    with open(nombre_archivo, 'rb') as archivo_pdf:
        lector_pdf = PyPDF2.PdfReader(archivo_pdf)
        texto = ''
        for pagina in lector_pdf.pages:
            texto += pagina.extract_text()
        return texto

#THIS FUNCTION LOAD DATA TO CSV
def loac_csv(candidatos,ruta_csv,columns):  
    with open(ruta_csv, 'w', newline='') as archivo_csv:
        writer = csv.writer(archivo_csv)
        
        # Escribir encabezados
        encabezados = list(columns)
        writer.writerow(encabezados)
        
        # Escribir datos de los candidatos
        writer.writerows(candidatos)

Stack = {
        'Lenguajes': ['Python', 'Java', 'Scala', 'Sas','Bash','Ruby','Html','Css','JavaScrip'],  
        'ETL': ['Apache Airflow', 'Airflow', 'Talend', 'Informatica PowerCenter', 'PowerCenter', 'IBM InfoSphere DataStage', 'DataStage', 'Microsoft SQL Server Integration Services', 'SSIS', 'Oracle Data Integrator', 'ODI'],
        'Cloud': ['Amazon Web Services', 'AWS', 'Microsoft Azure', 'Azure', 'Google Cloud Platform', 'GCP', 'IBM Cloud', 'Oracle Cloud Infrastructure', 'Alibaba Cloud'],
        'Clientes_base_datos': ['SQL','MySQL', 'Oracle Database', 'Microsoft SQL Server', 'PostgreSQL', 'SQLite', 'IBM Db2', 'MariaDB'],
        'Visualizacion': ['Metabase','Tableau', 'Power BI', 'QlikView', 'Plotly', 'Matplotlib', 'Seaborn', 'D3.js', 'Grafana'],
        'Data_Science': ['NumPy', 'Pandas', 'SciPy', 'Scikit-learn', 'TensorFlow', 'Keras', 'PyTorch', 'NLTK'],
        'Big_Data': ['Hadoop', 'Spark', 'Apache Kafka', 'Apache Hive', 'Apache Pig', 'Apache Cassandra', 'Apache HBase', 'Apache Beam'],
        'Almacenes_de_datos': ['Amazon Redshift', 'Snowflake', 'Google BigQuery', 'Microsoft Azure Synapse Analytics', 'Oracle Exadata', 'IBM Db2 Warehouse', 'Teradata']
    }

def main(columns,path_cv_to_proccesing,output_file,dataset):

    # Verificar si la ruta existe
    if os.path.exists(path_cv_to_proccesing):
        # Obtener la lista de archivos en la ruta
        archivos = os.listdir(path_cv_to_proccesing)
        candidatos = []
        # Recorrer los archivos y extraer los datos
        for aux_arch in archivos:
            archivo = os.path.join(path_cv_to_proccesing, aux_arch)
            texto_curriculum = extraer_texto_pdf(archivo)
            participante = {
                    'Nombre': [],
                    'Lenguajes': [],
                    'ETL': [],
                    'Cloud': [],
                    'Clientes_base_datos': [],
                    'Visualizacion': [],
                    'Data_Science': [],
                    'Big_Data': [],
                    'Almacenes_de_datos': []
            }
            for categoria, valores in Stack.items():
                for valor in valores:
                    if valor.lower() in texto_curriculum.lower():
                        participante[categoria].append(valor)

            longitudes = {categoria: len(valores) for categoria, valores in participante.items()}

            # Encuentra la lista con la mayor longitud
            longitud_maxima = max(longitudes.values())

            # Modifica todas las listas para que tengan la longitud de la lista más larga
            for categoria, valores in participante.items():
                if len(valores) < longitud_maxima:
                    if categoria == 'Nombre':
                        valores.extend([aux_arch]* (longitud_maxima - len(valores)))
                    else:
                        valores.extend([""] * (longitud_maxima - len(valores)))

            # Agregar el participante al dataset
            dataset.append(participante)

            tuplas = list(zip(*participante.values()))

            candidatos.append(tuplas)

        candidatos = [list(candidato) for candidato in candidatos]

        lista_candidatos = []
        for candidato in candidatos:
            for x in candidato:
                lista_candidatos.append(x)
        
        #print(lista_candidatos)

        # Crear el DataFrame y guardar como CSV
        loac_csv(lista_candidatos,output_file,columns) 
        print(f"Archivo CSV guardado exitosamente como '{output_file}'")
    else:
        print("La ruta especificada no existe.")

def transform_and_load(path,path_cand):
    data = pd.read_csv(path,encoding="utf-8")
    data['Cloud'] = data['Cloud'].replace('Amazon Web Services', 'AWS')
    data['Cloud'] = data['Cloud'].replace('Google Cloud Platform', 'GCP')
    data.to_csv(path_cand, index=False)

if __name__ == "__main__" :
    # Definir las columnas
    columns = ['Nombre', 'Lenguajes', 'ETL', 'Cloud', 'Clientes_base_datos', 'Visualizacion', 'Data_Science', 'Big_Data', 'Almacenes_de_datos']

    # Ejemplo de uso
    path_cv_to_proccesing = "C:/Users/Usuario/Desktop/PP/CV-automatization/1.cv_to_proccesing"
    output_file = "C:/Users/Usuario/Desktop/PP/CV-automatization/2.cv_proccesed/data.csv"  

    # Crear una lista para almacenar los participantes
    dataset = []

    main(columns,path_cv_to_proccesing,output_file,dataset)
    path = "C:/Users/Usuario/Desktop/PP/CV-automatization/2.cv_proccesed/data.csv"
    path_cand = "C:/Users/Usuario/Desktop/PP/CV-automatization/2.cv_proccesed/Candidatos.csv"
    transform_and_load(path,path_cand) 