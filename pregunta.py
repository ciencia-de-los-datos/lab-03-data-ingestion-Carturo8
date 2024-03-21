"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():
    # Leer el archivo y convertirlo en un dataframe de Pandas
    df = pd.read_fwf('clusters_report.txt', colspecs="infer", widths=[9, 16, 16, 80], header=None, 
                     names=['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave'], 
                     converters={"porcentaje_de_palabras_clave": lambda x: x.rstrip(" %").replace(",", ".")}).drop(index={0,1,2}).ffill()
    
    # Convertir el campo "principales_palabras_clave" a una cadena para cada cluster
    df['principales_palabras_clave'] = df.groupby('cluster')['principales_palabras_clave'].transform(lambda x: ' '.join(x))

    # Tomar el máximo de los valores de "cantidad_de_palabras_clave" y "porcentaje_de_palabras_clave"
    df = df.groupby('cluster', as_index=False).agg({
        'cantidad_de_palabras_clave': 'max',
        'porcentaje_de_palabras_clave': 'max',
        'principales_palabras_clave': 'first'  # Tomar el primer valor, ya que es el mismo para todos los registros
    })
    
    # Limpiar las palabras clave para eliminar espacios y tabulaciones adicionales
    df['principales_palabras_clave'] = df['principales_palabras_clave'].apply(lambda x: ' '.join(x.split()))

    # Eliminar el punto final en las palabras
    df['principales_palabras_clave'] = df['principales_palabras_clave'].apply(lambda x: re.sub(r'(\w+)\.', r'\1', x))

    # Convertir el campo "cluster" a entero
    df['cluster'] = df['cluster'].astype(int)

    # Convertir el campo "cantidad_de_palabras_clave" a entero
    df['cantidad_de_palabras_clave'] = df['cantidad_de_palabras_clave'].astype(int)

    # Convertir el campo "porcentaje_de_palabras_clave" a flotante
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].astype(float)

    # Ordenar el DataFrame por el campo "cluster"
    df = df.sort_values(by='cluster').reset_index(drop=True)
    
    return df