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


    with open('clusters_report.txt', 'r') as file:
        
        # PROCESAMIENTO DE ENCABEZADOS

        header = []

        for line in file:
            
            if re.match(r'-+', line):
                break

            else:

                titles = line.lower()
                titles = re.sub(r'\S\s\S', lambda x: x.group(0)[0] + '-' + x.group(0)[-1], titles)

                header.append([(x, y.start()) 
                               for x in titles.split()
                               for y in re.finditer(r'\b' + re.escape(x) + r'\b', titles)])
                
        
        header = [list(set(x)) for x in header]
        headerdict = {}

        for i in header:

            for item, key in i:

                if key in headerdict:
                    headerdict[key] = headerdict[key] + "-" + item
                
                else:
                    headerdict[key] = item
        

        headerdict = sorted(headerdict.items())
        header = [x[1] for x in headerdict]

        # PROCESAMIENTO DE ENTRADAS
        df_1 = pd.DataFrame([], columns = [header])

        while True:

            data = []

            for line in file:
                
                lines = [x for x in line.strip().replace("%", "").split(sep=" ") if x != ""]

                if lines == []:
                    break

                else: 
                    data += lines

            if len(data) == 0:
                break

            entry_1 = int(data[0])
            entry_2 = int(data[1])
            entry_3 = float(data[2].replace(",", "."))
            entry_4 = ' '.join(map(str, data[3:]))

            
            df_2 = pd.DataFrame([[entry_1, entry_2, entry_3, entry_4]], columns = [header])
            
            df_1 = pd.concat([df_1, df_2])
        
        
        return df_1


