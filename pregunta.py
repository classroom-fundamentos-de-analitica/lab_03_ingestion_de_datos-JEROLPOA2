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

def ingest_data() -> pd.DataFrame:

    with open('clusters_report.txt', 'r') as file:
        
        # PROCESAMIENTO DE ENCABEZADOS

        header = []

        for line in file:
            
            if re.match(r'-+', line):
                break

            else:

                titles = line.lower()
                titles = re.sub(r'\S\s\S', lambda x: x.group(0)[0] + '_' + x.group(0)[-1], titles)

                header.append([(x, y.start()) 
                               for x in titles.split()
                               for y in re.finditer(r'\b' + re.escape(x) + r'\b', titles)])
                
        
        header = [list(set(x)) for x in header]
        headerdict = {}


        for i in header:

            for item, key in i:

                if key in headerdict:
                    headerdict[key] = headerdict[key] + "_" + item
                
                else:
                    headerdict[key] = item
        
        
        headerdict = sorted(headerdict.items())
        header = [x[1] for x in headerdict]

        df1 = pd.DataFrame()

        # PROCESAMIENTO DE ENTRADAS

        entry_1 = []
        entry_2 = []
        entry_3 = []
        entry_4 = []

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

            entry_1.append(int(data[0]))
            entry_2.append(int(data[1]))
            entry_3.append(float(data[2].replace(",", ".")))
            entry_4.append(' '.join(map(str, data[3:])))


        df1[header[0]] = entry_1
        df1[header[1]] = entry_2
        df1[header[2]] = entry_3
        df1[header[3]] = entry_4

        return df1