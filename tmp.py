import os

outputDir = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output2'

with open(os.path.join(outputDir, "output.txt"), "r") as f:
    lineas = f.readlines()
    print(lineas[0].split()[2])
    print(lineas[1].split()[2])
    print(lineas[2].split()[1])
    #print(lineas[2].split("/t")[1])
    #promedio = lineas[2].split("/t")[1].split()[0]
    #print(promedio)