from MRE import Job
import os

inputDir1 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\input'
outputDir1 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output'

def fmap1(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)
        
def fred1(key, values, context):
    c=0
    for v in values:
        c += int(v)
    context.write(key, c)

job = Job(inputDir1, outputDir1, fmap1, fred1)
job.setCombiner(fred1)
job.waitForCompletion()                        

# se cuenta la cantidad de cada palabra

def fmap2(key, value, context):
    context.write(1, (key, value))
        
def fred2(key, values, context):
    minimo = float("inf")
    palabra_minima = ""

    maximo = -1
    palabra_maxima = ""

    cantidad_ocurrencias_total = 0
    cantidad_palabras = 0

    for v in values:
        palabra, cantidad = v
        cantidad = int(cantidad)
        
        if (cantidad > maximo):
            maximo = cantidad
            palabra_maxima = palabra

        if (cantidad < minimo):
            minimo = cantidad
            palabra_minima = palabra

        cantidad_ocurrencias_total += cantidad
        cantidad_palabras += 1

    context.write("palabra_maxima", " ".join([palabra_maxima, str(maximo)]))
    context.write("palabra_minima", " ".join([palabra_minima, str(minimo)]))
    context.write("promedio", " ".join([str(cantidad_ocurrencias_total / cantidad_palabras), str(cantidad_palabras), str(cantidad_ocurrencias_total)]))
    context.write("cantidad_palabras", " ".join([str(cantidad_palabras)]))

inputDir2 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output'
outputDir2 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output2'

job = Job(inputDir2, outputDir2, fmap2, fred2)
job.waitForCompletion()                         

# se calcula la cantidad mínima, máxima y promedio de ocurrencias de las palabras

with open(os.path.join(outputDir2, "output.txt"), "r") as f:
    lineas = f.readlines()
    maximo = lineas[0].split()[2]
    minimo = lineas[1].split()[2]
    promedio = lineas[2].split()[1]
    cantidad_palabras = int(lineas[3].split()[1])

def fmap3(key, value, context):
    context.write(1, (int(value) - float(context["promedio"]))**2)
        
def fred3(key, values, context):
    total_sumatoria = 0
    for v in values:
        total_sumatoria += int(v)
    context.write(key, total_sumatoria)

# se calcula la sumatoria para calcular posteriormente la desviación estandar

job = Job(outputDir1, outputDir1, fmap3, fred3)
job.setCombiner(fred3)
job.setParams({"promedio" : promedio })
job.waitForCompletion()                        

with open(os.path.join(outputDir1, "output.txt"), "r") as f:
    total_sumatoria = int(f.readlines()[0].split()[1])

print(maximo, minimo, promedio, total_sumatoria / ( cantidad_palabras - 1 ))