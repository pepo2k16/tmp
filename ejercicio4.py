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
    cantidad_ocurrencias_total = 0
    cantidad_palabras = 0

    for v in values:
        palabra, cantidad = v      
        cantidad_ocurrencias_total += int(cantidad)
        cantidad_palabras += 1

    context.write("promedio", " ".join([str(cantidad_ocurrencias_total / cantidad_palabras), str(cantidad_palabras), str(cantidad_ocurrencias_total)]))

inputDir2 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output'
outputDir2 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output2'

job = Job(inputDir2, outputDir2, fmap2, fred2)
job.waitForCompletion()                         

# se calcula el promedio de ocurrencias de las palabras

with open(os.path.join(outputDir2, "output.txt"), "r") as f:
    promedio = f.readlines()[0].split()[1]
    print(promedio)

def fmap3(key, value, context):
    if int(value) > float(context["promedio"]):
        context.write(key, value)
        
def fred3(key, values, context):
    for v in values:
        context.write(key, v)

# se escriben aquellas palabras que ocurrieron más cantidad de veces que el promedio

job = Job(outputDir1, outputDir1, fmap3, fred3)
job.setParams({"promedio" : promedio })
job.waitForCompletion()