from MRE import Job
import os

inputDir1 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\input'
outputDir1 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output'

def fmap1(key, value, context):
    id_user = key
    id_page, time = value.split()
    context.write(" ".join([id_user, id_page]), time)
        
def fred1(key, values, context):
    tiempo_total = 0
    for v in values:
        tiempo_total += int(v)
    context.write(key, tiempo_total)

job = Job(inputDir1, outputDir1, fmap1, fred1)
job.setCombiner(fred1)
job.waitForCompletion()                        

# se calcula para cada usuario y página la cantidad de tiempo permanecido

def fmap2(key, value, context):
    id_user, id_page = key.split()
    time = value
    context.write(id_user, (id_page, time))
        
def fred2(key, values, context):
    tiempo_maximo = -1
    pagina_tiempo_maximo = ""

    for v in values:
        pagina, cantidad = v
        cantidad = int(cantidad)
        
        if (cantidad > tiempo_maximo):
            tiempo_maximo = cantidad
            pagina_tiempo_maximo = pagina

    context.write(key, " ".join([pagina_tiempo_maximo, str(tiempo_maximo)]))

inputDir2 = outputDir1
outputDir2 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output2'

job = Job(inputDir2, outputDir2, fmap2, fred2)
job.setCombiner(fred2)
job.waitForCompletion()                         

# se calcula la página más visitada por cada usuario

def fmap3(key, value, context):
    id_user, id_page = key.split()
    context.write(id_user, 1)

def fred3(key, values, context):
    paginas_totales = 0
    for v in values:
        paginas_totales += int(v)
    context.write(key, paginas_totales)

inputDir3 = outputDir1
outputDir3 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output3'

job = Job(inputDir3, outputDir3, fmap3, fred3)
job.setCombiner(fred3)
job.waitForCompletion()     

# se calcula para cada usuario la cantidad de páginas distintas visitadas

def fmap4(key, value, context):
    context.write("usuario_que_mas_paginas_visito", (key, value))

def fred4(key, values, context):
    cantidad_maxima = -1
    usuario_maximo = ""

    for v in values:
        usuario, cantidad_paginas = v
        if int(cantidad_paginas) > cantidad_maxima:
            cantidad_maxima = int(cantidad_paginas)
            usuario_maximo = usuario
    
    context.write(key, (usuario_maximo, cantidad_maxima))

inputDir4 = outputDir3
outputDir4 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output4'

job = Job(inputDir4, outputDir4, fmap4, fred4)
job.setCombiner(fred4)
job.waitForCompletion() 

# se calcula el usuario que más páginas distintas visitó

inputDir5 = inputDir1
outputDir5 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output5'

def fmap5(key, value, context):
    id_user = key
    id_page, time = value.split()
    context.write(id_page, 1)

job = Job(inputDir1, outputDir5, fmap5, fred3)
job.setCombiner(fred3)
job.waitForCompletion()                     

# se calcula para cada página la cantidad de visitas recibidas

inputDir6 = outputDir5
outputDir6 = 'C:\\Users\\user\\Desktop\\Facultad de Informática\\Octavo semestre\\Conceptos y Aplicaciones en Big Data\\output6'

def fmap6(key, value, context):
    context.write("pagina_mas_visitada", (key, value))

job = Job(inputDir6, outputDir6, fmap6, fred4)
job.setCombiner(fred4)
job.waitForCompletion()  