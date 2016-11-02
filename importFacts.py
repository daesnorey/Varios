#!/usr/bin/python


###########################################.
#############################################
	##########################################
	###########################################
	## Author: Daniel Novoa Reyes		#######
	## Year: 2016						#######
	## License: MIT Copyright (c) 2016	######
	#########################################
	########################################
###########################################
#########################################.


#import pypyodbc as db
import pyodbc as db
import os
import shutil
import re
import time
from os import listdir
from os.path import isfile, join

# Datos default, base de datos
tipo = "FS"
rollectura = 1139
rolcreacion = 1010
nivelseguridad = 10
proceso = 102
area = 22000000
obligatorio = 1
etapa = 3
proceso = 102
usuario = "daniel.novoa"
fecha = time.strftime("%Y-%m-%d")
comentario = "Cargue de contratos inicial, Au."

# Regex obtiene nombre contrato
regex = re.compile('^[a-zA-Z]+')

# Abre coneccion a la base de datos
connection = db.connect( 'Driver={SQL Server};'
								'Server=servidatos;'
								'Database=SYNERGY;'
								'uid=sqluser; pwd=jsanabria01' )

# Consultas, insertar, seleccionar
SQLCommand= ( "INSERT INTO Documents " 
				"(pry, tipo, etapa, Area, version, obligatorio, rollectura, rolcreacion, nivelseguridad, proceso, nombre, Archivo, usuario, fecha, comentario) "
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
			)
SQLSelectCommand = "SELECT pry, tipo, etapa, Area, descripcion, version, obligatorio, rollectura, rolcreacion, nivelseguridad, proceso FROM Doc_master_Pry WHERE (pry LIKE ? OR pry LIKE ?) AND tipo like 'FS' ORDER BY version DESC"
SQLSelectCommandVersion = "SELECT TOP 1 version FROM Documents WHERE (pry LIKE ?) AND tipo like 'FS' ORDER BY version DESC"
SQLSelectCommandExist = " SELECT archivo FROM Documents WHERE (pry LIKE ?) AND archivo LIKE ? AND tipo like 'FS' ORDER BY version DESC"

# Directorios donde estan los archivos a cargar
paths = [ 
			"\\\\centrocopiado\\contratos\\FACTURAS ENERO Y FEBRERO",
			"\\\\centrocopiado\\contratos\\FACTURAS MARZO",
			"\\\\centrocopiado\\contratos\\FACTURAS ABRIL", 
			"\\\\centrocopiado\\contratos\\FACTURAS MAYO",
			"\\\\centrocopiado\\contratos\\FACTURAS JUNIO",
			"\\\\centrocopiado\\contratos\\FACTURAS JULIO",
			"\\\\centrocopiado\\contratos\\FACTURAS AGOSTO",
			"\\\\centrocopiado\\contratos\\FACTURAS SEPTIEMBRE"	
		]

# Funcion Init, recorre cada uno de los directorios en path, y extrae los archivos que 
# existen dentro de el.
def init():

	for path in paths:

		# Filtra, solo archivos
		onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

		# Encabezado, separador entre cada directorio
		print "................................................................"
		print "................................................................"
		print "................................................................"
		print "................................................................"
		print path
		print len(onlyfiles)

		# Arreglo facturas cargadas
		facturasAdded = []
		
		# Contador, contratos no encontrados
		noCont = 0
		# Contador archivos existentes
		noExist = 0
		# Contador archivos a subir
		toUpload = 0

		# Recorre cada archivo dentro del arreglo onlyfiles
		for file in onlyfiles:

			# srcfile, ruta donde se encuentra el archivo
			# une el directorio, con el nombre del archivo
			srcfile = join(path, file)

			# Valida que el archivo no tenga NC en el nombre, ya que estos archivos
			# son de nota credito
			if file.find("NC ") > -1:
				continue

			# nfile, almacena el nombre del archivo sin espacios
			nfile = file.replace(" ", "")

			# nombreContrato obtiene las letras por las cuales inicia el contrato ej: cma, cmt
			nombreContrato = regex.search(nfile).group()
			# regex para sacar el numero de contrato ej: 004, 1225
			reg = re.compile(nombreContrato + '[0-9]+')
			# numeroContrato obtiene el numero de contrato
			numeroContrato = reg.search(nfile).group().replace(nombreContrato, "")

			# contrato almacena la union del nombre de contrato y numero de contrato sin guion(-) separador
			contrato = nombreContrato + numeroContrato
			# contratow almacena la union del nombre de contrato y numero de contrato con guion(-) separador
			contratow = nombreContrato + "-" + numeroContrato

			# regex para sacar el anio del contrato, solo si es 16 o 15 ya que no se cargan anios menores
			reg = re.compile(contrato + "-(16|15)+")
			
			# Valida que el anio exista en el archivo, si no es 16 o 15 se salta a la siguiente iteracion
			if reg.match(nfile) is None:
				continue

			# anioContrato obtiene el anio del contrato ej: 15, 16
			anioContrato = reg.search(nfile).group().replace(contrato, "")

			# Aniade anio contrato al contrato
			contrato = contrato.upper() + anioContrato
			# Aniade anio contrato al contratow
			contratow = contratow.upper() + anioContrato

			# Crea cursor SQL
			cursor = connection.cursor()
			# Ejecuta SQLSelectCommand, este busca si el contrato existe o tiene definida estructura en 
			# el sistema
			cursor.execute(SQLSelectCommand, [contrato, contratow])
			# Obtiene respuesta de la consulta
			resultado = list(cursor)

			# Obtiene numero de filas en la consulta
			nrows = len(resultado)

			# Valida que no tenga mas o menos de 1 fila
			# En caso que traiga mas o menos de 1 fila aumenta en uno el contador de contratos no existentes
			# Se salta a la siguiente iteracion
			if nrows != 1:
				noCont += 1
				#print nrows, contrato, contratow
				continue

			# Obtiene el contrato traido desde la base de datos
			realcontrato = resultado[0][0].strip()
			# Reemplaza los espacios con rayas al piso en el nombre del archivo
			nfile = file.replace(" ", "_")
			# Ejecuta SQLSelectCommandExist, este busca el contrato y archivo en la base de datos
			cursor.execute(SQLSelectCommandExist, [realcontrato, nfile])

			# Obtiene numero de filas en la consulta
			nrows = len(list(cursor))

			# Valida que tenga cero filas, si son mas filas
			# Aumenta el contador de contratos existentes en uno
			# Se salta a la siguiente iteracion
			if nrows != 0:
				noExist += 1
				#print noExist, contrato, contratow
				continue

			# Ejecuta SQLSelectCommandVersion, este busca la version actual del documento
			cursor.execute(SQLSelectCommandVersion, [realcontrato])
			# Asigna primer fila a resultado
			resultado = cursor.fetchone()

			# Si el resultado es None asigna version 1, de otra manera le aumenta uno a la version
			version = 1 if resultado is None else resultado[0] + 1

			# Asigna a nombre el nombre que se va a almacenar en la base de datos
			# tipo_nombrecontrato_dia_mes_anio_version
			nombre = tipo + "_" + str(realcontrato) + "_" + time.strftime("%d_%m_%Y") + "_" + str(version)

			# Imprime datos
			#print version, realcontrato, file, nombre

			# Agrega archivo al arreglo facturas added
			facturasAdded.append(nfile)

			# Arreglo Values con los datos a grabar en la base de datos
			Values = [realcontrato, tipo, etapa, area, version, obligatorio, rollectura, rolcreacion, nivelseguridad, proceso, nombre, nfile, usuario, fecha, comentario]

			# Aumenta contados archivos a cargar en 1
			toUpload += 1

			# Ejecuta la insercion en la base de datos
			cursor.execute(SQLCommand, Values)
			connection.commit()

			# Llama funcion copy, para copiar el archivo a la ubicacion fisica del servidor
			copy(realcontrato, srcfile)

		# Informativo
		print noCont, "No Existe Contrato"
		print noExist, "Archivos que ya existen"
		print toUpload, "Archivos a cargar"

		if(len(facturasAdded) > 0):
			print "______________Archivos cargados______________"
			print facturasAdded

# Funcion copy, copia el archivo a la direccion respectiva en el servidor.
def copy(folder, srcfile):

	# Directorio raiz donde se debe grabar el archivo
	dstroot = "\\\\serviback\\d$\\Synergy\\docsonline\\gest_doc\\" + folder + "\\Facturacion\\Facturas"
	#dstroot = "prueba\\" + folder + "\\Facturacion\\Facturas"


	dstdir =  dstroot#os.path.join(dstroot, os.path.basename(srcfile))
	oldFileName = join(dstdir, os.path.basename(srcfile))
	newFilename = join(dstdir, os.path.basename(srcfile).replace(" ", "_"))

	#print dstdir, "dstdir"
	#print dstroot
	#print srcfile
	#print "/////////////////////////////////////////////////////////////////////////"
	if os.path.isdir(dstdir) is False:
		os.makedirs(dstdir) # create all directories, raise an error if it already exists
	shutil.copy(srcfile, dstdir)
	os.rename(oldFileName, newFilename)

	print dstdir, newFilename


# Ejecuta funcion init
init()

# Cierra coneccion
connection.close()
