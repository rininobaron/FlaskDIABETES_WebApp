from flask import Flask, render_template, request
import ibm_db
import requests

# Estableciendo parámetros de la base de datos en IBM-DB2
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BLUDB"
dsn_hostname = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.databases.appdomain.cloud"
dsn_port = "XXXXXX" 
dsn_protocol = "TCPIP"
dsn_uid = "XXXXXXXXXXXXX"
dsn_pwd = "XXXXXXXXXXXXX"
dsn_security = "SSL"

# inicializando flask
app = Flask (__name__)

app.secret_key = 'mysecretkey'

@app.route('/')
def index():
	return render_template('index.html')

@app.route('/calculando', methods = ['POST'])
def calculando():
	if request.method == 'POST':
		# Creando la conexión a la base datos
		dsn = (
	    	"DRIVER={0};"
	    	"DATABASE={1};"
	    	"HOSTNAME={2};"
	    	"PORT={3};"
	    	"PROTOCOL={4};"
	    	"UID={5};"
	    	"PWD={6};"
	    	"SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)
		
		try:
			conn = ibm_db.connect(dsn, "", "")
			print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
		except:
			print ("Unable to connect: ", ibm_db.conn_errormsg() )

		id_paciente = request.form['id_paciente']
		embarazos = request.form['embarazos']
		glucosa = request.form['glucosa']
		diastole = request.form['diastole']
		espesor = request.form['espesor']
		insulina = request.form['insulina']
		imc = request.form['imc']
		edad = request.form['edad']
		print(embarazos)
		print(glucosa)
		print(diastole)
		print(espesor)
		print(insulina)
		print(imc)
		print(edad)

		# Definiendo nombres de los campos de la tabla en IBM-DB2
		id_paciente_="PACIENTE"
		embarazos_="Número de embarazos"
		glucosa_="Nivel de glucosa plasmática"
		diastole_="Presión arterial diastólica"
		espesor_="Espesor de pliegue cutáneo"
		insulina_="Nivel de insulina sérica"
		imc_="IMC"
		prob_="PROBABILIDAD_DE_DIABETES"
		edad_="EDAD"
		diab_="¿Diabético?"

		# Inicializando prob y diab
		prob = 0
		diab = 0

		# Convirtiendo datos al tipo correcto
		id_paciente = int(id_paciente)
		embarazos = int(embarazos)
		glucosa = int(glucosa)
		diastole = int(diastole)
		espesor = int(espesor)
		insulina = int(insulina)
		imc = float(imc)
		prob = float(prob)
		edad = int(edad)
		diab = int(diab)

		print(type(id_paciente))
		print(type(embarazos))
		print(type(glucosa))
		print(type(diastole))
		print(type(espesor))
		print(type(insulina))
		print(type(imc))
		print(type(prob))
		print(type(edad))
		print(type(diab))	

		# Realizando petición al modelo desplegado en IBM Watson

		try:
			# Configurando permisos de acceso al modelo desplegado en IBM Watson
			API_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
			token_response = requests.post('https://iam.cloud.ibm.com/identity/token', data={"apikey": API_KEY, "grant_type": 'urn:ibm:params:oauth:grant-type:apikey'})
			mltoken = token_response.json()["access_token"]
			header = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + mltoken}
			# Declarando listas con los campos y valores de interes
			fields = ["embarazos", "glucosa", "diastole", "espesor", "insulina", "imc", "edad", ]
			values = [embarazos, glucosa, diastole, espesor, insulina, imc, edad]
			payload_scoring = {"input_data": [{"fields": fields, "values": [values]}]}
			# Realizando consulta
			response_scoring = requests.post('https://us-south.ml.cloud.ibm.com/ml/v4/deployments/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/predictions?version=2021-12-23', json=payload_scoring, headers={'Authorization': 'Bearer ' + mltoken})
			print("Scoring response")
			print(response_scoring.json())
			# Probabilidad de pertenencia a la clase positiva 'probability(1)'
			prob = response_scoring.json()['predictions'][0]['values'][0][2]

			if prob > 0.5:
				diab = 1
			else:
				diab = 0

		except:
			prob = 0
			diab = 0	
		
		# Insertando datos en la base de datos IBM-DB2
		insertQuery = insertQuery = "insert into DIABETES \
			values (%d, %d, %d, %d, %d, %d, %f, %f, %d, %d)" \
			% (id_paciente, embarazos, glucosa, diastole, espesor, insulina, imc, prob, edad, diab)
		insertStmt = ibm_db.exec_immediate(conn, insertQuery)

		# Cerrando conexión
		ibm_db.close(conn)

		return render_template('resultados.html', id_paciente = id_paciente, embarazos = embarazos, glucosa = glucosa,
			diastole = diastole, espesor = espesor, insulina = insulina, imc = imc, prob = prob, edad = edad, diab = diab)
