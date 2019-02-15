import csv

CSV_EXT = ".csv"
CSV_BASE = "csv/"
COMMA = ","
QUOTE = '"'
BREAK = '\n'
WRITE = 'w'
APPEND = 'a'
JOURNAL = 'Journal'

class SQLInMemory:

	def __init__(self, paths):
		print("SQLInMemory")

		self.result = []

		self.tables = {}

		for path in paths:

			self.openCSV(path)

	def openCSV(self, path):
		print("SQLInMemory -> openCSV()")

		with open(CSV_BASE + path + CSV_EXT) as csvfile:

			rows = csv.reader(csvfile, delimiter = COMMA)

			table = []

			fieldsByName = {}

			fieldsByIndex = []

			first = True

			for row in rows:

				if first:

					first = False

					i = 0

					for field in row:

						fieldsByName[field] = i

						fieldsByIndex.append(field)

						i += 1

				table.append(row)
			
			obj = {"table":table, "fieldsByName":fieldsByName, "fieldsByIndex":fieldsByIndex}

			self.tables[path] = obj	

	def Select(self, fields):
		print("SQLInMemory -> Select()")
		self.fieldsToSelectByName = fields

	def From(self, tables):
		print("SQLInMemory -> From()")
		self.fromTables = tables

	def Where(self, lookUps):
		print("SQLInMemory -> Where()")

		for tableName in self.fromTables:

			obj = self.tables[tableName]

			table = obj["table"]

			fieldsByName = obj["fieldsByName"]

			fieldsByIndex = obj["fieldsByIndex"]

			fieldsToSelectByIndex = self.getFieldsToSelectByIndex(fieldsByName)

			for row in table:
				
				#loop through WHERE cases
				for fieldName in lookUps:

					lookUp = lookUps[fieldName]

					fieldIndex = fieldsByName[fieldName]

					fieldVal = row[fieldIndex]

					#check if WHERE case is true
					if fieldVal == lookUp:

						result = self.getResult(fieldsToSelectByIndex, fieldsByIndex, row)
						
						#add the result to result
						self.result.append(result)

	def getResult(self, fieldsToSelectByIndex, fieldsByIndex, row):

			result = {}

			#if WHERE case is true grab the field data specified in SELECT
			for fieldIndex in fieldsToSelectByIndex:

				fieldName = fieldsByIndex[fieldIndex]

				result[fieldName] = row[fieldIndex]

			return result


	def getFieldsToSelectByIndex(self, fieldsByName):

		fieldsToSelectByIndex = []

		for fieldName in self.fieldsToSelectByName:

				fieldIndex = fieldsByName[fieldName]

				fieldsToSelectByIndex.append(fieldIndex)

		return fieldsToSelectByIndex

class SQLOnDisk:

	def __init__(self):
		#do something here
		print("SQLOnDisk")

	def Select(self, fields):
		print("SQLOnDisk -> Select()")
		
		with open(CSV_BASE + JOURNAL + CSV_EXT, mode=WRITE) as journalFile:

			writer = csv.writer(journalFile, delimiter=COMMA, quotechar=QUOTE, quoting=csv.QUOTE_MINIMAL, lineterminator=BREAK)
			writer.writerow(fields)

	def From(self, tables):
		print("SQLOnDisk -> From()")

		with open(CSV_BASE + JOURNAL + CSV_EXT, mode=APPEND) as journalFile:

			writer = csv.writer(journalFile, delimiter=COMMA, quotechar=QUOTE, quoting=csv.QUOTE_MINIMAL, lineterminator=BREAK)
			writer.writerow(tables)

	def Where(self, lookUps):
		print("SQLOnDisk -> Where()")

		self.result = []

		fieldsToSelectByName = []

		tables = []

		with open(CSV_BASE + JOURNAL + CSV_EXT) as csvfile:

			rows = csv.reader(csvfile, delimiter = COMMA)

			line = 0

			for row in rows:

				for i in row:

					if line == 0:
						fieldsToSelectByName.append(i)
					else:
						tables.append(i)

				line += 1

		for table in tables:

			with open(CSV_BASE + table + CSV_EXT) as csvfile:

				rows = csv.reader(csvfile, delimiter = COMMA)

				line = 0

				fieldsByName = {}
				fieldsByIndex = []
				fieldsToSelectByIndex = []

				for row in rows:

					if line == 0:

						i = 0

						for rowVal in row:

							fieldsByIndex.append(rowVal)

							fieldsByName[rowVal] = i

							i += 1

						fieldsToSelectByIndex = self.getFieldsToSelectByIndex(fieldsByName, fieldsToSelectByName)

					else:
						
						#loop through WHERE cases
						for fieldName in lookUps:

							lookUp = lookUps[fieldName]

							fieldIndex = fieldsByName[fieldName]

							fieldVal = row[fieldIndex]

							#check if WHERE case is true
							if fieldVal == lookUp:

								result = self.getResult(fieldsToSelectByIndex, fieldsByIndex, row)
								
								#add the result to result
								self.result.append(result)
						
					line += 1

	def getResult(self, fieldsToSelectByIndex, fieldsByIndex, row):

			result = {}

			#if WHERE case is true grab the field data specified in SELECT
			for fieldIndex in fieldsToSelectByIndex:

				fieldName = fieldsByIndex[fieldIndex]

				result[fieldName] = row[fieldIndex]

			return result


	def getFieldsToSelectByIndex(self, fieldsByName, fieldsToSelectByName):

		fieldsToSelectByIndex = []

		for fieldName in fieldsToSelectByName:

				fieldIndex = fieldsByName[fieldName]

				fieldsToSelectByIndex.append(fieldIndex)

		return fieldsToSelectByIndex

#instantiate SQLInMemory
sql = SQLInMemory(['Restaurants', 'Ratings'])
sql.Select(['name'])
sql.From(['Restaurants'])
sql.Where({'country':'Spain'})
print(sql.result)

#instantiate SQLOnDisk
sql = SQLOnDisk()
sql.Select(['name'])
sql.From(['Restaurants'])
sql.Where({'country':'Spain'})
print(sql.result)

#TODO

#1. check if field exists
#2. check if Select, From and Where are in the correct format
#3. create new methods to properly handle Join -> INNER JOINT and ON
#4. in OnDisk, need to check if the correct call sequence has been issued
#5. various other error checking
