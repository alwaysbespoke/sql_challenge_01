import csv

CSV_EXT = ".csv"
CSV_BASE = "csv/"
COMMA = ","

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


#instantiate SQLInMemory
sql = SQLInMemory(["Restaurants", "Ratings"])
sql.From(["Restaurants"])
sql.Select(["name"])
sql.Where({"country":"Spain"})
print(sql.result)
