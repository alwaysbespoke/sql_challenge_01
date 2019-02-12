import csv

class SQLInMemory:

	def __init__(self, paths):
		print("SQLInMemory")

		self.result = "INSERT RESULT"

		for path in paths:
			self.openCSV(path)

	def openCSV(self, path):
		print("SQLInMemory -> openCSV()")

		with open("csv/"+path+".csv") as csvfile:

			rows = csv.reader(csvfile, delimiter=",")

			table = []

			for row in rows:
				table.append(row)

			if path == "Restaurants":
				self.restaurants = table
			else:
				self.ratings = table

			print(table)	

	def select(self, fields):
		print("SQLInMemory -> select()")
		self.selectQ = fields

	def fromR(self, tables):
		print("SQLInMemory -> from()")
		self.tablesQ = tables

	def where(self, locations):
		print("SQLInMemory -> where()")
		


class SQLOnDisk:

	def __init__(self):
		#do something here
		print("SQLOnDisk")


#instantiate SQLInMemory
sql = SQLInMemory(["Restaurants", "Ratings"])
print(sql.result)
