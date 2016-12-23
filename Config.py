import psycopg2
import sqlite3
import os
import time
import getopt
import logging

class Borg:
	_shared_state = {}
	def __init__(self):
		self.__dict__ = self._shared_state

class Configuration(Borg):
	def __init__(self):
		Borg.__init__(self)
		self.mqttHost = os.getenv('MQTT_HOST', 'localhost')
		self.pgHost = os.getenv('PG_HOST', 'localhost')
		self.pgUser = os.getenv('PG_USER', 'admin')
		self.pgPass = os.getenv('PG_PASS', 'password')
		self.pgDatabase = os.getenv('PG_DATABASE', 'database')

	def getDatabaseConnection(self):
		try:
			return psycopg2.connect(
			host=self.pgHost,
			database=self.pgDatabase,
			user=self.pgUser,
			password=self.pgPass)
		except:
			logging.error("Unable to connect to Postgres.")
			raise
