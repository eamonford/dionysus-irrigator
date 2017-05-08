import psycopg2
import sqlite3
import os
import time
import getopt
import logging


logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

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
		self.irrigatorMaster = os.getenv('IRRIGATOR_MASTER', '')

	def getDatabaseConnection(self):
		try:
			connection = psycopg2.connect(
			host=self.pgHost,
			database=self.pgDatabase,
			user=self.pgUser,
			password=self.pgPass)
			Logger.info("Connected to Postgres host " + self.pgHost)
			return connection
		except:
			Logger.error("Unable to connect to Postgres at " + self.pgHost)
			raise
