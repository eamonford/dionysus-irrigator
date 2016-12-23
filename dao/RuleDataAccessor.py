from DataAccessor import DataAccessor

class RuleDataAccessor(DataAccessor):

    def __init__(self, connection):
        super(RuleDataAccessor, self).__init__("rules", connection)


    def getBySensorId(self, sensorId):
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT * FROM " + self.table + " WHERE sensor_id = '" + str(sensorId) + "'")
        except:
            raise
        else:
            return self._makeDictOfResponse(cursor.description,
                                             cursor.fetchall())
        finally:
            cursor.close()
