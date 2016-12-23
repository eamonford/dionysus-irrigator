from DataAccessor import DataAccessor

class SensorDataAccessor(DataAccessor):

    def __init__(self, connection):
        super(SensorDataAccessor, self).__init__("sensors", connection)
