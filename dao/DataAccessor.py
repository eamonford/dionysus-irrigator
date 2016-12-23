import logging


class DataAccessor(object):
    """Access plant records in the database."""

    def __init__(self, table, connection):
        """Initialize a database connection and use the specified table for all queries."""
        self.connection = connection
        self.table = table

    def create(self, **kwargs):
        """Create a record."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("INSERT INTO " + self.table + " (" +
                           ", ".join(("created_at",
                                      ", ".join([key
                                                 for key, value
                                                 in kwargs.iteritems()]))) +
                           ") VALUES (" +
                           ", ".join(("now()",
                                     ", ".join(["'" + value + "'"
                                                if isinstance(value, basestring)
                                                else str(value)
                                                for key, value
                                                in kwargs.iteritems()]))) +
                           ")")
            self.connection.commit()
        except:
            raise
        finally:
            cursor.close()

    def getById(self, id):
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT * FROM " + self.table + " WHERE id = '" + str(id) + "'")
        except:
            raise
        else:
            return self._makeDictOfResponse(cursor.description,
                                             cursor.fetchall())
        finally:
            cursor.close()


    def _makeDictOfResponse(self, description, records):
        return [{columnName: record[columnIndex]
                for columnName, columnIndex
                in zip([column[0] for column in description],
                       range(len(record)))}
                for record in records]

    def getWithDateRanges(self, dateRanges, **params):
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT * FROM " + self.table + " WHERE " +
                           " OR ".join(["(created_at >= %s AND created_at <= %s)"
                                        for dateRange in dateRanges]) +
                           (" AND " +
                            " AND ".join(["{}={}".format(key, value)
                                          for key, value in params.iteritems()]))
                           if len(params) > 0 else "" +
                           " ORDER BY created_at ASC",
                           [date for dateRange in dateRanges
                            for date in dateRange])
        except:
            raise
        else:
            return self._makeDictOfResponse(cursor.description,
                                             cursor.fetchall())
        finally:
            cursor.close()

    def getAll(self):
        """Retrieve all records."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT * FROM " + self.table)
        except:
            logging.error("Unable to retrieve " + self.table +
                          " info from database.")
            raise
        else:
            return [{columnName: record[columnIndex]
                    for columnName, columnIndex
                    in zip([column[0] for column in cursor.description],
                           range(len(record)))}
                    for record in cursor.fetchall()]
        finally:
            cursor.close()

    def update(self, id, **kwargs):
        """Update a record in the database."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("UPDATE " + self.table + " SET " +
                           ", ".join(["{}={}".format(key, value)
                                     for key, value in kwargs.iteritems()]) +
                           " WHERE id=" +
                           str(id))
        except:
            logging.error("Unable to update " + self.table + " in database.")
            raise
        finally:
            cursor.close()

    def __del__(self):
        """Close the database connection."""
        self.connection.close()
