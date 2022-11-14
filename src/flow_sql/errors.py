"""
psycopg2 errors redeclares here. The module will serve as facade in the future when (if)
other databases be supported.
For docs of the errors please refer to the psycopg2 documentation
"""


import psycopg2

Error = psycopg2.Error
InterfaceError = psycopg2.InterfaceError
DatabaseError = psycopg2.DatabaseError
DataError = psycopg2.DataError
OperationalError = psycopg2.OperationalError
IntegrityError = psycopg2.IntegrityError
InternalError = psycopg2.InternalError
ProgrammingError = psycopg2.ProgrammingError
NotSupportedError = psycopg2.NotSupportedError
