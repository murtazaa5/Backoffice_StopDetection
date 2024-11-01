import pyodbc
import aioodbc

class AsyncConnection:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        await self.connection.close()
        
    async def execute(self, query, *params):
        async with self.connection.cursor() as cursor:
            await cursor.execute(query, *params)
            return await cursor.fetchall()

    async def commit(self):
        await self.connection.commit()

    async def connect(self):
        return self.connection

async def get_async_connection(autocommit):
    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=192.168.6.3;"
        "Database=Central2;"
        "UID=sa;"
        "PWD=Regency1;"
        "autocommit=" + str(autocommit)
    )
    
    cnxn = await aioodbc.connect(dsn=connection_string)
    return AsyncConnection(cnxn)
    
def getConnection(autocommit):
    cnxn = pyodbc.connect("DRIVER={SQL Server}"
        + ";SERVER=192.168.6.3"
        + ";DATABASE=Central2"
        + ";UID=sa"
        + ";PWD=Regency1", autocommit=autocommit)
    return cnxn