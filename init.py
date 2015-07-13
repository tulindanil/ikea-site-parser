import sqlite3

schemaFilename = 'schema.txt'
databaseName = 'data.sqlite3'

if __name__ == '__main__':

    print 'Initialize the database'

    f = open(schemaFilename, 'r')
    sqlRawText = f.read()

    database = sqlite3.connect(databaseName)
    cursor = database.cursor()

    sqlList = sqlRawText.split(';')
    
    for sql in sqlList:
        cursor.execute(sql + ';')

    database.commit()
    database.close()
