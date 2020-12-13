#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

def Sorting(inputTable, lower, upper, tb, column, openconnection, value_max):
    conn = openconnection.cursor()
    if (upper == value_max):
        conn.execute('CREATE TABLE {0} AS SELECT * FROM {1} WHERE {2}>={3} AND {2}<={4} order by {2}'.format(tb,inputTable,column,lower,upper))
        
    else:
        conn.execute('CREATE TABLE {0} AS SELECT * FROM {1} WHERE {2}>={3} AND {2}<{4} order by {2}'.format(tb,inputTable,column,lower,upper))
    openconnection.commit()

# Donot close the connection inside this file i.e. do not perform openconnection.close() 
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    conn = openconnection.cursor()
    conn.execute('SELECT max({0}) from {1}'.format(SortingColumnName,InputTable))
    value_max = conn.fetchone()[0]
    conn.execute('SELECT min({0}) from {1}'.format(SortingColumnName,InputTable))
    value_min = conn.fetchone()[0]
    diff = value_max - value_min
    diff = diff / 5.0
    list1 = []
    for i in range(5):
        list1.append(threading.Thread(target=Sorting, args=(
        InputTable, value_min, value_min + diff, "tb" + str(i), SortingColumnName, openconnection, value_max)))
        list1[i].start()
        value_min = value_min + diff
    for t in list1:
        t.join()
    conn.execute(
        "CREATE TABLE " + OutputTable + " AS (SELECT * FROM tb0  union all  SELECT * FROM tb1 union all SELECT * FROM tb2 union all SELECT * FROM tb3 union all SELECT * FROM tb4)")
    openconnection.commit()

    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    conn = openconnection.cursor()
    conn.execute('SELECT max({0}) from {1}'.format(Table1JoinColumn,InputTable1))
    value_max = conn.fetchone()[0]
    conn.execute('SELECT max({0}) from {1}'.format(Table2JoinColumn,InputTable2))
    value_max = max(value_max,conn.fetchone()[0])

    conn.execute('SELECT min({0}) from {1}'.format(Table1JoinColumn,InputTable1))
    value_min = conn.fetchone()[0]
    conn.execute('SELECT min({0}) from {1}'.format(Table2JoinColumn,InputTable2))
    value_min = min(value_min, conn.fetchone()[0])


    diff = value_max - value_min
    diff = diff / 5.0
    list1 = []
    for i in range(5):
        list1.append(threading.Thread(target=Join, args=(
            InputTable1,InputTable2, value_min, value_min + diff, "tb" + str(i), Table1JoinColumn, Table2JoinColumn, openconnection, value_max)))
        list1[i].start()
        value_min = value_min + diff
    for t in list1:
        t.join()
    conn.execute(
        "CREATE TABLE " + OutputTable + " AS (SELECT * FROM tb0  union all  SELECT * FROM tb1 union all SELECT * FROM tb2 union all SELECT * FROM tb3 union all SELECT * FROM tb4)")
    openconnection.commit()
    
    
def Join(InputTable1,InputTable2, lower, upper, tb, Table1JoinColumn, Table2JoinColumn, openconnection, value_max):
    conn = openconnection.cursor()
    if (upper == value_max):              
        conn.execute('Create table {0} as select * from {1} t1,{2} t2 \
        where t1.{3}=t2.{4} and t1.{3}>={5} and t1.{3}<={6} \
        and t2.{4}>={5} and t2.{4}<={6}'.format(tb,InputTable1,InputTable2,Table1JoinColumn 
        ,Table2JoinColumn,lower,upper))

    else:
        conn.execute('Create table {0} as select * from {1} t1,{2} t2 \
        where t1.{3}=t2.{4} and t1.{3}>={5} and t1.{3}<{6} \
        and t2.{4}>={5} and t2.{4}<{6}'.format(tb,InputTable1,InputTable2,Table1JoinColumn
        ,Table2JoinColumn,lower,upper))
    openconnection.commit()
    
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


