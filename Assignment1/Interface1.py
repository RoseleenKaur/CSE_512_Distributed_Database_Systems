import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur=openconnection.cursor()
    cur.execute('Drop table if exists {0}'.format((ratingstablename)))
    cur.execute('Create table if not exists temp_Table (userid integer,temp1 text, movieid integer,temp2 text,rating float,temp3 text,time text)')
    
    f_contents = open(ratingsfilepath, 'r')
    cur.copy_from(f_contents, 'temp_Table',":",columns=('userid','temp1', 'movieid','temp2','rating','temp3','time'))
    cur.execute('ALTER TABLE temp_table \
                DROP COLUMN IF EXISTS temp1,\
                DROP COLUMN IF EXISTS temp2,\
                DROP COLUMN IF EXISTS temp3,\
                DROP COLUMN IF EXISTS time')
    cur.execute('ALTER TABLE temp_table RENAME TO {0}'.format(ratingstablename))
    
    openconnection.commit()
    
    


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions<1:
        return
    cur=openconnection.cursor()
    step_size=(5.0)/numberofpartitions
    count=0
    lower,upper=0,step_size   
    while(count<numberofpartitions):
        table_name='range_ratings_part'+str(count)
        cur.execute('CREATE TABLE {0} AS SELECT * FROM {1} WHERE rating >{2} and rating<={3}'.format(table_name,ratingstablename,lower,upper))
        lower=upper
        upper=upper+step_size
        count=count+1
    cur.execute('Insert into range_ratings_part0 Select * from {0} where rating={1}'.format(ratingstablename,'0'))
    openconnection.commit()
    


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur=openconnection.cursor()
    for i in range(numberofpartitions):
        table_name="round_robin_ratings_part"+str(i)
        cur.execute('Create table if not exists {0} (userid integer, movieid integer,rating float)'.format(table_name))
    cur.execute('select  *,row_number() over(order by userid,movieid,rating) from ratings')
    list=cur.fetchall()
    for row in list:
        partition=(row[3]-1)%numberofpartitions
        table_name="round_robin_ratings_part"+str(partition)
        cur.execute('Insert into {0} VALUES {1}'.format(table_name,(row[0],row[1],row[2])))
    openconnection.commit()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur=openconnection.cursor()
    cur.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE '{0}'".format('round_robin_ratings_part%'))
    number_of_part=cur.fetchone()[0]
    cur.execute('SELECT count(*) from {0}'.format(ratingstablename))
    number_of_rows=cur.fetchone()[0]
    partition=(number_of_rows)%number_of_part
    fragment='round_robin_ratings_part'+str(partition)
    cur.execute('Insert into {0} VALUES {1}'.format(ratingstablename,(userid, itemid, rating)))
    cur.execute('Insert into {0} VALUES {1}'.format(fragment,(userid, itemid, rating)))
    openconnection.commit()
    


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur=openconnection.cursor()
    cur.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE 'range_ratings_part%'")
    number_of_part=cur.fetchone()[0]
    cur.execute('Insert into {0} VALUES {1}'.format(ratingstablename,(userid, itemid, rating)))
    part=int(rating/(5.0/number_of_part))
    if part==rating/(5.0/number_of_part) and part>0:
        part=part-1
    fragment='range_ratings_part'+str(part)
    cur.execute('Insert into {0} VALUES {1}'.format(ratingstablename,(userid, itemid, rating)))
    cur.execute('Insert into {0} VALUES {1}'.format(fragment,(userid, itemid, rating)))
    openconnection.commit()
    

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    cur=openconnection.cursor()
    count=0
    cur.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE '{0}'".format('range_ratings_part%'))
    number_of_range_part=cur.fetchone()[0]
    start=int(ratingMinValue/(5.0/number_of_range_part))
    if start==ratingMinValue/(5.0/number_of_range_part) and start>0:
        start=start-1
        
    end=int(ratingMaxValue/(5.0/number_of_range_part))
    if end==ratingMaxValue/(5.0/number_of_range_part) and end>0:
        end=end-1
    f=open(outputPath, 'w')
        
    for i in range(start,end+1):
        table_name='range_ratings_part'+str(i)
        query="SELECT '{0}',* FROM {0} WHERE rating>={1} AND rating<={2}".format(table_name,ratingMinValue,ratingMaxValue)
        outputquery = "COPY ({0}) TO STDOUT DELIMITER as ','".format(query)
        with open(outputPath, 'a') as f:
            cur.copy_expert(outputquery, f)
            
    
    cur.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE '{0}'".format('round_robin_ratings_part%'))
    number_of_robin_part=cur.fetchone()[0]
    for i in range(number_of_robin_part):
        table_name='round_robin_ratings_part'+str(i)
        query="SELECT '{0}',* FROM {0} WHERE rating>={1} AND rating<={2}".format(table_name,ratingMinValue,ratingMaxValue)
        outputquery = "COPY ({0}) TO STDOUT DELIMITER as ','".format(query)
        with open(outputPath, 'a') as f:
            cur.copy_expert(outputquery, f)
    openconnection.commit()


def pointQuery(ratingValue, openconnection, outputPath):
    rangeQuery(ratingValue, ratingValue, openconnection, outputPath)


def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
