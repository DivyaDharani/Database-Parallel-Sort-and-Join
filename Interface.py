#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading
import traceback

# Donot close the connection inside this file i.e. do not perform openconnection.close()

def createTableWithSameSchema(inputTable, outputTable, openconnection):
    try:
        with openconnection.cursor() as cur:
            cur.execute("select count(*) from information_schema.tables where table_name ilike '%s'" % inputTable)
            count = cur.fetchone()[0]
            if(count == 0):
                print('%s table does not exist' % inputTable)
                return
            cur.execute('DROP TABLE IF EXISTS {0}'.format(outputTable))
            cur.execute('CREATE TABLE IF NOT EXISTS {0} (LIKE {1})'.format(outputTable, inputTable))
            openconnection.commit()
            print('Table [{0}] created'.format(outputTable))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def parallelRangeSort(threadName, InputTable, SortingColumnName, RangeOutputTable, rangeIndex, rangeMin, rangeMax, openconnection):
    try:
        print('{0} running'.format(threadName))
        with openconnection.cursor() as cur:
            createTableWithSameSchema(InputTable, RangeOutputTable, openconnection)
            if(rangeIndex == 0):
                query = "insert into {0} (select * from {1} where {2} >= {3} and {2} <= {4} ORDER BY {2})".format(RangeOutputTable, InputTable, SortingColumnName, rangeMin, rangeMax)
            else:
                query = "insert into {0} (select * from {1} where {2} > {3} and {2} <= {4} ORDER BY {2})".format(RangeOutputTable, InputTable, SortingColumnName, rangeMin, rangeMax)
            cur.execute(query)
            openconnection.commit()
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()


def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    try:
        with openconnection.cursor() as cur:
            cur.execute("select count(*) from information_schema.tables where table_name ilike '%s'" % InputTable)
            count = cur.fetchone()[0]

            if (count == 0):
                print('%s table does not exist' % InputTable)
                return

            # getting min and max values
            cur.execute('select min({0}), max({0}) from {1}'.format(SortingColumnName, InputTable))
            minValue, maxValue = cur.fetchone()
            numberOfIntervals = 5
            increment = (maxValue - minValue) / numberOfIntervals

            # range partitioning and sorting using threads

            threads = [None] * numberOfIntervals
            rangePartitionPrefix = 'range_part_'
            lowerBound = upperBound = minValue
            for i in range(numberOfIntervals):
                lowerBound = upperBound
                upperBound = (lowerBound + increment) if (i < numberOfIntervals - 1) else maxValue

                rangePartitionTable = rangePartitionPrefix + str(i)
                threads[i] = threading.Thread(target=parallelRangeSort,
                                              args=('Thread-' + str(i), InputTable, SortingColumnName,
                                                    rangePartitionTable, i, lowerBound, upperBound, openconnection))
                threads[i].start()

            # combining data
            createTableWithSameSchema(InputTable, OutputTable, openconnection)
            for i in range(numberOfIntervals):
                rangePartitionTable = rangePartitionPrefix + str(i)
                threads[i].join()
                cur.execute('insert into {0} select * from {1}'.format(OutputTable, rangePartitionTable))
                print('Output Table [{0}] loaded with data from {1}'.format(OutputTable, rangePartitionTable))

            openconnection.commit()
            cur.execute('select count(*) from {0}'.format(OutputTable))
            print('Count of rows in the output table : ' + str(cur.fetchone()[0]))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()


def sort_merge(ThreadName, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, MergeOutputTable, rangeIndex,
               rangeMin, rangeMax, openconnection):
    try:
        print('{0} running'.format(ThreadName))
        with openconnection.cursor() as cur:

            sortedTable1 = 'table1_range_sort_' + str(rangeIndex)
            sortedTable2 = 'table2_range_sort_' + str(rangeIndex)

            createTableWithSameSchema(InputTable1, sortedTable1, openconnection)
            createTableWithSameSchema(InputTable2, sortedTable2, openconnection)

            if (rangeIndex == 0):
                cur.execute("insert into {0} (select * from {1} where {2} >= {3} and {2} <= {4} ORDER BY {2})".format(
                    sortedTable1, InputTable1, Table1JoinColumn, rangeMin, rangeMax))
                cur.execute("insert into {0} (select * from {1} where {2} >= {3} and {2} <= {4} ORDER BY {2})".format(
                    sortedTable2, InputTable2, Table2JoinColumn, rangeMin, rangeMax))
            else:
                cur.execute("insert into {0} (select * from {1} where {2} > {3} and {2} <= {4} ORDER BY {2})".format(
                    sortedTable1, InputTable1, Table1JoinColumn, rangeMin, rangeMax))
                cur.execute("insert into {0} (select * from {1} where {2} > {3} and {2} <= {4} ORDER BY {2})".format(
                    sortedTable2, InputTable2, Table2JoinColumn, rangeMin, rangeMax))

            # merge
            cur.execute('DROP TABLE IF EXISTS ' + MergeOutputTable)
            cur.execute('CREATE TABLE {0} AS SELECT * FROM {1} a INNER JOIN {2} b ON a.{3} = b.{4}'.format(
                MergeOutputTable, sortedTable1, sortedTable2, Table1JoinColumn, Table2JoinColumn))

            openconnection.commit()
            print('\nTable [' + MergeOutputTable + '] created and loaded with joined data')
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        with openconnection.cursor() as cur:

            # getting min and max values
            cur.execute('select min({0}), max({0}) from {1}'.format(Table1JoinColumn, InputTable1))
            min1, max1 = cur.fetchone()
            cur.execute('select min({0}), max({0}) from {1}'.format(Table2JoinColumn, InputTable2))
            min2, max2 = cur.fetchone()
            minValue, maxValue = min(min1, min2), max(max1, max2)

            # sort-merge using threads
            numberOfIntervals = 5
            increment = (minValue + maxValue) / numberOfIntervals

            threads = [None] * numberOfIntervals
            mergeOutputTablePrefix = 'merge_'
            lowerBound = upperBound = minValue

            for i in range(numberOfIntervals):
                lowerBound = upperBound
                upperBound = (lowerBound + increment) if i < numberOfIntervals - 1 else maxValue

                mergeOutputTable = mergeOutputTablePrefix + str(i)
                threads[i] = threading.Thread(target=sort_merge, args=('Thread-' + str(i), InputTable1, InputTable2,
                                                                       Table1JoinColumn, Table2JoinColumn,
                                                                       mergeOutputTable,
                                                                       i, lowerBound, upperBound, openconnection))
                threads[i].start()

            # combining data
            cur.execute('DROP TABLE IF EXISTS ' + OutputTable)
            threads[0].join()
            cur.execute('CREATE TABLE {0} AS SELECT * FROM {1}'.format(OutputTable, mergeOutputTablePrefix + '0'))

            for i in range(1, numberOfIntervals):
                threads[i].join()
                cur.execute('INSERT INTO {0} SELECT * FROM {1}'.format(OutputTable, mergeOutputTablePrefix + str(i)))

            openconnection.commit()

            print('Output Table [{0}] loaded with data successfully'.format(OutputTable))
            cur.execute('select count(*) from {0}'.format(OutputTable))
            print('Count of rows in the output table : ' + str(cur.fetchone()[0]))

    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

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


