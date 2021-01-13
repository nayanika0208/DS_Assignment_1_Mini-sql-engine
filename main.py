import os
import sys
import copy
import sqlparse
import re
import csv
from functools import reduce

META = './files/metadata.txt'
tables_list = {}            #dictionary of table name to list of columns it has
tables_needed = {}

AGG = [ 'max', 'sum', 'avg', 'min']

cartesianTable = []             #joined table of cartesian product of all rows of tables being used in the query
                
tables = {}                     #dictionary of table name to Table object with name, cols and data in cols
                #list of all columns in order of cartesian product for all tables


class Table:
    def __init__(self, name, cols, data):
        self.name = name
        self.columns = cols
        self.data = data

def read_metadata(file):
    
    table_name = ""
    start = 0
 
    try:
        my_file = open(file, 'r')
        lines = my_file.readlines()
    except IOError:
        sys.stderr.write("Error.\n")
        exit(-1)

    for line in lines:
        line = line.strip()     # Remove white spaces
        if line == '<begin_table>':
            start = 1
            continue
        if start == 1:
            table_name = line
            tables_list[table_name] = []
            start = 0
        elif line != "<end_table>":
            tables_list[table_name].append(line)

    print ("table list",tables_list)      


def get_data_columns(table_name):
   

    file = './files/' + table_name + '.csv'
    data = []
    try:
        src = open(file, "rt")
    except IOError:
        sys.stderr.write("Error: No corresponding .csv file found.\n")
        quit(-1)

    info = csv.reader(src)
    for row in info:
        row=[int(i) for i in row]
        # print(type(i))
        data.append(row)
    src.close()
    return data


def check_col(col):
    global tables
    table_names=list(tables.keys())
    print(" table names ",table_names)
    found=0
    for i in table_names:
        if col in tables[i].columns:
            found=1
    if not found :
        print ("Error: Column " + str(col) + " not found in the given table.")
        sys.exit(0)
            


def fullJoin():
    global tables
    global cartesianTable
  
    table_l = list(tables.keys())

    cartesianTable = copy.deepcopy(tables[table_l[0]].data)

    for i in range(1, len(table_l)):
        cartesianTable = [[*row1, *row2] for row1 in cartesianTable for row2 in tables[table_l[i]].data]
    
    print("cartesianTable\n")
    for row in cartesianTable:
        print((str(row)))
  

def findColNum(arg,final_cols):
    global tables

    k = list(tables.keys())
      
    for i in range(len(final_cols)):
        if final_cols[i] == str(arg):
            colInd = i
            return colInd
  

def getTablename(col):
    global tables

    table_names = list(tables.keys())
    for i in table_names:
        if col in tables[i].columns:
            return str(i)

def checkAggregate(query_list):
    print(" query list ",query_list)
    #aggregate only on single column
    temp = -1
    agg = -1
    for i in range(len(query_list)):
        x = query_list[i]
        temp = x.find("(")
        if temp > -1:
            agg = x[0:temp]
            agg = agg.lower()
            if agg == "max":
                agg = 1
            elif agg == "min":
                agg = 2
            elif agg == "avg":
                agg = 3
            elif agg == "sum":
                agg = 4   
            else:
                agg = -1
            query_list[i] = x[temp+1:-1]
    
    print(query_list)        
    return agg, query_list


def projectColumns(columns_to_display, table_cols, distinct, redundant,final_cols):
    global cartesianTable
    global tables
    global tables_list

    col_ind = []
    col_name = []

    
    columns_to_display = columns_to_display.replace(",", " ")
    columns_to_display = columns_to_display.split()
    # if(len(columns_to_display) >2) :
    #     columns_to_display=columns_to_display[:-1]

    
    print ("query colums in project colms",columns_to_display)

    agg,  columns_to_display  = checkAggregate(columns_to_display)

    for i in range(len(columns_to_display)):
        check_col(columns_to_display[i])
        temp = findColNum(columns_to_display[i],final_cols)
        print("temp ",temp)

        if temp in redundant:
            # print (redundant, temp)
            continue
        col_ind.append(temp)
        # print (col_ind)
        col_name.append(final_cols[col_ind[-1]])
        # print("col name ",col_name)
        # print("col index ",col_ind)
    if distinct:
        disp_set = set()

        for i in range(len(cartesianTable)):
            row = ""
            if i in table_cols:
                for j in range(len(col_ind)):
                    if j == len(col_ind) - 1:
                        row += str(cartesianTable[i][col_ind[j]])
                    else:
                        row += str(cartesianTable[i][col_ind[j]]) + ", "
            if row != "":
                disp_set.add(row)
    elif len(columns_to_display) > 1 :
        disp_set = []

        for i in range(len(cartesianTable)):
            row = ""
            if i in table_cols:
                for j in range(len(col_ind)):
                    if j == len(col_ind) - 1:
                        row += str(cartesianTable[i][col_ind[j]])
                    else:
                        row += str(cartesianTable[i][col_ind[j]]) + ", "
            if row != "":
                disp_set.append(row)

                 
    else:
        disp_set = []
        k = list(tables.keys())
        # tab = col_name[0].split(".")[0]
        col = col_name[0]
        tab=getTablename(col)

        data = tables[tab].data
        ind = 0
        for i in range(len(tables_list[tab])):
            if tables_list[tab][i] == str(col):
                ind = i

        for i in range(len(data)):
            disp_set.append(str(data[i][ind]))

    if agg == -1:
        print (str(col_name)[1:-1])
        for i in disp_set:
            print (i)

    elif agg == 1:
        print ("max(" + str(col_name)[1:-1] + ")")
        print (max(disp_set))
    elif agg == 2:
        print ("min(" + str(col_name)[1:-1] + ")")
        print (min(disp_set))
    elif agg == 3:
        print ("avg(" + str(col_name)[1:-1] + ")")
        print (reduce(lambda x,y:float(x) + float(y), disp_set)/float(len(disp_set)))
    elif agg == 4:
        print ("sum(" + str(col_name)[1:-1] + ")")
        print (reduce(lambda x,y:float(x) + float(y), disp_set))
    

def selectQuery(querybits):
    global tables_list
    global tables 
    global cartesianTable

    dist=0

    if querybits[1].lower() in ["distinct"]:
        curr_tables = querybits[4]
        columns_to_display= querybits[2]
        dist = 1
    else:
        curr_tables = querybits[3]
        columns_to_display= querybits[1]
       

    curr_tables = re.findall("[\'\w]+", curr_tables)
    print ("curr_tables ",curr_tables)

    #getting the tables data from csv files to store in program

    count_tables=len(curr_tables)
    for i in range(count_tables):
        if curr_tables[i] not in tables_list:
            sys.stderr.write("Error: Table name is not valid.\n")
            quit(-1)
        data=get_data_columns(curr_tables[i])
        tables[str(curr_tables[i])] = Table(str(curr_tables[i]), tables_list[str(curr_tables[i])], data)
        # print("my table data",tables[str(curr_tables[i])].data)
       

   
    #create big cartesian table
    final_cols=[]

    table_l = list(tables.keys())
    for i in range(len(table_l)):
        tab = str(table_l[i])
        for j in range(len(tables_list[tab])):
            # colname = tab + "." + str(tables_list[tab][j])
            colname=str(tables_list[tab][j])
            final_cols.append(colname)
    print("final  ",final_cols)
           
    fullJoin()
    

    res = [x for x in range(len(cartesianTable))]
    print('result ',res)
    redundant = set()
    # #check for where conditions
    # last = querybits[-1]
    # temp = last.split()
    # # if (temp[0] in  ["where", "WHERE"]):
    # #     res, redundant = whereQuery(querybits)

    #project columns
    if columns_to_display == "*":
        columns_to_display = ""
        k = list(tables_list.keys())
        for x in k:
            if x in curr_tables:
                for j in range(len(tables_list[x])):
                    # columns_to_display += (str(x) + "." + str(tables_list[x][j]) + ",")
                    columns_to_display += ( str(tables_list[x][j]) + ",")


    print (columns_to_display)
    projectColumns(columns_to_display, res, dist, redundant,final_cols)


def processQuery(raw_query):
    queries = sqlparse.split(raw_query)
    print("queries ",queries)
    for query in queries:
        if query[-1] != ';':
            print ("Syntax error: SQL command should end with semi colon")
            return
        query = query[:-1]
        # q = q.lower()
        parsed = sqlparse.parse(query)[0]
        parsed = parsed.tokens
        #print ("parsed ",parsed)
        #print("sqlparse.sql.IdentifierList(parsed) ",sqlparse.sql.IdentifierList(parsed))
        identifier_list = sqlparse.sql.IdentifierList(parsed)
        identifier_list =identifier_list.get_identifiers()
        # print("identifier list" ,type( identifier_list))
        querybits = []
        for cmd in identifier_list:
            querybits.append(str(cmd))

        if querybits[0].lower() in ["select"]:           
            print("querybits ",querybits)            
            selectQuery(querybits)
        else:
            print ("Incorrect Query type")
            continue

    


       
def main():
    if len(sys.argv) != 2:
        print ("Incorrect number of arguments passed")
        return
    query = sys.argv[1]
    read_metadata(META)
    processQuery(query)


# if ( __name__ == "__main__"):
#     main()           

main()    