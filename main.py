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

AGG = [ 'max', 'min', 'avg','sum']

cartesianTable = []             #joined table of cartesian product of all rows of tables being used in the query
                
tables = {}                     #dictionary of table name to Table object with name, cols and data in cols
                #list of all columns in order of cartesian product for all tables
temp_col_name=[]

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
    # print(" table names ",table_names)
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
    
    # print("cartesianTable\n")
    # for row in cartesianTable:
    #     print((str(row)))
  

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

def checkAggregate(agg_part):
    global AGG
    # print(" query list ",agg_part)
   
    col_start = -1
    agg = -1
    col_name = re.search(r"\(([A-Za-z0-9_]+)\)", agg_part[0])
    # if(agg_part > 1)
    # print("col_name  ",agg_part)

    for i in range(len(AGG)):
        if AGG[i] in agg_part[0].lower():
            agg=i+1

    if(agg !=-1) :
        agg_part[0]=col_name.group(1) 
            
     
    # print("agg ",agg)      
          
    return agg, agg_part

def operator_evaluate(operand1,operand2,identifier1,identifier2,row_no,operator):
    global cartesianTable

    if identifier1 == -1:     
        if identifier2 == -1:
            pass       #argument 1 and 2 are numbers
        else:         
            operand2 = cartesianTable[row_no][identifier2]
    else:              
        operand1 = cartesianTable[row_no][identifier1]
        if identifier2 == -1:
            pass
        else:
            operand2 = cartesianTable[row_no][identifier2]  #argument 1 and 2  col

    if operator == "=":
        return operand1 == operand2
    elif operator == ">":
        return operand1 > operand2
    elif operator == ">=":
        return operand1 >= operand2
    elif operator == "<":
        return operand1 < operand2
    elif operator == "<=":
        return operand1 <= operand2        



            
def preEvaluate(cond,final_cols):
    global cartesianTable
    print(" cond " ,cond)
    operator = cond[1]

    ind1,ind2 = -1,-1
    arg = cond[0]
    no = 0
    try:
        arg = int(arg)
    except:
        check_col(arg)
        ind1 = findColNum(arg,final_cols)
        print("index 1" ,ind1)

    arg2 = cond[2]
    try:
        arg2 = int(arg2)
    except:
        check_col(arg2)
        ind2 = findColNum(arg2,final_cols)
        print("index 2" ,ind2)


    finalData = []
    finalRows = []
    for i in range(len(cartesianTable)):
        bool_var = operator_evaluate(arg, arg2, ind1,ind2, i,operator)
        # print (bool_var)
        if bool_var:
            finalData.append(cartesianTable[i])
            finalRows.append(i)
    print (finalRows)
    return finalRows


def checkJoin(cond,final_cols):
    print("\n\n ")
    print("check join condition ",cond)
    redundant = []
    operator = cond[1]
    ind1,ind2 = -1,-1
    arg = cond[0]
    try:
        arg = int(arg)
    except:
        ind1 = findColNum(arg,final_cols)

    arg2 = cond[2]
    try:
        arg2 = int(arg2)
    except:
        ind2 = findColNum(arg2,final_cols)
        if operator == "=":
            redundant.append(ind2)
            # print (ind2 )
    print("redundant",redundant)        
    return redundant




def get_condition_array(condition,index):

    beg = 0
    condition_array=[]

    if index != 0 :

        temp_c=condition[slice(beg,index)]
         # print(" temp c" ,"".join(temp_c))
        condition_array.append("".join(temp_c))

        beg = index + 1
        temp_c=condition[slice(beg,len(condition))]
        condition_array.append("".join(temp_c))

    else:
        temp_c=condition
        print(" temp c" ,"".join(temp_c))
        condition_array.append("".join(temp_c))

       
           
           
    

    operator_array=[">=","<=",">","<","="]

    for i in range(len(condition_array)):
        temp = [0,0,0]

        for op in operator_array:
            print(" c-op ",op)
            if condition_array[i].find(op) !=-1 :
                cols=condition_array[i].replace(op,",").split(",")
                # print("cols ",cols)
                temp[0],temp[1],temp[2]=cols[0],op,cols[1]
                condition_array[i]=temp;
                break


       
            
    # print("condition_array[i] ",condition_array[i])
    return condition_array
   
 
def whereQuery(condition,final_cols):
    #split on basis of and and or conditions
    
    condition = condition.split()[1:]
    
    #validate condirion 

    print("condition ",condition)

   
    condition_array = []
    two_cond=0

    for c in range(len(condition)):
        if condition[c].lower() == "or" :
            two_cond = 1
            condition_array=get_condition_array(condition,c)

            print("condition_array ",condition_array)

            res1 = set(preEvaluate(condition_array[0],final_cols))

            res2 = set(preEvaluate(condition_array[1],final_cols))
            print("res1 " ,res1)
            res = res1 | res2
            print("res2 " ,res2)


            red1 = set(checkJoin(condition_array[0],final_cols))
            red2 = set(checkJoin(condition_array[1],final_cols))
            print("red1 " ,red1)
            print("red2 " ,red2)

            red = red1 | red2
            break


        elif condition[c] == "and" or condition[c] == "AND":
            two_cond = 1
            condition_array=get_condition_array(condition,c)

            print("condition_array ",condition_array)
            

            res1 = set(preEvaluate(condition_array[0],final_cols))
            res2 = set(preEvaluate(condition_array[1],final_cols))
            res = res1 & res2

            red1 = set(checkJoin(condition_array[0],final_cols))
            red2 = set(checkJoin(condition_array[1],final_cols))
            red = red1 | red2
            break

    if two_cond == 0 :

        condition_array=get_condition_array(condition,0)
        print("c_aarray ",condition_array)

      
        res = set(preEvaluate(condition_array[0],final_cols))
        red = checkJoin(condition_array[0],final_cols)
           
        
    return res,red



def projectColumns(columns_to_display, table_cols, distinct, redundant,final_cols):
    global cartesianTable
    global tables
    global tables_list
    global temp_col_name

    index_of_col = []
    name_of_col = []
    temp_name_of_col=[]

    
    columns_to_display = columns_to_display.replace(",", " ")
    columns_to_display = columns_to_display.split()
 
    # print ("query colums in project colms",columns_to_display)

    agg,  columns_to_display  = checkAggregate(columns_to_display)

    for i in range(len(columns_to_display)):
        check_col(columns_to_display[i])
        temp = findColNum(columns_to_display[i],final_cols)
        # print("temp ",temp)

        if temp in redundant:
            # print (redundant, temp)
            continue
        index_of_col.append(temp)
        # print (index_of_col)
        name_of_col.append(final_cols[index_of_col[-1]])
        temp_name_of_col.append(temp_col_name[index_of_col[-1]])
        # print("col name ",name_of_col)
        # print("col index ",index_of_col)
    if distinct:
        diaplay_data = set()

        for i in range(len(cartesianTable)):
            row = ""
            if i in table_cols:
                for j in range(len(index_of_col)):
                    if j == len(index_of_col) - 1:
                        row += str(cartesianTable[i][index_of_col[j]])
                    else:
                        row += str(cartesianTable[i][index_of_col[j]]) + ", "
            if row != "":
                diaplay_data.add(row)
    else:
        diaplay_data = []

        for i in range(len(cartesianTable)):
            row = ""
            if i in table_cols:
                for j in range(len(index_of_col)):
                    if j == len(index_of_col) - 1:
                        row += str(cartesianTable[i][index_of_col[j]])
                    else:
                        row += str(cartesianTable[i][index_of_col[j]]) + ", "
            if row != "":
                diaplay_data.append(row)

                 
    # else:
    #     diaplay_data = []
    #     k = list(tables.keys())
    #     # tab = name_of_col[0].split(".")[0]
    #     col = name_of_col[0]
    #     tab=getTablename(col)

    #     data = tables[tab].data
    #     ind = 0
    #     for i in range(len(tables_list[tab])):
    #         if tables_list[tab][i] == str(col):
    #             ind = i

    #     for i in range(len(data)):
    #         diaplay_data.append(str(data[i][ind]))

    # print(" display data ",diaplay_data)
    if agg == -1:
        print (str(temp_name_of_col)[1:-1])
        for i in diaplay_data:
            print (i)


    elif agg == 1:
        print ("max(" + str(name_of_col)[1:-1] + ")")
        print (max(diaplay_data))
    elif agg == 2:
        print ("min(" + str(name_of_col)[1:-1] + ")")
        print (min(diaplay_data))
    elif agg == 3:
        print ("avg(" + str(name_of_col)[1:-1] + ")")
        print (reduce(lambda x,y:float(x) + float(y), diaplay_data)/float(len(diaplay_data)))
    elif agg == 4:
        print ("sum(" + str(name_of_col)[1:-1] + ")")
        print (reduce(lambda x,y:float(x) + float(y), diaplay_data))
    

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
    # print ("curr_tables ",curr_tables)

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
    global temp_col_name
    table_l = list(tables.keys())
    for i in range(len(table_l)):
        tab = str(table_l[i])
        for j in range(len(tables_list[tab])):
            colname = tab + "." + str(tables_list[tab][j])
            temp_col_name.append(colname)
            colname=str(tables_list[tab][j])
            final_cols.append(colname)
    # print("final  ",final_cols)
           
    fullJoin()
    

    res = [x for x in range(len(cartesianTable))]
    # print('result ',res)
    redundant = set()
   
    #first check for where query
    for i in querybits:
        if  i.split()[0].lower() in ["where"] :
             res, redundant = whereQuery(i,final_cols)

     

    print(" results ",res)

    #then check for group query

    

    #project columns
    if columns_to_display == "*":
        columns_to_display = ""
        k = list(tables_list.keys())
        for x in k:
            if x in curr_tables:
                for j in range(len(tables_list[x])):
                    # columns_to_display += (str(x) + "." + str(tables_list[x][j]) + ",")
                    columns_to_display += ( str(tables_list[x][j]) + ",")


    # print (columns_to_display)
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
            # print("querybits ",querybits)            
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