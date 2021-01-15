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

AGG = [ 'max', 'min', 'avg','sum' ,'count']

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
    agg_data={}
    print("\n\n")
    for i in range(len(agg_part)):
        col_name = re.search(r"\(([A-Za-z0-9_]+)\)", agg_part[i])
             # if(agg_part > 1)
        print("col_name  ",col_name)

        for j in range(len(AGG)):
            if AGG[j] in agg_part[i].lower():
                agg=1
                agg_data[i]=AGG[j]

        if(agg !=-1) :
            agg_part[i]=col_name.group(1) 


   
    
    print("agg_ data ",agg_data)
    print("agg_part ",agg_part)       
    print("\n\n")
    # print("agg ",agg)      
          
    return agg,agg_data, agg_part

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


def get_redundant_rows(cond,final_cols):
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
   
# def group_by_function() :


 
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


            red1 = set(get_redundant_rows(condition_array[0],final_cols))
            red2 = set(get_redundant_rows(condition_array[1],final_cols))
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

            red1 = set(get_redundant_rows(condition_array[0],final_cols))
            red2 = set(get_redundant_rows(condition_array[1],final_cols))
            red = red1 | red2
            break

    if two_cond == 0 :

        condition_array=get_condition_array(condition,0)
        print("c_aarray ",condition_array)

      
        res = set(preEvaluate(condition_array[0],final_cols))
        red = get_redundant_rows(condition_array[0],final_cols)
           
        
    return res,red


def evaluate_agg(agg_func,data_to_perform) :
    if agg_func.lower() == "max" :
        return(max(data_to_perform))  
    elif agg_func.lower() == "min" :
        return min(data_to_perform)

    elif agg_func.lower() =="avg":
        return reduce(lambda x,y:float(x) + float(y), data_to_perform)/float(len(data_to_perform))

    elif agg_func.lower() =="count":    
        return (len(data_to_perform))

    elif agg_func.lower() =="sum":
        return (reduce(lambda x,y:float(x) + float(y), data_to_perform))
    
            




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

    agg, agg_data, columns_to_display  = checkAggregate(columns_to_display)

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
    print("name of col ",name_of_col)    
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
    print("\n\n")
    print(" display data ",diaplay_data)
    if agg == -1:
        print (str(temp_name_of_col)[1:-1])
        for i in diaplay_data:
            print (i)
    else :
        agg_d_keys=list(agg_data.keys())
        print("aggr data keys ",agg_d_keys)
        data_to_perform={}
        for k in agg_d_keys :
            data_to_perform[k]=[]
        for i in diaplay_data : 
            for j in agg_d_keys :
                temp=i.split(",")
                # print("temp ",temp )
                # print("i ",i,"   j ",j)

                # print("i[j]",temp[int(j)])
                data_to_perform[j].append(int(temp[int(j)]))

        print("\n\n")
        # print("data to perform ",data_to_perform)   
        # evaluate_agg(agg_data,data_to_perform,name_of_col)


        for i in agg_data.keys():
            if agg_data[i].lower() == "max" :
                print ("max(" + str(name_of_col[i]) + ")")
                print (max(data_to_perform[i]))  
            elif agg_data[i].lower() == "min" :
                print ("min(" + str(name_of_col[i])+ ")")
                print (min(data_to_perform[i]))

            elif agg_data[i].lower() =="avg":
                print ("avg(" + str(name_of_col[i]) + ")")
                print (reduce(lambda x,y:float(x) + float(y), data_to_perform[i])/float(len(data_to_perform[i])))

            elif agg_data[i].lower() =="count":    
                print ("count(" + str(name_of_col[i]) + ")")
                print (len(data_to_perform[i])+1)

            elif agg_data[i].lower() =="sum":
                print ("sum(" + str(name_of_col[i]) + ")")
                print (reduce(lambda x,y:float(x) + float(y), data_to_perform[i]))
            


    # elif agg == 1:
    #     print ("max(" + str(name_of_col)[1:-1] + ")")
    #     print (max(diaplay_data))
    # elif agg == 2:
    #     print ("min(" + str(name_of_col)[1:-1] + ")")
    #     print (min(diaplay_data))
    # elif agg == 3:
    #     print ("avg(" + str(name_of_col)[1:-1] + ")")
    #     print (reduce(lambda x,y:float(x) + float(y), diaplay_data)/float(len(diaplay_data)))
    # elif agg == 4:
    #     print ("sum(" + str(name_of_col)[1:-1] + ")")
    #     print (reduce(lambda x,y:float(x) + float(y), diaplay_data))
    
def get_data_to_perform(rows,col) :
    data=[]
    global cartesianTable

    for i in rows:
        data.append(cartesianTable[i][col])

    return data    



def groupQuery(grp_by_col,rows,redundant,final_cols,columns_to_display) :
    global  cartesianTable
    
    print("grou_by col ",grp_by_col)
    grp_by_col_num=findColNum(grp_by_col,final_cols)
    print(" grp_by_col_num ",grp_by_col_num)

    group_dict={} #dictionary of grp_by_col data to rows 

    data=[]
    for i in rows:
    
            data.append(cartesianTable[i][grp_by_col_num])

    print(" data " ,data)    
    data=set(data)

    for i in data :
        group_dict[i]=[]
    
    for i in rows:
        group_dict[cartesianTable[i][grp_by_col_num]].append(i)

    print(" group_dict ",group_dict)    
    columns_to_display = columns_to_display.replace(",", " ")
    columns_to_display = columns_to_display.split()
    agg, agg_data, columns_to_display  = checkAggregate(columns_to_display)
    
    print(" columns_to_display in group query ",columns_to_display )
    print("agg data in grp query ",agg_data)

   
    
    for i in group_dict.keys() :
        print_data=str(i)

        for j in agg_data.keys():
            t=findColNum(columns_to_display[j],final_cols)
            # print(" t ",t)
            data_to_perform=get_data_to_perform(group_dict[i],t)
            # print("data to perform  ",data_to_perform)
            ans=evaluate_agg(agg_data[j],data_to_perform)
            # print(" ans ",ans)
            print_data+= " " +str(ans)
        print("print_data ",print_data)    





        

    #check error columns to dsplay and grp_by_col

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
    for i in range(len(querybits)):
        if  querybits[i].split()[0].lower() in ["where"] :
             res, redundant = whereQuery(querybits[i],final_cols)

        if querybits[i] == "group by" :
            # print("querybits [i ] ",querybits[i])
            groupby=1
            try :
                # print(" querybits [i+ 1]",querybits[i+1])
                groupQuery(querybits[i+1],res,redundant,final_cols,columns_to_display)
                # print("no eroorrrrrrrrr")
            except :
                print(" error in quryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
                     


     


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
    if(groupby != 1) :

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