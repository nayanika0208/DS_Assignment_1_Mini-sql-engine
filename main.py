import sqlparse
import re
import csv
import os
import sys
import copy

from functools import reduce




class Table:
    def __init__(self, name,data,cols,):
        self.name = name
        self.columns = cols
        self.data = data


META = './metadata.txt'
tables_list = {}            #dictionary of table name to list of columns it has
tables_needed = {}

tables = {}                     #dictionary of table name to Table object with name, cols and data in cols
                #list of all columns in order of cartesian product for all tables
temp_col_name=[]

AGG = [ 'max', 'min', 'avg','sum' ,'count','average']

cartesian_Table = []             #joined table of cartesian product of all rows of tables being used in the query
                



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

    # print ("table list",tables_list)      


def get_data_columns(table_name):
   

    file =  table_name + '.csv'
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
            


def full_Join():
    global tables
    global cartesian_Table
  
    table_list = list(tables.keys())

    cartesian_Table = copy.deepcopy(tables[table_list[0]].data)
    len_t=len(table_list)

    for i in range(1,len_t ):
        cartesian_Table = [[*row1, *row2] for row1 in cartesian_Table for row2 in tables[table_list[i]].data]
    
    # print("cartesian_Table\n")
    # for row in cartesian_Table:
    #     print((str(row)))
  

def findColNum(arg,final_cols):
    global tables

    k = list(tables.keys())
    # print("k ",k)
      
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
    global temp_col_name
    global AGG
    # print(" query list ",agg_part)
   
    col_start = -1
    agg = -1
    agg_data={}
    temp=-1
    # print("\n\n")
    for i in range(len(agg_part)):
        col_name = re.search(r"\(([A-Za-z0-9\*_]+)\)", agg_part[i])

             # if(agg_part > 1)
       
        # print("col_name 1 ",col_name)
        for j in range(len(AGG)):
            if AGG[j] in agg_part[i].lower():
                agg=1
                agg_data[i]=AGG[j]
                temp=1
        try:
            if(temp !=-1) :
                agg_part[i]=col_name.group(1) 
            temp=-1  

        except :
                col_name=agg_part[i]
               
                


    # print("agg_part ",agg_part)  
    # print("final_cols ",temp_col_name)
    
    # print("agg_ data ",agg_data)
          
    # print("\n\n")
    # print("agg ",agg)  

    


          
    return agg,agg_data, agg_part

def operator_evaluate(operand1,operand2,identifier1,identifier2,row_no,operator,final_cols):
    
    global cartesian_Table
    if identifier1 == -1 and identifier2 == -1 :
        pass
    elif identifier1 == -1 and identifier2 != -1 :
        operand2 = cartesian_Table[row_no][identifier2]
    elif identifier1 != -1 and identifier2 == -1 :
        operand1 = cartesian_Table[row_no][identifier1]
    elif identifier1 !=-1 and identifier2 != -1 :
        operand1 = cartesian_Table[row_no][identifier1]
        operand2 = cartesian_Table[row_no][identifier2]  


 

        
    # #print(final_cols)
    # if identifier1 == -1:     
    #     if identifier2 == -1:
    #         pass       #argument 1 and 2 are numbers
    #     else:         
    #         operand2 = cartesian_Table[row_no][identifier2]
    # else:              
    #     operand1 = cartesian_Table[row_no][identifier1]
    #     if identifier2 == -1:
    #         pass
    #     else:
    #         operand2 = cartesian_Table[row_no][identifier2]  #argument 1 and 2  col

    if operator == ">=":
        ans=(operand1 >= operand2)
        return ans
    elif operator == "==":
        ans= (operand1 == operand2)
        return ans
    elif operator == "<=":
        ans= (operand1 <= operand2)
        return ans
    elif operator == "<":
        ans= (operand1 < operand2)
        return ans
    elif operator == ">":
        ans= (operand1 > operand2)
        return ans  

              



            
def get_rows_by_condition(cond,final_cols):
    global cartesian_Table
    # print(" cond " ,cond)
    num = 0
    operator = cond[1]
    arg = cond[0]
    ind1,ind2 = -1,-1
    arg2 = cond[2]
    finalData = []
    final_rows = []
    
    try:
        arg = int(arg)
    except:
        check_col(arg)
        ind1 = findColNum(arg,final_cols)
        # print("arg ind1 ",arg,"  ",ind1)
        # print("index 1" ,ind1)

    
    try:
        arg2 = int(arg2)
    except:
        check_col(arg2)
        ind2 = findColNum(arg2,final_cols)
        # print("index 2" ,ind2)
        # print("arg2 ind2 ",arg2,"  ",ind2)


   
    for i in range(len(cartesian_Table)):
        
        # print (bool_var)
        if operator_evaluate(arg, arg2, ind1,ind2, i,operator,final_cols):
            finalData.append(cartesian_Table[i])
            final_rows.append(i)
    # print (final_rows)
    return final_rows


def get_redundant_rows(cond,final_cols):
    # print("\n\n ")
    # print("check join condition ",cond)
   
    operator = cond[1]
    arg2 = cond[2]
    ind1,ind2 = -1,-1
    arg = cond[0]
    redundant = []
    try:
        arg = int(arg)
    except:
        ind1 = findColNum(arg,final_cols)

    
    try:
        arg2 = int(arg2)
    except:
        ind2 = findColNum(arg2,final_cols)
        if operator == "=":
            redundant.append(ind2)
            # print (ind2 )
    # print("redundant",redundant)        
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
        # print(" temp c" ,"".join(temp_c))
        condition_array.append("".join(temp_c))

       
           
           
    

    operator_array=[">=","<=",">","<","="]

    for i in range(len(condition_array)):
        temp = [0,0,0]

        for op in operator_array:
            # print(" c-op ",op)
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

    # print("condition ",condition)

   
    condition_array = []
    two_cond=0

    for c in range(len(condition)):
        if condition[c].lower() == "or" :
            two_cond = 1
            condition_array=get_condition_array(condition,c)

            # print("condition_array ",condition_array)

            res1 = set(get_rows_by_condition(condition_array[0],final_cols))

            res2 = set(get_rows_by_condition(condition_array[1],final_cols))
            # print("res1 " ,res1)
            res = res1 | res2
            # print("res2 " ,res2)


            red1 = set(get_redundant_rows(condition_array[0],final_cols))
            red2 = set(get_redundant_rows(condition_array[1],final_cols))
            # print("red1 " ,red1)
            # print("red2 " ,red2)

            red = red1 | red2
            break


        elif condition[c] == "and" or condition[c] == "AND":
            two_cond = 1
            condition_array=get_condition_array(condition,c)

            # print("condition_array ",condition_array)
            

            res1 = set(get_rows_by_condition(condition_array[0],final_cols))
            res2 = set(get_rows_by_condition(condition_array[1],final_cols))
            res = res1 & res2

            red1 = set(get_redundant_rows(condition_array[0],final_cols))
            red2 = set(get_redundant_rows(condition_array[1],final_cols))
            red = red1 | red2
            break

    if two_cond == 0 :

        condition_array=get_condition_array(condition,0)
        # print("c_aarray ",condition_array)

      
        res = set(get_rows_by_condition(condition_array[0],final_cols))
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
    else :
        print("error")
        sys.exit(0)
        
    
            




def column_projection(columns_to_display, table_cols, distinct, redundant,final_cols,orderby,orderbycol):
    global cartesian_Table
    global tables
    global tables_list
    global temp_col_name

    index_of_col = []
    name_of_col = []
    temp_name_of_col=[]

    
    columns_to_display = columns_to_display.replace(",", " ")
    columns_to_display = columns_to_display.split()

    temp_col=copy.deepcopy(columns_to_display)
 
    # print ("query colums in project colms",columns_to_display)

    # print("final_cols" ,final_cols)
    agg, agg_data, columns_to_display  = checkAggregate(columns_to_display)
    # for i in columns_to_display :
    #     if i not in 

    new_cart_table=[]
    for i in range(len(cartesian_Table)):
            if i in table_cols: 
                new_cart_table.append(cartesian_Table[i])

    cartesian_Table=copy.deepcopy(new_cart_table)            
    try :
        order_by_col_no = findColNum(orderbycol,final_cols)
        if orderby == -1 :
            cartesian_Table=sorted(cartesian_Table, key=lambda x: int(x[order_by_col_no]),reverse=True) 

        elif orderby==1 :
            cartesian_Table=sorted(cartesian_Table, key=lambda x: int(x[order_by_col_no])) 
    except :
        print("error in query")
        sys.exit(0)


             



    for i in range(len(columns_to_display)):
        # print("i ",columns_to_display[i])
        if columns_to_display[i] == "*" :
                
                temp = 0
                index_of_col.append(temp)
                # print (index_of_col)
                name_of_col.append("*")
                temp_name_of_col.append("*")

        
        else:
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
    # print("name of col ",name_of_col)    
    if distinct:
        diaplay_data  =[]
        diaplay_data_dup=set()


        for i in range(len(cartesian_Table)):
            row = []
            row_dup=""
            for j in range(len(index_of_col)):
                if j == len(index_of_col) - 1:
                    row_dup += str(cartesian_Table[i][index_of_col[j]])
                    row.append( cartesian_Table[i][index_of_col[j]]) 
                else:
                    row_dup+= str(cartesian_Table[i][index_of_col[j]]) + ", "
                    row.append( cartesian_Table[i][index_of_col[j]]) 
                    
                    
                    
                    
            if len(row)>0:
                if(row_dup not in diaplay_data_dup):
                    diaplay_data.append(row)
                    diaplay_data_dup.add(row_dup)

                
    else:
        diaplay_data = []

        for i in range(len(cartesian_Table)):
            row = []
            
            for j in range(len(index_of_col)):
                row.append( cartesian_Table[i][index_of_col[j]]) 
                    
                   
            if len(row)>0:
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
    # print("\n\n")
    # print(" display data ",diaplay_data)
    if agg == -1:
        # temp=str(temp_name_of_col)[1:-1]
        print(",".join(temp_name_of_col))

        for i in diaplay_data:
            i=[str(j) for j in i]
            temp_i=" ".join(i)
            print(",".join(i))
    else :

        # print(temp_col)

        for i in temp_col :
            # print("i ",i)
            found=0
            for j in AGG:
                if j in i.lower() :
                    found=1
            if found == 0 :
                print("Invalid Query")
                sys.exit(0)
                    



        agg_d_keys=list(agg_data.keys())
        # print("aggr data keys ",agg_d_keys)
        data_to_perform={}
        for k in agg_d_keys :
            data_to_perform[k]=[]
        for i in diaplay_data : 
            for j in agg_d_keys :
                # temp=i.split(",")
                # print("temp ",temp )
                # print("i ",i,"   j ",j)

                # print("i[j]",temp[int(j)])
                data_to_perform[j].append(int(i[int(j)]))

        # print("\n\n")
        # print("data to perform ",data_to_perform)   
        # evaluate_agg(agg_data,data_to_perform,name_of_col)

        print_data=[]
        for i in agg_data.keys():
            # print("columns_to_display[i]",columns_to_display[i])
            # if(columns_to_display[i] == "*") :
            print_data.append(str(agg_data[i])+"("+str(columns_to_display[i]) + ")")
            # else :
                # try :
                #     no=findColNum(columns_to_display[i],final_cols)
                #     print_data.append(str(agg_data[i])+"("+str(temp_col_name[i]) + ")")
                # except:
                #     print_data.append(str(agg_data[i])+"("+str(columns_to_display[i]) + ")")


            
            

                

            
            

        print_data=",".join(print_data)
        
        
        print(print_data)
        print_data=[]

        for i in agg_data.keys():
            if agg_data[i].lower() == "max" :
                # print ("max(" + str(name_of_col[i]) + ")")

                print_data.append(max(data_to_perform[i])  )
            elif agg_data[i].lower() == "min" :
                # print ("min(" + str(name_of_col[i])+ ")")
                print_data.append(min(data_to_perform[i]))

            elif agg_data[i].lower() =="avg" or agg_data[i].lower() == "average":
                # print ("avg(" + str(name_of_col[i]) + ")")
                print_data.append (reduce(lambda x,y:float(x) + float(y), data_to_perform[i])/float(len(data_to_perform[i])))

            elif agg_data[i].lower() =="count":    
                # print ("count(" + str(name_of_col[i]) + ")")
                print_data.append(len(data_to_perform[i]))

            elif agg_data[i].lower() =="sum":
                # print ("sum(" + str(name_of_col[i]) + ")")
                print_data.append (reduce(lambda x,y:float(x) + float(y), data_to_perform[i]))
            else :
                print("Error")


                
        # print(print_data)  
        print_data=[str(j) for j in print_data]
        print_data=",".join(print_data)
        print(print_data)
    
def get_data_to_perform(rows,col) :
    data=[]
    global cartesian_Table

    for i in rows:
        data.append(cartesian_Table[i][col])

    return data    



def groupQuery(grp_by_col,rows,redundant,final_cols,columns_to_display,orderby,orderbycol) :
    global  cartesian_Table
    
    # print("grou_by col ",grp_by_col)
    grp_by_col_num=findColNum(grp_by_col,final_cols)
    # print(" grp_by_col_num ",grp_by_col_num)
    # print("orderbycol ",orderbycol)

    if orderby != 0 :
        if orderbycol != grp_by_col :
            print(" error in query ")
            exit(-1)




    group_dict={} #dictionary of grp_by_col data to rows 

    data=[]
    for i in rows:
    
            data.append(cartesian_Table[i][grp_by_col_num])

    # print(" data " ,data)    
    data=set(data)

    for i in data :
        group_dict[i]=[]
    
    for i in rows:
        group_dict[cartesian_Table[i][grp_by_col_num]].append(i)
    # print("coming here 1") 
    # print(" group_dict ",group_dict)    
    columns_to_display = columns_to_display.replace(",", " ")
    columns_to_display = columns_to_display.split()



    print_cols=copy.deepcopy(columns_to_display)
    for i in range(0,len(print_cols)) :
        col_name = re.search(r"\(([A-Za-z0-9\*_]+)\)", print_cols[i])
        try :
            col_name=col_name.group(1)
            for j in AGG :
                if j in print_cols[i].lower():
                    no=findColNum(col_name,final_cols)
                    # print("no",no)
                    print_cols[i]=j+"("+str(temp_col_name[no])+")"
                    break
        except:
            no=findColNum(print_cols[i],final_cols)
            print_cols[i]=str(temp_col_name[no])
            # print("print_cols p",print_cols[i])
            continue
          
        
       
        # print("print_cols i",print_cols[i] )

        
    # for i in print_cols:
    #     try :
    #         check_col(i)
    #     except:
    #         continue
    #     num=findColNum(i,final_cols)
    #     print("numm ",num)
    #     i=temp_col_name[num]    
            

    for i in range(len(columns_to_display)) :
        if(columns_to_display[i] == grp_by_col) :
            index_grp_by_col=i


    agg, agg_data, columns_to_display_dup  = checkAggregate(columns_to_display)

    for i in columns_to_display:
        if i not in final_cols :
            # print("Error:Query not Valid")
            exit(-1)
   
    
    # print(" columns_to_display_dup in group query ",columns_to_display_dup )
    # print("agg data in grp query ",agg_data)

   
    print_data_list=[]
    # print("group dict",group_dict)
    for i in group_dict.keys() :
        print_data=[]
        
        print_data.append(i)

        

        for j in agg_data.keys():
            t=findColNum(columns_to_display_dup[j],final_cols)
            # print(" t ",t)
            data_to_perform=get_data_to_perform(group_dict[i],t)
            # print("data to perform  ",data_to_perform)
            ans=evaluate_agg(agg_data[j],data_to_perform)
            # print(" ans ",ans)
            print_data.append(ans)
        # print("print_data ",print_data)  
        print_data_list.append(print_data)  

    # print("coming here 2")    
    if orderby == -1 :
        print_data_list=sorted(print_data_list, key=lambda x: int(x[0]),reverse=True) 

    elif orderby==1 :
        print_data_list=sorted(print_data_list, key=lambda x: int(x[0]))   


     

    # print(" \n \n \n ")
    # print(columns_to_display)
    # print(print_cols)
    print_cols=",".join(print_cols)
    print(print_cols)
    for i in print_data_list:
        if index_grp_by_col >=0:
            #have to change the pattern
            if index_grp_by_col == len(i)-1 :
                temp=i[:]
            else :
                temp=i[:index_grp_by_col+1]

            temp=temp[1:] + temp[0:1]   

            # print("temp ",temp) 

            if index_grp_by_col == len(i)-1 :
                temp2=temp
            else :
                temp2=temp+i[index_grp_by_col+1:]  


           
                     

            i=copy.deepcopy(temp2)
            
            i=[str(j) for j in i]
            temp_i=" ".join(i)
            print(",".join(i))
        else :
            i=[str(j) for j in i]

            
            temp_i=",".join(i[1:])
            print(temp_i)

            
       


        

    # print(" \n \n \n ")    



#select A,B fro
        

    #check error columns to dsplay and grp_by_col

def select_Query(query_tokens):
    global tables_list
    global tables 
    global cartesian_Table

    dist=0
    temp=query_tokens[1].lower()

    if temp in ["distinct"]:
        dist = 1
        curr_tables = query_tokens[4]
        columns_to_display= query_tokens[2]
        
    else:
        curr_tables = query_tokens[3]
        columns_to_display= query_tokens[1]
       

    curr_tables = re.findall("[\'\w]+", curr_tables)
    # print ("curr_tables ",curr_tables)

    #getting the tables data from csv files to store in program

    count_tables=len(curr_tables)
    for i in range(count_tables):
        if curr_tables[i] not in tables_list:
            sys.stderr.write("Error: Table name is not valid.\n")
            quit(-1)
        data=get_data_columns(curr_tables[i])
        tables[str(curr_tables[i])] = Table(str(curr_tables[i]),data, tables_list[str(curr_tables[i])])
        # print("my table data",tables[str(curr_tables[i])].data)
       

   
    #create big cartesian table
  
    global temp_col_name
    table_keys = list(tables.keys())
    final_cols=[]

    for i in range(len(table_keys)):
        table_name = table_keys[i]
        table_name=str(table_name)
        for j in range(len(tables_list[table_name])):
           
            temp_col_name.append(table_name + "." + str(tables_list[table_name][j]))
            # col_name=str(tables_list[tab][j])
            final_cols.append(str(tables_list[table_name][j]))
    # print("final  ",final_cols)
           
    full_Join()
    

    res = [x for x in range(len(cartesian_Table))]
    # print('result ',res)
    redundant = set()
    groupby=0
    orderby=0;
    orderbycol=""
    #first check for where query

    for i in range(len(query_tokens)):
         if query_tokens[i].lower() == "order by"    : 
            orderby=1
            try :
                temp=query_tokens[i+1].split()
                # print("temp ",temp)
                orderbycol=temp[0]

                if len(temp) >1 :
                    if temp[1].lower() =="desc" :
                            orderby=-1

            except:
                print("error in order by statement")

    for i in range(len(query_tokens)):
        if  query_tokens[i].split()[0].lower() in ["where"] :
             res, redundant = whereQuery(query_tokens[i],final_cols)
             # print("res",res)

    
        if query_tokens[i].lower() == "group by" :
            # print("query_tokens [i ] ",query_tokens[i])
            groupby=1
            try :
                # print(" query_tokens [i+ 1]",query_tokens[i+1])
                groupQuery(query_tokens[i+1],res,redundant,final_cols,columns_to_display,orderby,orderbycol)
                # print("no eroorrrrrrrrr")
            except :
                # print(" error in quryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
                print("Error:Query not Valid")
                exit(-1)
                     


     


    # print(" results ",res)

    #then check for group query

    

    #project columns
    k = list(tables_list.keys())
    if columns_to_display == "*":
        columns_to_display = ""        
        for x in k:
            if x in curr_tables:
                for j in range(len(tables_list[x])):
                    # columns_to_display += (str(x) + "." + str(tables_list[x][j]) + ",")
                    columns_to_display += ( str(tables_list[x][j]) + ",")


   
    if(groupby != 1) :

        column_projection(columns_to_display, res, dist, redundant,final_cols,orderby,orderbycol)



def query_pre_process(raw_query):
    queries=[]
    queries = sqlparse.split(raw_query)
    # print("queries ",queries)
    for query in queries:
        if query[-1] != ';':
            print ("Error: SQL command should end with semi colon")
            sys.exit(0)
        query = query[:-1]
        
        parsed_query = sqlparse.parse(query)[0]
        parsed_query = parsed_query.tokens
        #print ("parsed_query ",parsed_query)
        #print("sqlparse.sql.IdentifierList(parsed_query) ",sqlparse.sql.IdentifierList(parsed_query))
        identifier_list = sqlparse.sql.IdentifierList(parsed_query)
        identifier_list =identifier_list.get_identifiers()
        # print("identifier list" ,type( identifier_list))
        query_tokens = []
        for cmd in identifier_list:
            query_tokens.append(str(cmd))

        if query_tokens[0].lower() in ["select"]:           
            # print("query_tokens ",query_tokens)            
            select_Query(query_tokens)
        else:
            print ("Error:Incorrect Query type")
            sys.exit(0)

    


       
def main():
    if len(sys.argv) != 2:
        print ("Error:Incorrect number of arguments passed")
        return
    query = sys.argv[1]
    read_metadata(META)
    query_pre_process(query)


# if ( __name__ == "__main__"):
#     main()           

main()    