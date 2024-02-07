import pandas as pd

from Utility.write_db_file_lib import write_output,out

#schema/DDL validation
class StructType:
    pass


def schema_check(source_df,target_df,out,expected_schema=None):
    if expected_schema is None:
        ls = []
        for scol in source_df.schema.fields:
            for tcol in target_df.schema.fields:
                if scol.name==tcol.name:
                    if scol.dataType==tcol.dataType:
                        pass
                    else:
                        ls.append((scol.name,scol.dataType,tcol.dataType))
        pd.DataFrame(ls,coulmns=['column_nmae','source_datatype','target_datatype'])
        if len(ls)>0:
            write_output(6,"Schema_validation",'NA','NA','Fail',len(ls),out)
        else:
            write_output(6,"Schema_validation",'NA','NA','Pass',0,out)
    else:
        ls = []
        with open(expected_schema) as file:
            source_schema = StructType.fromJson(json.load(file))
            for scol in source_schema:
                for tcol in target_df.fields:
                    if scol.name == tcol.name:
                        if scol.dataType == tcol.dataType
                            pass
                        else:
                            ls.append(scol.name,scol.dataType,tcol.dataType)
                pd.DataFrame(ls,columns=['column_name','sourcetype','targettype'])

#count validation
def count_validation(sourceDF,targetDF,out:dict):
    source_count=sourceDF.count()
    target_count=targetDF.count()
    diff=source_count-target_count
    if source_count == target_count:
        print("Count is Matching between source and target")
        write_output(1,"Count_validation",source_count,target_count,'Pass',abs(diff),out)
    else:
        print("Count is Not Matched between source and trget adn difference is",abs(source_count-target_count))
        write_output(1,"Count_validation",source_count,target_count,'Fail',abs(diff),out)
#duplicate validation
def duplicate(dataframe,key_columns:list,out):
    dup_df=dataframe.groupby(key_columns).count().filter('counter>1')
    target_count=dataframe.count()
    if dup_df.count()>0:
        print("Duplicates Present")
        dup_df.show()
        write_output(2,"Duplicate Check","NA",target_count,'Fail','target_count',out)
    else:
        print("No Duplicates")
        write_output(2,"Duplicate Check","NA",target_count,'PASS','target-_count',out)
#column value validation
#Null value check
def null_value_check(dataframe,Null_columns,out):
    target_count = dataframe.count()
    Null_columns = Null_columns.split(",") #col1,col2 ==>['col1','col2']
    for col in Null_columns:
        Null_df = dataframe.select(count(when(col(column).contains('None) | \
                                        col(column).contains('NULL')| \
                                        col(column).contains('Null')| \
                                        (col(column) == ''| \
                                        col(column).isNull()| \
                                        isnan(column),column)).alias("NUll_value_count"))
        count=Null_df.collect()
        print(count)

        if count[0]['NULL_value_count']>0:
            print(f"{coulmn} contains null values")
            Null_df.show(10)
            write_output(7,"NUll Value Check","NA",target_count,'Fail',count[0][0],out)
        else:
            print(f"{column} has no null values")
            write_output(7,"NUll Value Check","NA",target_count,'Pass',count[0][0],out)


#Uniqueness check
def Unique_check(dataframe,unique_col:list,out):
    target_count = dataframe.count()
    for col in unique_col:
        dup_df=dataframe.groupBy(col).count().filter('count>1')
        if dup_df.count()>0:
            print(f"{cokumn} has duplicate data")
            dup_df.show(10)
            write_output(3,"Uniqueness Check","NA",target_count,'PASS','target-_count',out)
        else:
            print(f"{col} values are unique records")
            write_output(3,"Uniqueness Check","NA",target_count,'PASS','target-_count',out)

#incremental -> SCD types, CDC
#reords check from target to source
def records_present_only_in_target(source,target,keyList:list,out):
srctemp=source.select(keyList).roupBy(keyList).count().withCoulumnRenamed("count","source_count")
trgtemp=target.select(keyList).roupBy(keyList).count().withCoulumnRenamed("count","target_count")
count_compare=srctemp.join(trgtemp,keyList,how='full_outer')
count = count_compare.filter("source_count is null or source_count!=target_count").count()
print("Key Coulumn values present in target but not in source",+str(count))
source_count=source.count()
target_count=target.count()
if count>0:
    count_compare.filter("source_count is null").show()
    write_output(4,"records_present_only_in_target Check",source_count,target_count,'FAIL',count,out)
else:
    print("All records between source and target are matched")
    write_output(4,"records_present_only_in_target Check",source_count,target_count,'pass',target,out)

#reords check from source to target
def records_present_only_in_source(source,target,keyList:list,out):
srctemp = source.select(keyList).roupBy(keyList).count().withCoulumnRenamed("count","source_count")
trgtemp = target.select(keyList).roupBy(keyList).count().withCoulumnRenamed("count","target_count")
count_compare=srctemp.join(trgtemp,keyList,how='full_outer')
count = count_compare.filter("target_count is null or source_count!=target_count").count()
print("Key Coulumn values present in target but not in source",+str(count))
source_count=source.count()
target_count=target.count()
if count>0:
    count_compare.filter("target_count is null").show()
    write_output(5,"records_present_only_in_source Check",source_count,target_count,'FAIL',count,out)
else:
    print("All records between source and target are matched")
    write_output(5,"records_present_only_in_source Check",source_count,target_count,'pass',count,out)
#data check between source and target
def data_compare(source,target,keycolumn,out):
    keycolumn = keycolumn.split(",")
    columnList=source.columns
    for column in columnList:
        if column not in keycolumn:
            keycolumn.append(column)
            temp_source = source.select(keycolumn).withColumnRenamed(Column,"Source_"+column)
            temp_target = source.select(keycolumn).withColumnRenamed(Column,"target_"+column)
            keycolumn.remove(column)
            temp_join = temp_source.join(temp_target,keycolumn,how='full_outer')
            temp_join.withColumn("comparision",when(col('source_'+column) == col("target_
+column),"True").otherwise("False")).filter("comparision"=="False")
