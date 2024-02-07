def db_read(url, user, pwd, query, driver, spark):
    df = spark.read\
        .format("jdbc")\
        .option("url",url)\
        .option("query",query)\
        .option("user",user)\
        .option("password",pwd)\
        .option("driver",driver)\
        .load()
    return df

class db_read:
    def __init__(self,url,user,table,query,password,driver,spark):
        self.url=url
        self.user=user
        self.password=password
        self.driver=driver
        self.query=query
        self.table=table
        self.spark=spark
    def db_read_table(self):
        df = self.spark.read\
            .format("jdbc")\
            .option("url",self.url)\
            .option("dbtable",self.table)\
            .option("user",self.user)\
            .option("password",self.password)\
            .option("driver",self.driver)\
            .load()
        return df
    def db_read_table(self):
        df = self.spark.read\
            .format("jdbc")\
            .option("url",self.url)\
            .option("query",self.query)\
            .option("user",self.user)\
            .option("password",self.password)\
            .option("driver",self.driver)\
            .load()
        return df





