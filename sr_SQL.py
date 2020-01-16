import mysql.connector#导入mysql库
import csv
class csv_insert_sql():
    def __init__(self,sql_name):
        self.conn=mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="1234",
            database=sql_name,
            charset='utf8mb4'
        )
        print('成功连接数据库！')#使用mysql库连接本地MySQL数据库
    def drop_table(self,table_name):
        mycursor=self.conn.cursor()
        str_sql="drop table "+table_name
        mycursor.execute(str_sql) 
        self.conn.commit()
        mycursor.close()
        print('删除%s表！'%(table_name)) # 删除表C919sr_info
    def create_table(self,table_name):
        mycursor = self.conn.cursor()
        str_sql="create table "+table_name+'''(
            sr_id varchar (20)  PRIMARY KEY not null ,
            sr_creatime varchar (20),
            sr_title mediumtext ,
            sr_status varchar (20) ,
            sr_ques mediumtext ,
            sr_ans mediumtext ,
            sr_reqtime varchar (20),
            flyty varchar (25) ,
            flyid varchar (15) ,
            sr_custom varchar (25) ,
            sr_anstime varchar (20),
            sr_closetime varchar (20),
            sr_priority varchar (15) ,
            sr_type varchar (20) ,
            sr_method varchar (10) ,
            sr_contact varchar (20),
            isr_num int(10) ,
            sr_firstover varchar (5) ,
            sr_timeval varchar (25) ,
            sr_ata varchar (5) ,
            sr_isovertime varchar (5) 
            )engine=innodb default charset=utf8mb4'''
        mycursor.execute(str_sql)  
        self.conn.commit()
        mycursor.close()
        print('成功创建%s表！'%(table_name))# 在数据库中创建表单
    def data_insert(self,csv_filename,table_name):
        mycursor = self.conn.cursor()
        sr_file=open(csv_filename,encoding='utf-8-sig')
        sr_reader=csv.reader(sr_file)
        sr_data=list(sr_reader)
        print('正在导入数据...')
        str_sql="INSERT INTO "+table_name+''' (
            sr_id, sr_creatime, sr_title, sr_status, sr_ques,sr_ans, 
            sr_reqtime, flyty, flyid, sr_custom, sr_anstime, sr_closetime, 
            sr_priority, sr_type, sr_method, sr_contact, isr_num, sr_firstover, 
            sr_timeval, sr_ata, sr_isovertime ) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        for i in range(1,len(sr_data)):
            data_val=sr_data[i][:]
            mycursor.execute(str_sql,data_val)
        self.conn.commit()
        mycursor.close()
        print('%s导入MySQL成功！共有%s行数据完成插入'%(csv_filename,i)) #完成数据插入
    def db_close(self):
        self.conn.close()#关闭数据库
if __name__=='__main__':
    mydb=csv_insert_sql('rrs_db')
    mydb.drop_table('sr_info')
    mydb.drop_table('c919sr_info')
    mydb.create_table('sr_info')
    mydb.create_table('c919sr_info')
    mydb.data_insert('all_sr_new.csv','sr_info')
    mydb.data_insert('c919_new.csv','c919sr_info')
    mydb.db_close()
