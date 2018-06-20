num = 50663511
num_of_p = num/50000
sql_text = "ALTER TABLE Posts PARTITION BY RANGE(Id)  ("
for i in range(num_of_p):
	sql_text += "PARTITION p"+str(i)+" VALUES LESS THAN ("+str(50000*(i+1))+"),"

sql_text += "PARTITION p"+str(num_of_p+1)+" VALUES LESS THAN MAXVALUE);"
