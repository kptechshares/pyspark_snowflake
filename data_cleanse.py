
df1

df2=df1
df2["avg_calc"]=df2.groupBY("company").avg("price")

main_result = df1.merge(df2,df1.company=df2.company,how="cross")



def fun1(x):


