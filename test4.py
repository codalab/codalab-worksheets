import pandas as pd


df1 = pd.DataFrame.from_dict({
"uuid": ["1", "2", "3"],
"status": ["fail", "fail", "fail"]
})

df2 = pd.DataFrame.from_dict({
"uuid": ["1", "4"],
"status": ["success", "success"]
})

print(df1)
print(df2)


print(df1.merge(df2, how='outer'))
df1 = df1.merge(df2, how='outer')
print(df1.drop_duplicates('uuid', keep='last'))
