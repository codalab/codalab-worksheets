import pandas as pd
from tqdm import tqdm

items = list()
for i in tqdm(range(200000)):
  items.append({"status": "success", "error_message": "error", "uuid": "123412341243"})
print("appended")
df = pd.DataFrame.from_records(items)
df.to_csv('test.csv')


