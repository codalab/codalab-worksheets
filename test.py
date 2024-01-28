import pandas as pd
from dataclasses import dataclass

@dataclass
class Test:
  calories: int
  duration: int

data = {
  "calories": [420, 380, 390],
  "duration": [50, 40, 45]
}

#load data into a DataFrame object:
df = pd.DataFrame(data)

print(df) 

print(df["calories"] == 420)
print(df[df["calories"] == 420])
import pdb; pdb.set_trace()
