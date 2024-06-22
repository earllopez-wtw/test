
# COMMAND ----------

import pandas as pd

# Define the conversion ratio dictionary
currency_conversion = {'AED': 0.27, 'HKD': 0.17, 'EUR': 1.18}

# Define the DataFrame
df = pd.DataFrame({
    'currency': ['AED', 'HKD', 'AED']
})

print("Before:\n", df)