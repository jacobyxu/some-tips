## Logging Config

#### Set up logging config

```python
import logging
logging.basicConfig(format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p",
                    level=logging.INFO)
```

#### Filter python module

By setting the module logging level as `logging.ERROR`.

```python
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
```

## Reduce Operations

```python
from functools import reduce
from operator import and_, or_, add
add_all = lambda *not_null_vals: reduce(add, not_null_vals)
and_all = lambda *logical_vars: reduce(and_, logical_vars)
or_all = lambda *logical_vars: reduce(or_, logical_vars)
```

* Be careful of `add`. If there is `None`, the return will be `None`.

## Pyspark

#### Union all DataFrames with the same schema

```python
from functools import reduce
from pyspark.sql import DataFrame
union_all = lambda *dataframe_lists: reduce(DataFrame.unionByName,
                                            dataframe_lists)
```

#### Apply udf with High-Order function

```python
import pyspark.sql.functions as F

def define_lt_udf(argument_list, game):
    return "in_list" if game in argument_list else "out_of_list"
  
def define_lt(argument_list):
    return F.udf(lambda game: define_lt_udf(argument_list, game))

argument_list = ["args1", "args2"]
df = df.withColumn("if_in_list", define_lt(argument_list)(F.col("game")))
```

#### Apply udf on row level

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

_udf = F.udf( lambda _row: func(_row), T.DoubleType())
df = df.withColumn("new_col",
                   _udf(F.struct([song_df[_col] for _col in df.columns])))
```

#### Pivot and Unpivot

wide table:

CAT | A | B | C
-----|-----|---|--
cat1 |a1 | b1 | c1
cat2 |a2 | b2 | c3

long table:

CAT | key | val
-----|-----|--
cat1 | A | a1 
cat2 | A | a2 
cat1 | B | b1 
cat2 | B | b2 
cat1 | C | c1 
cat2 | C | c2 

```python
# wide -> long
import pyspark.sql.functions as F

pivot_cols = ["CAT"]
sub_strs = []

for _col in ["A", "B", "C"]:
  # "value of column key, value of column val" in long table
  sub_str =  "'{}', {}".format(_col, _col)
  sub_strs.append(sub_str)

exprs = ("stack({}, ".format(len(sub_strs)) +
         ", ".join(sub_strs) +
         ") as (key, val)")
long_table = wide_table.select(*pivot_cols, F.expr(exprs))
# exprs = "stack(3, 'A', A, 'B', B, 'C', C) as (key, val)"
```

```python
# long -> wide
wide_table = long_table.groupBy(pivot_cols).pivot('key').agg({'val':'sum'})
```