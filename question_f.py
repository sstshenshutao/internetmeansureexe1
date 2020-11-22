import os

from pandao import Dao

source = 'Umbrella'
db = "umbrella20201015.db"
dao = Dao.load_table(os.path.join('data', db), source)
dfs = dao.read_data("SELECT * FROM " + dao.table_name, 10000)
pp = []  # [(amazon,5)]
i = 0
for df in dfs:
    if i == 0:
        pp = df["host"].value_counts()
    else:
        pp = pp.add(df["host"].value_counts(), fill_value=0)
    i += 1
    print(
        "\r calculating %d0000... " % (i))
pp = pp.sort_values(ascending=False)
pp = pp.rename_axis('host').reset_index(name='num_uniq_websites').convert_dtypes()
print(pp)
pp.to_csv(source + '_out.csv', index=False)
