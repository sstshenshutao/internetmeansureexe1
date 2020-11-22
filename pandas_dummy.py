# import os
# import numpy as np
# import pandas as pd
#
# from pandao import Dao
#
# # a = pd.DataFrame(alexa_a_struct, index=[0])
# dao = Dao.load_table(os.path.join('data', 'example.db'), 'Umbrella')
# chunksize = 10
# data_frames = dao.read_data('select * from %s' % dao.table_name, chunksize=chunksize)
# for df in data_frames:
#     # add empty column
#     df["ASes"] = ""
#     for index, row in df.iterrows():
#         ip4_addr = row['ip4_address']
#         ip6_addr = row['ip6_address']
#         ases = ''
#         if ip4_addr is not None:
#             print(get_as(ip4_addr))
#             if get_as(ip4_addr) is not None:
#                 print(ip4_addr)
#                 ases = get_as(ip4_addr)[0]
#             else:
#                 ases = None
#         elif ip6_addr is not None:
#             if get_as(ip6_addr) is not None:
#                 ases = get_as(ip6_addr)[0]
#             else:
#                 ases = None
#         else:
#             ases = None
#         df.loc[index, 'ASes'] = ases
#     # print(df)
#     df.to_sql(name='Umbrella', con=dao.conn,
#               if_exists='replace', index=False)
#
#     # ip4_address_column = pd_table['ip4_address']
#     # print( or pd_table['ip6_address'])
#     # exit()
# # pd.DataFrame.to_sql(a, name='table1', con=dao._conn,
# #                     if_exists='append', index=False)
# # print(cc)
from question_d import Asn

asn = Asn("umbrella20201015.db", 'umbrella', year=2020, month=10, chunksize=1000)
# single process (a little slow)
asn.flush_ases()
