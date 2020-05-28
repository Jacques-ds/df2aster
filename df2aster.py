# ======================================================================================================================
# FUNCTIONS FOR REPETITIVE OR REGULAR TASKS
# Created by: Jacques-ds
# ======================================================================================================================
import pyodbc
import time
import pandas as pd


# ======================================================================================================================
# (1) df2aster
# ======================================================================================================================
# load dataframe into the aster
# tips for improvement:
#   * try to work with different data structure than pandas dataframe
#   * all columns are initiated as varchar - create something that works with more data types


def tup2string(tup):
    s = ""
    for i in range(len(tup)):
        s += str(tup[i]) + "','"
    s = "('" + s[:-2] + "),"
    return s


def df2aster(data, db_table, schema="dsc_ci_work", create_table=True, distribute_by="uid_party_id",
             analytic=True, connection="DSN=CI_Aster", drop_if_exists=True):
    before_df2aster = time.time()
    print("The data transfer has begun... wait for it!")
    # Check whether the db_table is pandas table
    if str(type(data)) == "<class 'pandas.core.frame.DataFrame'>":
        pass
    else:
        raise ValueError('Change table into pandas dataframe, geez!')

    # Create full table name
    db_table = schema + '.' + db_table

    # Start connection with database
    conn = pyodbc.connect(connection)
    crsr = conn.cursor()

    # Drop table
    if drop_if_exists:
        crsr.execute('drop table if exists ' + db_table + ';')
        print("Drop table executed.")
    else:
        pass

    # Create analytic|fact table distribute by hash or replication
    if analytic:
        table_type = 'ANALYTIC'
    else:
        table_type = 'FACT'

    if distribute_by in data.columns:
        distribution = "DISTRIBUTE BY HASH(" + distribute_by + ")"
    else:
        distribution = "DISTRIBUTE BY REPLICATION"

    if create_table:
        create_list = ['CREATE ' + table_type + ' TABLE ' + db_table + '\n' + '( ']
        for col in data.columns:
            col_init = col + " varchar,\n"
            create_list.append(col_init)
        create_list = ''.join(create_list)[:-2] + ')\n' + distribution + '\n' + 'COMPRESS LOW' ';'
        crsr.execute(create_list)
        print("Create table executed.")
    else:
        pass

    # Insert data into the table (takes some time)
    insert_list = ['INSERT INTO ' + db_table + ' VALUES \n']
    print("Insert statement has started.")
    for row in data.itertuples(index=False):
        tb_row = tup2string(row)
        result = tb_row + "\n"
        insert_list.append(result)
    insert_list = ''.join(insert_list)[:-2] + ';'
    crsr.execute(insert_list)
    crsr.commit()
    conn.close()
    after_df2aster = time.time()
    print("We did it in %s seconds" % (after_df2aster - before_df2aster))


# df2aster(data=df_output, db_table="jk_test")







