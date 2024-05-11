def df_to_sql_insert(df, table_name, filter_columns):
    filter_columns_str = ''
    if (len(filter_columns) > 0):
        filter_columns_str = '(' + (','.join(filter_columns))+')'

    array_insert = []
    for index, row in df.iterrows():
        index_data = ''
        sql_values = '(\''+index_data + \
            '\',\''.join([str(i).replace("'", "\\'") for i in row])+'\')'
        array_insert.append(sql_values)
    sql_insert = "insert into {} {} values {};".format(table_name,  filter_columns_str,
                                                      ",".join([i for i in array_insert]))
    return sql_insert
