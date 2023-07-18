import collections
import os
import random
from src.attr_rel_dic import rel_attr_list_dict
from src.dict import attr_2_key

explain = "EXPLAIN (ANALYZE,VERBOSE,COSTS,BUFFERS,TIMING,SUMMARY,FORMAT JSON) "
select = " SELECT {} "
join = " JOIN {} "
on = " ON {} = {} "
from_t = " FROM {} "
where = " WHERE "
group_by = " GROUP BY {} "
order_by = " ORDER BY {} "
between = " {} BETWEEN {} AND {} "
in_ = " {} IN ({},{}) "
eq = " {} = {} "
ge = " {} >= {} "
le = "  {} >= {} "
interval = " {} <= date '1998-12-01' - interval '{}' day "
notlike = " {} NOT LIKE '{}' "
like = " {} LIKE '{}' "

total=10

def where_func(table):
    if table[0] not in [i[0] for i in attr_2_key.keys()] or table == "partsupp":
        return ""

    attr = rel_attr_list_dict[table][random.randint(0, len(rel_attr_list_dict[table]) - 1)]

    while attr not in attr_2_key.keys():
        print(attr)
        attr = rel_attr_list_dict[table][random.randint(0, len(rel_attr_list_dict[table]) - 1)]

    where_type = []

    if attr == "o_comment" or attr == "p_name":
        where_type.append(like.format(attr, attr_2_key[attr]))
        where_type.append(notlike.format(attr, attr_2_key[attr]))
    elif type(attr_2_key[attr]) != str:
        where_type.append(in_.format(attr, attr_2_key[attr], attr_2_key[attr]))
        where_type.append(eq.format(attr, attr_2_key[attr]))
        where_type.append(ge.format(attr, attr_2_key[attr]))
        where_type.append(le.format(attr, attr_2_key[attr]))
    else:
        where_type.append(eq.format(attr, '\'' + attr_2_key[attr] + '\''))
        where_type.append(in_.format(attr, '\'' + attr_2_key[attr] + '\'', '\'' + attr_2_key[attr] + '\''))

    if "date" in attr:
        where_type.append(interval.format(attr, random.randint(60, 120)))
    if attr == "l_discount":
        where_type.append(between.format(attr, attr_2_key[attr], attr_2_key[attr]))

    return where + where_type[random.randint(0, len(where_type) - 1)]


def agg(root_dir, idx):
    if not os.path.exists(root_dir + "/" + idx):
        os.mkdir(root_dir + "/" + idx)
    count = len(os.listdir(root_dir + "/" + idx)) + 1
    pre = count
    for table in rel_attr_list_dict.keys():
        for attr in rel_attr_list_dict[table]:
            if count - pre > total:
                return
            if "key" not in attr:
                continue
            with open(root_dir + "/" + idx + "/" + str(count) + ".sql", "wb") as f:
                count += 1
                # SELECT COUNT(*) FROM supplier GROUP BY s_nationkey ;
                sql = explain + select + from_t + where_func(table) + group_by
                sql = sql.format("COUNT(*)", table, attr)
                f.write(str.encode(sql, 'utf-8'))


def hashjoin(root_dir, idx):
    keys = {}

    if not os.path.exists(root_dir + "/" + idx):
        os.mkdir(root_dir + "/" + idx)

    count = len(os.listdir(root_dir + "/" + idx)) + 1
    pre = count
    for table in rel_attr_list_dict.keys():
        for key in rel_attr_list_dict[table]:
            if "key" in key:
                if key[1:] not in keys.keys():
                    keys[key[1:]] = []
                keys[key[1:]].append(table)

    for key in keys.keys():
        for tablex in keys[key]:
            for tabley in keys[key]:
                if tabley != tablex:
                    if count - pre > total:
                        return
                    with open(root_dir + "/" + idx + "/" + str(count) + ".sql", "wb") as f:
                        count += 1
                        # SELECT * FROM nation join customer on c_nationkey = n_nationkey ;
                        sql = explain + select + from_t + join + on + where_func(tablex)
                        sql = sql.format("*", tablex, tabley, tablex[0] + key, tabley[0] + key)
                        f.write(str.encode(sql, 'utf-8'))


def mergejoin(root_dir, idx):
    keys = {}

    if not os.path.exists(root_dir + "/" + idx):
        os.mkdir(root_dir + "/" + idx)

    count = len(os.listdir(root_dir + "/" + idx)) + 1
    pre = count
    for table in rel_attr_list_dict.keys():
        for key in rel_attr_list_dict[table]:
            if "key" in key:
                if key[1:] not in keys.keys():
                    keys[key[1:]] = []
                keys[key[1:]].append(table)

    for key in keys.keys():
        for tablex in keys[key]:
            for tabley in keys[key]:
                if tabley != tablex:
                    if count - pre > total:
                        return
                    with open(root_dir + "/" + idx + "/" + str(count) + ".sql", "wb") as f:
                        count += 1
                        # SELECT * FROM nation join customer on c_nationkey = n_nationkey order by c_nationkey ;
                        sql = explain + select + from_t + join + on + where_func(tablex) + order_by
                        sql = sql.format("*", tablex, tabley, tablex[0] + key, tabley[0] + key, tabley[0] + key)
                        f.write(str.encode(sql, 'utf-8'))


def nestedloop(root_dir, idx):
    keys = {}

    if not os.path.exists(root_dir + "/" + idx):
        os.mkdir(root_dir + "/" + idx)

    count = len(os.listdir(root_dir + "/" + idx)) + 1
    pre = count
    for table in rel_attr_list_dict.keys():
        for key in rel_attr_list_dict[table]:
            if "key" in key:
                if key[1:] not in keys.keys():
                    keys[key[1:]] = []
                keys[key[1:]].append(table)

    for key in keys.keys():
        for tablex in keys[key]:
            for tabley in keys[key]:
                if tabley != tablex:
                    if count - pre > total:
                        return
                    with open(root_dir + "/" + idx + "/" + str(count) + ".sql", "wb") as f:
                        count += 1
                        # SELECT * FROM nation join customer on c_nationkey = n_nationkey ;
                        sql = explain + select + from_t + join + on + where_func(tablex)
                        sql = sql.format("*", tablex, tabley, tablex[0] + key, tabley[0] + key)
                        f.write(str.encode(sql, 'utf-8'))


def seqscan(root_dir, idx):
    if not os.path.exists(root_dir + "/" + idx):
        os.mkdir(root_dir + "/" + idx)
    count = len(os.listdir(root_dir + "/" + idx)) + 1
    pre = count
    for table in rel_attr_list_dict.keys():
        if count - pre > total:
            return
        with open(root_dir + "/" + idx + "/" + str(count) + ".sql", "wb") as f:
            count += 1
            # SELECT * FROM customer ;
            sql = explain + select + from_t + where_func(table)
            sql = sql.format("*", table)
            f.write(str.encode(sql, 'utf-8'))


def sort(root_dir, idx):
    if not os.path.exists(root_dir + "/" + idx):
        os.mkdir(root_dir + "/" + idx)
    count = len(os.listdir(root_dir + "/" + idx)) + 1
    pre = count
    for table in rel_attr_list_dict.keys():
        for attr in rel_attr_list_dict[table]:
            if "key" in attr:
                if count - pre > total:
                    return
                with open(root_dir + "/" + idx + "/" + str(count) + ".sql", "wb") as f:
                    count += 1
                    # SELECT * FROM supplier ORDER BY s_nationkey ;
                    sql = explain + select + from_t + where_func(table) + order_by
                    sql = sql.format("*", table, attr)
                    f.write(str.encode(sql, 'utf-8'))


OP_INPUT_DICT = {
    "agg": agg,
    "hashjoin": hashjoin,
    "mergejoin": mergejoin,
    "nestedloop": nestedloop,
    "seqscan": seqscan,
    "sort_index": sort
}

OP_INPUT_DICT = collections.defaultdict(lambda: agg, OP_INPUT_DICT)

if __name__ == '__main__':
    root_dir = "../sqls"

    for dir_path, dir_names, file_names in os.walk(root_dir):
        for file_name in file_names:
            file_path = os.path.join(dir_path, file_name)
            os.remove(file_path)

    for i in range(1):
        for idx, key in enumerate(OP_INPUT_DICT.keys()):
            OP_INPUT_DICT[key](root_dir, "total")
