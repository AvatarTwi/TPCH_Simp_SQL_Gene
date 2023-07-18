import random
import numpy as np

formats = ["0"]*21

formats[4] = "19{}-{}-01"
formats[5] = "19{}-01-01"
formats[6] = "19{}-01-01"
formats[7] = "0.{}"
formats[10] = "19{}-{}-01"
formats[12] = "19{}-01-01"
formats[14] = "19{}-{}-01"
formats[15] = "19{}-{}-01"
formats[16] = "Brand#{}{}"
formats[17] = "Brand#{}{}"
formats[19] = "Brand#{}{}"
formats[20] = "19{}-01-01"

def read_dists():
    dict = {}
    with open("dists.dss", "rb") as f:
        lines = [l.decode("utf-8").replace("\r\n", "") for l in f.readlines()]
        for i in range(len(lines)):
            if "BEGIN " in lines[i].upper() and "#" not in lines[i].upper():
                attr_name = lines[i].lower().replace("begin ", "")
                i += 1
                while "END" not in lines[i].upper():
                    if "count" in lines[i].lower():
                        i += 1
                        continue
                    if attr_name not in dict:
                        dict[attr_name] = []
                    dict[attr_name].append(lines[i].split('|')[0])
                    i += 1
    return dict

dists_result = read_dists()

def rand(a, b):
    return random.randint(a, b)

def get_dists_result(key):
    return dists_result[key][rand(0,len(dists_result[key])-1)]

attr_2_key = {
    "c_mktsegment": get_dists_result("msegmnt"),
    "l_shipdate": formats[14].format(int(93 + rand(0, 59) / 12), rand(0, 59) % 12),
    "l_discount": formats[7].format(str(rand(2, 9))),
    "l_quantity": rand(24, 25),
    "l_shipmode": get_dists_result("smode"),
    "l_receiptdate": formats[12].format(rand(93, 97)),
    "n_name": get_dists_result("nations"),
    "o_orderdate": formats[10].format(int(93 + rand(1, 24) / 12), rand(1, 24) % 12 + 1),
    "o_comment":"%"+get_dists_result("q13a")+"%",
    "p_size":rand(1,50),
    "p_type":get_dists_result("p_types"),
    "p_name":"%"+get_dists_result("colors")+"%",
    "p_brand":formats[17].format(rand(1,3),rand(3,5)),
    "p_container":get_dists_result("p_cntr"),
    "r_name":get_dists_result("regions")
}
