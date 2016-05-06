#!/usr/bin/env python

import igraph
import collections
import logging
import json
import time
from random import randint

# from h3.tree import Tree


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
            filename="log",
            filemode="w+",
            format="%(asctime)-10s \
                    %(levelname)-6s %(message)s")
    log = logging.getLogger()
    edges = igraph.Graph.Barabasi(n=1000, m=100, directed=True).spanning_tree(None, True).get_edgelist()

    referrer_list = ["t.co", "twitter.com", "twitterrific.com", "www.buzzfeed.com", \
            "m.facebook.com", "www.facebook.com", "null"]
    
    refer_history = []
    for child_id, parent_id in edges:
         refer_history += [{"parent": parent_id, \
               "referrer": referrer_list[randint(0, len(referrer_list) - 1)], \
               "created_at": int(time.time()) - randint(0, 200), \
               "child": child_id, \
               "buzzid": 2449673, 
               "multiplicity": 1}]
    # http://www.buzzfeed.com/ryanhatesthis/bryan-cranston-wore-a-really-terrifying-bryan-cranston-mask
    print refer_history

    logging.info(edges)

    with open('data.json', 'w') as outfile:
        json.dump(refer_history, outfile)


    """
    "parent": 0, "referrer": "t.co", "created_at": 1429302541, "child": 1, 
    "buzzid": 3752663, "multiplicity": 1
    #tree = Tree(edges)
    #tree.scatter_plot()
    """

