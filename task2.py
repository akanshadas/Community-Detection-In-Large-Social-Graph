from pyspark import SparkConf, SparkContext
import sys
import json
import csv
import itertools
import time
import math
import random
import operator
import os

# variables

filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
betweenness_output = sys.argv[3]
community_output = sys.argv[4]

# ======================================================== FUNCTIONS ========================================================


def getBetweennessForNode(root, num):
    all_nodes = [user for user in num]

    # 1. PERFORM BFS to get
    queue = []
    visited = set()
    node_level_map = dict()
    child_parents_map = {}

    queue.append(root)  # node,level
    node_level_map[root] = 0
    visited.add(root)
    child_parents_map[root] = {}
    while len(queue) > 0:
        item = queue.pop(0)
        children = num[item]

        for child in children:
            if child not in visited:
                queue.append(child)
                visited.add(child)
                node_level_map[child] = node_level_map[item] + 1
            # child:{parents}
            if node_level_map[child] == node_level_map[item] + 1:
                if child in child_parents_map:
                    par = child_parents_map[child]
                    par.add(item)
                    child_parents_map[child] = par
                else:
                    par = set()
                    par.add(item)
                    child_parents_map[child] = par

    # Go from bottom to top and find the betweenness for each edge wrt the root node
    levels_list = sorted(node_level_map.items(), key=operator.itemgetter(1), reverse=True)

    node_bw_dict = dict([(node, 1.0) for node in all_nodes])
    edge_bw_dict = {}

    for node, level in levels_list:
        parents = child_parents_map[node]
        if level == 0:  # ROOT
            break
        elif len(parents) == 1:  # node can be at level 1 or more
            for parent in parents:
                edge_bw_dict[tuple(sorted([node, parent]))] = node_bw_dict[node]
                node_bw_dict[parent] += node_bw_dict[node]
        elif len(parents) > 1:  # multiple parents means that node is at level >=2
            # 2 conditions: if grandparents exist and if grandparents dont exist
            sumVal = 0
            for parent in parents:
                grandparents = child_parents_map[parent]
                no_of_shortest_paths_to_parent = len(grandparents)
                sumVal += no_of_shortest_paths_to_parent
            
            for parent in parents:
                grandparents = child_parents_map[parent];
                no_of_shortest_paths_to_parent = len(grandparents)
                partial_credit = node_bw_dict[node] * (float(no_of_shortest_paths_to_parent) / float(sumVal))
                edge_bw_dict[tuple(sorted([node, parent]))] = partial_credit
                node_bw_dict[parent] += partial_credit

    return edge_bw_dict


def calculateBetweennessOfEdges(nearby_users_map):
    all_nodes = [user for user in nearby_users_map]
    edge_betweenness = {}

    final_result = {}
    for root in all_nodes:
        getBw = getBetweennessForNode(root, nearby_users_map)
        for edge, bw in getBw.items():
            if edge in edge_betweenness:
                edge_betweenness[edge] += bw
            else:
                edge_betweenness[edge] = bw
    for k, v in edge_betweenness.items():
        final_result[k] = float(v) / 2.0
    sorted_bw_map = sorted(final_result.items(), key=lambda x: (-x[1], x[0]))

    return sorted_bw_map


def BFS_find_community(root, nearby_users_map):
    all_nodes = [user for user in nearby_users_map]

    # 1. PERFORM BFS to get
    queue = []
    visited = set()

    queue.append(root)  # node,level
    visited.add(root)
    while len(queue) > 0:
        item = queue.pop(0)
        children = nearby_users_map[item]

        for child in children:
            if child not in visited:
                queue.append(child)
                visited.add(child)
    return visited


def GirvanNewman(graph):
    graph_ = graph.copy()
    graph_set = set(graph.keys())
    communities = set()
    for user in graph_set: 
        cm = set()
        cc = set()

        if user in graph_.keys():
            cm = graph_.get(user)
            cc = set([user])

        while len(cm) > 0:
            members = set()
            for item in cm:
                cc.add(item)
                if item in graph_.keys():
                    members = members.union(
                        set(graph_.get(item)))
                    graph_.pop(item)

            cm = members.difference(set([user]))
        if len(cc) > 0:
            communities.add(tuple(sorted(list(cc))))

    return communities


def findModularity(clusters, m, adjacency_matrix, degree_matrix):

    d = float(2 * m)
    modularity = 0
    for cluster in clusters:
        c = list(cluster)
        mod_cluster = 0
        for node1 in c:
            for node2 in c:
                sorted_edge = tuple(sorted((node1, node2)))
                lz2 = (degree_matrix[node1] * degree_matrix[node2]) / (2 * m)
                if adjacency_matrix[(node2, node1)] == 1.0 or adjacency_matrix[(node1, node2)] == 1.0:
                    lz1 = 1
                else:
                    lz1 = 0
                mod_cluster += (lz1 - lz2)
        modularity += mod_cluster

    return modularity / d


# =========================================== START ===========================================

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task1')
# sc.setLogLevel("ERROR")

start = time.time()
ufRDD = sc.textFile(input_file_path).map(lambda e: e.split(",")).map(lambda e: (e[0], e[1]))
headers = ufRDD.take(1)
fRDD = ufRDD.filter(lambda e: e[0] != headers[0][0])

# Step 1 : Creating graph
user_business_list = fRDD.groupByKey().mapValues(lambda entry: list(set(entry))).collect()
nearby_users_map = {}
for user1, b1s in user_business_list:
    related_users = set()
    for u2, b2s in user_business_list:
        if u1 != u2:
            if len(set(b1s).intersection(set(b2s))) >= filter_threshold:
                related_users.add(u2)
    if (len(related_users)) > 0:
        nearby_users_map.update({u1: related_users})

nodes = set([user for k, v in nearby_users_map.items() for user in v])

# BFS for each node as root
betweenness_map = calculateBetweennessOfEdges(nearby_users_map)
f = open(betweenness_output, "w")
for i in range(len(betweenness_map
    if (i != len(betweenness_map) - 1):
        line = str(edge) + ", " + str(bw) + "\n"
        line.replace("u\'", "u\'")
        f.write(line)
    else:
        line = str(edge) + ", " + str(bw)
        f.write(line)
f.close()

betweenness_dict = dict(betweenness_map)
m = len(betweenness_map)

# TASK 2.2
nodes = [user for user in nearby_users_map]
adjacency_matrix = {}
for u1 in nodes:
    for u2 in nodes:
        if u2 in nearby_users_map[u1]:
            adjacency_matrix.update({tuple([u1, u2]): 1.0})
        else:
            adjacency_matrix.update({tuple([u1, u2]): 0.0})

degree_matrix = {}
for u1 in nodes:
    degree_matrix[u1] = len(nearby_users_map[u1])

#start
mod_communities_map = []

# LOCAL COPIES:
l_nearby_users_map = nearby_users_map.copy()
l_betweenness_map = betweenness_map.copy()
edgesRemoved = 0

# modularity for iteration i = 0, original graph
temp_cluster_set = [];
# to avoid recomupting cluster_set for original graph
temp_cluster_set.append(set(nearby_users_map.keys()));
clusters = GirvanNewman(l_nearby_users_map)
clusters_set = [set(x) for x in clusters]
modularity = findModularity(clusters_set, m, adjacency_matrix, degree_matrix);
max_modularity = modularity;
mod_communities_map.append((modularity, temp_cluster_set));
numberOfCommunities = 1

while (edgesRemoved < len(betweenness_map)):

    # calculating betweenness
    if edgesRemoved != 0:
        l_betweenness_map = calculateBetweennessOfEdges(l_nearby_users_map)  # calculate new edge betweennesses after removing edge
    # Removing edge
    (n1, n2), max_bw = l_betweenness_map.pop(0)
    a = l_nearby_users_map[n1]; diff_n1 = a - {n2};
    l_nearby_users_map[n1] = diff_n1
    b = l_nearby_users_map[n2]; diff_n2 = b - {n1};
    l_nearby_users_map[n2] = diff_n2

    edgesRemoved += 1

    clusters = GirvanNewman(l_nearby_users_map)
    clusters_set = [set(x) for x in clusters]

    modularity = findModularity(clusters_set, m, adjacency_matrix, degree_matrix)

    if modularity > max_modularity:    max_modularity = modularity
    mod_communities_map.append((modularity, clusters_set))
final_modularity_output = sorted(mod_communities_map, key=lambda x: (-x[0], x[1]))

debug_mod = []
for k in range(len(final_modularity_output)):
    mod, clusts = final_modularity_output[k]
    debug_mod.append((k, mod, len(clusts)))

mod, communities = final_modularity_output[0]

output_communities = sc.parallelize(communities).map(lambda x: (tuple(sorted(x)), len(x))).sortBy(=lambda x: (x[1], x[0])).map(lambda x: x[0]).collect()

f = open(community_output, "w")
for i in range(len(output_communities)):
    line = ""
    for node in output_communities[i]:
        line += "\'" + str(node) + "\'" + ", "
    if i != len(output_communities) - 1:
        f.write(line[:-2] + "\n")
    else:
        f.write(line[:-2])
f.close()

end = time.time()
print("\n\nDuration:", end - start)



