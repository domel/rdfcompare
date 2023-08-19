#!/usr/bin/env python
import queue as QQ
import networkx as nx
import copy
import hashlib as hlib
import rdflib
from time import process_time
import argparse

##################################################################
# Class describing a single RDF node in a given RDF graph.

class RDF_node:
    def __init__(self, name):
        self.name = name  # name of the node
        self.incoming_reals = {}  # keeps track of blank nodes which leads to this node
        self.incoming_blanks = {}  # keeps track of non-blank nodes which leads to this node
        self.in_degree = 0  # number of total vertices pointing to this node (including blank nodes)
        self.out_degree = 0  # number of total vertices visitable directly from this node
        self.real_neighbours = {}  # keeps track of non-blank neighbours
        self.blank_neighbours = {}  # keeps track of blank neighbours to which this node leads
        self.is_blank = self.is_blank_node()  # boolean variable which is true if node is blank, false otherwise
        self.blank_in_degree = 0  # number of blank vertices pointing to this node
        self.blank_out_degree = 0  # number of blank vertices visitable directly from this node
        self.temp_degree = 0  # temporary degree for hashing purposes
        self.structure_number = -1  # denotes the number of blank-node tree to which the node belongs
        self.structure_level = -1  # denotes the level of the tree at which given node can be found

    def __lt__(self, other):  # we need some form of __lt__ to resolve situations where
        if(self.is_blank and not other.is_blank):  # the priority tuples are exactly the same.
            return False
        else:
            return True

    ##########################################
    # Generates a priority tuple, which is used to sort RDF triples in certain cases.
    # Two variants are generated -- one for blank nodes, one for standard ones.

    def generate_priority_tuple(self, RDFGraph=None, predicate="", interwoven=True):
        if(self.is_blank):
            if(interwoven):
                neighbours = ""

                #############################
                # Adding real incoming neighbours to the hash material in predefined order
                miniqueue = QQ.PriorityQueue()
                for neighbour in self.incoming_reals:
                    for predicate in self.incoming_reals[neighbour]:
                        neigh = RDFGraph.standard_nodes[neighbour]
                        miniqueue.put((neigh.generate_priority_tuple(RDFGraph, predicate), [neigh, predicate]))
                while not miniqueue.empty():
                    RDFedge = miniqueue.get()[1]
                    neighbours += prepare_single_triplet(RDFedge[0], RDFedge[1], self)

                #############################
                # Adding blank incoming neighbours to the hash material in predefined order
                miniqueue = QQ.PriorityQueue()
                for neighbour in self.incoming_blanks:
                    for predicate in self.incoming_blanks[neighbour]:
                        neigh = RDFGraph.blanks[neighbour]
                        miniqueue.put((neigh.generate_priority_tuple(
                            RDFGraph, predicate, interwoven=False), [neigh, predicate]))
                while not miniqueue.empty():
                    RDFedge = miniqueue.get()[1]
                    neighbours += prepare_single_triplet(RDFedge[0], RDFedge[1], self)

                #############################
                # Adding real neighbours to the hash material in predefined order
                miniqueue = QQ.PriorityQueue()
                for neighbour in self.real_neighbours:
                    for predicate in self.real_neighbours[neighbour]:
                        neigh = RDFGraph.standard_nodes[neighbour]
                        miniqueue.put((neigh.generate_priority_tuple(RDFGraph, predicate), [neigh, predicate]))
                while not miniqueue.empty():
                    RDFedge = miniqueue.get()[1]
                    neighbours += prepare_single_triplet(self, RDFedge[1], RDFedge[0])

                #############################
                # Adding blank neighbours to the hash material in predefined order
                miniqueue = QQ.PriorityQueue()
                for neighbour in self.blank_neighbours:
                    for predicate in self.blank_neighbours[neighbour]:
                        neigh = RDFGraph.blanks[neighbour]
                        miniqueue.put((neigh.generate_priority_tuple(
                            RDFGraph, predicate, interwoven=False), [neigh, predicate]))
                while not miniqueue.empty():
                    RDFedge = miniqueue.get()[1]
                    neighbours += prepare_single_triplet(self, RDFedge[1], RDFedge[0])

                return (self.structure_level, self.blank_in_degree, self.in_degree, self.blank_out_degree, self.out_degree, neighbours, predicate)
            else:
                return (self.structure_level, self.blank_in_degree, self.in_degree, self.blank_out_degree, self.out_degree, predicate)
        else:
            return (self.name, self.blank_in_degree, self.in_degree, self.blank_out_degree, self.out_degree, predicate)
    ###########################################

    ############################################
    # A method recognizing whether node is blank or not, based on its name
    # For the sake of simplicity, the names of blank nodes in this mock-up
    # database start with five letter substring "blank".

    def is_blank_node(self):
        if self.name[:5] == "blank":
            return True
        else:
            return False

    ############################################

    ############################################
    # Adds a blank neighbour to a given node
    # where connection is given by a defined predicate

    def add_blank_neighbour(self, object, predicate):
        if(object.name in self.blank_neighbours):
            self.blank_neighbours[object.name].append(predicate)
        else:
            self.blank_neighbours.update({object.name: [predicate]})
        self.blank_out_degree += 1
        self.out_degree += 1
    ############################################

    ############################################
    # Adds a blank neighbour to a given node
    # where connection is given by a defined predicate

    def add_real_neighbour(self, object, predicate):
        if(object.name in self.blank_neighbours):
            self.real_neighbours[object.name].append(predicate)
        else:
            self.real_neighbours.update({object.name: [predicate]})
        self.out_degree += 1
    ############################################

    ############################################
    # Adds a blank neighbour to a given node
    # where connection is given by a defined predicate

    def add_incoming_blank(self, subject, predicate):
        #print("While reading, added to " + self.name, predicate, subject.name,sep='\t')
        if(subject.name in self.incoming_blanks):
            self.incoming_blanks[subject.name].append(predicate)
        else:
            self.incoming_blanks.update({subject.name: [predicate]})
        self.blank_in_degree += 1
        self.in_degree += 1
    ############################################

    ############################################
    # Adds a blank neighbour to a given node
    # where connection is given by a defined predicate

    def add_incoming_real(self, subject, predicate):
        if(subject.name in self.incoming_reals):
            self.incoming_reals[subject.name].append(predicate)
        else:
            self.incoming_reals.update({subject.name: [predicate]})
        self.in_degree += 1
    ############################################

    ############################################
    # Converts node to string with list of blank neighbours.
    # Mainly for debugging purposes.

    def to_string(self):
        return(self.name + " with blank neighbours " + str(self.blank_neighbours))

    ############################################

###########################################
# Class depicting an RDF Graph, consisting
# both of standard nodes and blank nodes.


class RDF_graph:
    def __init__(self, blank_nodes, real_nodes, triplets_collection):
        self.blanks = blank_nodes  # Collection of blank nodes
        self.standard_nodes = real_nodes  # Collection of 'normal' nodes
        self.triplets_collection = triplets_collection  # List of all triplets
        self.component_hashvalue = {}
        self.weakly_cc = {}
        self.hash_value = ""

    def add_RDF_triple(self, triple):
        s, p, o = read_RDF_triple(triple)  # reads the rdf triple and preserves it in the database structure
        # Place both object and subject into appropriate parts of RDF graph.
        # If discussed node is already in a graph, get its reference.
        if (o.is_blank):
            if (o.name not in self.blanks):
                self.blanks.update({o.name: o})
            o = self.blanks[o.name]
        else:
            if (o.name not in self.standard_nodes):
                self.standard_nodes.update({o.name: o})
            o = self.standard_nodes[o.name]

        if (s.is_blank):
            if (s.name not in self.blanks):
                self.blanks.update({s.name: s})
            s = self.blanks[s.name]
        else:
            if (s.name not in self.standard_nodes):
                self.standard_nodes.update({s.name: s})
            s = self.standard_nodes[s.name]

        # Update proper informations in both nodes.

        if (o.is_blank):
            s.add_blank_neighbour(o, p)
        else:
            s.add_real_neighbour(o, p)

        if (s.is_blank):
            o.add_incoming_blank(s, p)
        else:
            o.add_incoming_real(s, p)

        self.triplets_collection.append([s, p, o])

    def to_string(self):
        resulting_string = ""
        for triplet in self.triplets_collection:
            resulting_string += (triplet[0].name+'\t'+triplet[1]+'\t'+triplet[2].name+'\n')
        return resulting_string

    def to_file(self, filename):
        f = open(filename, 'w')
        f.write(self.to_string().strip())
        f.close()

    def contains_bnode(self, node):
        if (node.name in self.blanks.keys()):
            return True
        else:
            return False

    def contains_gnode(self, node):
        if (node.name in self.standard_nodes.keys()):
            return True
        else:
            return False

    def hash_increment_triple(self, triplet, Hashtype='md5', Debug=False):
        s, p, o = read_RDF_triple(triplet)
        case = 0
        blank_number = 0
        if(s.is_blank_node()):
            blank_number += 1
            if(self.contains_bnode(s)):
                case += 1
        if(o.is_blank_node()):
            blank_number += 2
            if(self.contains_bnode(o)):
                case += 2
        if(case == 1):
            triplets_to_be_rehashed = [triplet for triplet in self.triplets_collection if (
                (triplet[0].name == s.name and not triplet[2].is_blank) or (triplet[2].name == s.name and not triplet[0].is_blank))]
        elif(case == 2):
            triplets_to_be_rehashed = [triplet for triplet in self.triplets_collection if (
                (triplet[0].name == o.name and not triplet[2].is_blank) or (triplet[2].name == o.name and not triplet[0].is_blank))]
        elif(case == 3):
            triplets_to_be_rehashed = [triplet for triplet in self.triplets_collection if (
                (triplet[0].name == s.name and not triplet[2].is_blank) or (triplet[2].name == s.name and not triplet[0].is_blank) or
                (triplet[0].name == o.name and not triplet[2].is_blank) or (triplet[2].name == o.name and not triplet[0].is_blank))]
        if(case != 0):
            totality_to_subtract = sum([int(hashstring(prepare_single_triplet(
                triplet[0], triplet[1], triplet[2])), 16) for triplet in triplets_to_be_rehashed])

        self.add_RDF_triple(triplet)

        if(s.is_blank_node()):
            s = self.blanks[s.name]
        else:
            s = self.standard_nodes[s.name]

        if(o.is_blank_node()):
            o = self.blanks[o.name]
        else:
            o = self.standard_nodes[o.name]

        # Case 0 -- B(G) structure was not altered
        if(case == 0):
            if(blank_number == 0):
                q = int(hashstring(prepare_single_triplet(s, p, o)), 16)
                self.hash_value = hex(int(self.hash_value, 16)+q)

            if(blank_number == 1):
                s.structure_level = 0
                s.structure_number = max(self.component_hashvalue.keys())+1
                structure = nx.Graph()
                structure.add_node(s.name)
                q = hashstring(prepare_single_component(self, structure, preparing=False), Hashtype)
                self.component_hashvalue[s.structure_number] = int(q, 16)
                # we want to remember the exact value of hash of the given connected component
                self.hash_value = hex(
                    int(self.hash_value, 16)+self.component_hashvalue[s.structure_number]+int(hashstring(prepare_single_triplet(s, p, o)), 16))

            if (blank_number == 2):
                o.structure_level = 0
                o.structure_number = max(self.component_hashvalue.keys()) + 1
                structure = nx.Graph()
                structure.add_node(o.name)
                q = hashstring(prepare_single_component(self, structure, preparing=False), Hashtype)
                self.component_hashvalue[o.structure_number] = int(q, 16)
                # we want to remember the exact value of hash of the given connected component
                self.hash_value = hex(
                    int(self.hash_value, 16) + self.component_hashvalue[o.structure_number] +
                    int(hashstring(prepare_single_triplet(s, p, o)), 16))
            if (blank_number == 3):
                s.structure_level = 0
                s.structure_number = max(self.component_hashvalue.keys()) + 1
                o.structure_level = 1
                o.structure_number = s.structure_number
                structure = nx.Graph()
                structure.add_node(s.name)
                structure.add_node(o.name)
                structure.add_edge(s.name, o.name)
                q = hashstring(prepare_single_component(self, structure, preparing=False), Hashtype)
                self.component_hashvalue[s.structure_number] = int(q, 16)
                # we want to remember the exact value of hash of the given connected component
                self.hash_value = hex(int(self.hash_value, 16) + self.component_hashvalue[s.structure_number])

        # Case 1 and 2 -- B(G) components split was not altered, but one tree needs to be rehashed.
        elif(case == 1 or case == 2):  # Case 1 -- subject is old blank node, case 2 -- object
            # Proper labeling
            if(case == 1):
                old_blank_node = s
                other_node = o
            else:
                old_blank_node = o
                other_node = s

            struct_number = old_blank_node.structure_number
            rehashed_wcc = self.weakly_cc[struct_number]

            # altering the wcc if necessary
            if (other_node.is_blank):
                other_node.structure_number = struct_number
                rehashed_wcc.add_node(other_node.name)
                if(case == 1):
                    other_node.structure_level = old_blank_node.structure_level + 1
                    rehashed_wcc.add_edge(old_blank_node.name, other_node.name)
                else:
                    rehashed_wcc.add_edge(other_node.name, old_blank_node.name)
                    prepare_single_component(self, rehashed_wcc, preparing=True)
            wcc_hash_value = int(hashstring(prepare_single_component(
                self, rehashed_wcc, preparing=False), Hashtype), 16)
            if (other_node.is_blank):
                self.hash_value = hex(int(self.hash_value, 16) -
                                      self.component_hashvalue[struct_number] + wcc_hash_value)
            else:
                self.hash_value = hex(int(self.hash_value, 16) - self.component_hashvalue[struct_number] + wcc_hash_value + int(
                    hashstring(prepare_single_triplet(s, p, o)), 16))
            totality_to_be_added = 0
            for triplet in triplets_to_be_rehashed:
                totality_to_be_added += int(hashstring(prepare_single_triplet(triplet[0], triplet[1], triplet[2])), 16)
            if(Debug == True):
                print(totality_to_subtract, totality_to_be_added)
                print("Hashed structure value:", wcc_hash_value, sep='\t')
            self.hash_value = hex(int(self.hash_value, 16) - totality_to_subtract + totality_to_be_added)

            self.component_hashvalue[struct_number] = wcc_hash_value

        # Case 3 -- both ends are blanks present within graph
        # Here, B(G) structure has been altered and two structures might need to be merged
        elif(case == 3):
            # We have two situations. In the first one, let us assume that both s and o belong to the same structure
            if(s.structure_number == o.structure_number):
                struct_number = s.structure_number
                rehashed_wcc = self.weakly_cc[struct_number]
                rehashed_wcc.add_edge(s.name, o.name)
                structure_blanks = {nodename: self.blanks[nodename]
                                    for nodename in self.weakly_cc[struct_number].nodes()}
                if(cycle_detection(structure_blanks)):
                    print("Adding the edge to graph would introduce vicious-cycle!")
                    return False
                else:
                    if(Debug):
                        print('\n')
                        printwcc(rehashed_wcc, self)
                        print('\n')
                    prepare_single_component(self, rehashed_wcc, preparing=True, Debug=True)
                    wcc_hash_value = int(hashstring(prepare_single_component(
                        self, rehashed_wcc, preparing=False), Hashtype), 16)
                    self.hash_value = hex(int(self.hash_value, 16) -
                                          self.component_hashvalue[struct_number] + wcc_hash_value)
                    self.component_hashvalue[struct_number] = wcc_hash_value
                    if (Debug == True):
                        print("Re-hashed structure value:", wcc_hash_value, sep='\t')

            else:
                firstcc = self.weakly_cc[s.structure_number]
                removed_hashnumber = o.structure_number
                secondcc = self.weakly_cc[o.structure_number]
                for node in secondcc.nodes():
                    self.blanks[node].structure_number = s.structure_number  # Combined sets
                new_cc = nx.union(firstcc, secondcc)
                prepare_single_component(self, new_cc, preparing=True)
                wcc_hash_value = int(hashstring(prepare_single_component(self, new_cc, preparing=False), Hashtype), 16)
                hashes_to_remove = self.component_hashvalue[removed_hashnumber] + \
                    self.component_hashvalue[s.structure_number]
                if (Debug == True):
                    print("Removing two structures of total hash value:",
                          self.component_hashvalue[o.structure_number], self.component_hashvalue[s.structure_number], hashes_to_remove, sep='\t')
                self.hash_value = hex(int(self.hash_value, 16) - hashes_to_remove + wcc_hash_value)
                self.component_hashvalue[s.structure_number] = wcc_hash_value
                self.component_hashvalue.pop(removed_hashnumber)
                if (Debug == True):
                    print("Hashed merged structure value:", wcc_hash_value, sep='\t')
            totality_to_be_added = 0
            for triplet in triplets_to_be_rehashed:
                q = int(hashstring(prepare_single_triplet(triplet[0], triplet[1], triplet[2])), 16)
                if (Debug == True):
                    print("Rehashing triplet: ", triplet[0].name, triplet[1],
                          triplet[2].name, " to value: ", q, sep='\t')
                totality_to_be_added += q
            self.hash_value = hex(int(self.hash_value, 16) - totality_to_subtract + totality_to_be_added)

        # Apply modulo if the operations have taken us outside of the range of standard hash values.
        self.hash_value = hex(int(self.hash_value, 16) % (2**256))


############################################

#########################################################
# Reads an RDF triple from a given string.
# We assume that RDF triples are written
# as subject-predicate-object, separated by tabs.
# Returns a triplet consisting of subject/predicate/object.

def read_RDF_triple(triple):
    s, p, o = triple.split("\t")  # this is purely for test methods,
    # we assume that RDF triples are written
    # as subject-predicate-object, separated by tabs
    return RDF_node(s), p, RDF_node(o)

#########################################################
# Reads the RDF graph from a given text file converted to array.
# Additionally, this creates a list of blank nodes.


def read_RDF_graph(RDF_array):
    blank_nodes = {}
    standard_nodes = {}
    triplets_collection = []
    rdf = RDF_graph(blank_nodes, standard_nodes, triplets_collection)
    for triple in RDF_array:
        rdf.add_RDF_triple(triple)

    return rdf

#####################################################################################

#####################################################################################
# Checks whether the given RDF graph is vicious-cycle free.
# Returns true if the blank node graph contains a cycle,
# false otherwise.

def cycle_detection(BG_graph, original_graph=None):
    queue = QQ.Queue(0)  # create a queue for vertices
    visited = []
    for neighbour in BG_graph.values():
        neighbour.temporary_degree = neighbour.blank_in_degree

    for node in list(BG_graph.values()):
        if node.blank_in_degree == 0:  # place vertices with in_degree equal to 0 on queue
            queue.put(node)
            if (original_graph != None):
                original_graph.blanks[node.name].structure_level = 0

    while not queue.empty():
        node = queue.get()
        for neighbour in node.blank_neighbours:
            # decrease the in_degree by a number
            BG_graph[neighbour].temporary_degree -= len(node.blank_neighbours[neighbour])
            # of edges from node to neighbour
            if(BG_graph[neighbour].temporary_degree == 0):  # if the in-degree of vertex is 0
                queue.put(BG_graph[neighbour])  # place it at the end of queue
                if (original_graph != None):
                    original_graph.blanks[BG_graph[neighbour].name].structure_level = original_graph.blanks[node.name].structure_level+1

        visited.append(node)
    if(len(visited) != len(BG_graph)):  # that means that some node has not been
        # visited due to existing incoming edges
        # from other unvisited vertices
        #print("B(G) graph contains a cycle")
        return True
    else:
        #print("B(G) graph is acyclic")
        return False
###############################################################

###############################################################
# Convert blank into a unique label,
# containing info on the level in the
# tree hierarchy in blank-nodes subgraph,
# as well as number of edges coming in and
# out of a given blank node.

def translate_blank_node(blank, role):
    S = "blvl:"+str(blank.structure_level) + "::bind:" + str(blank.blank_in_degree) + "::ind:" + \
        str(blank.in_degree) + "::boud:" + str(blank.blank_out_degree) + "::outd:" + str(blank.out_degree) + "::role:"
    if (role == 's'):
        return S+"Sblank"
    if (role == 'o'):
        return S+"Oblank"
###############################################################

###############################################################
# Same as above, but does not contain info on tree hierarchy.
# To be used on real nodes, it adds info on node name at the
# end of the node description.

def translate_real_node(node, role):
    S = "grounded_node::role:"
    if (role == 's'):
        S += "S"
    if (role == 'o'):
        S += "O"

    return S+"::name:"+node.name
###############################################################

###############################################################
# Converts a single triplet into a serialized string for
# hashing purposes. If triplet contains some blank nodes,
# they are automatically converted to uniquely-identifiable
# strings, representing the information on the number of the
# incoming edges, neighbours, level in the forest structure
# and so forth.

def prepare_single_triplet(subject, predicate, object):
    conversion_value = ""
    sub, obj = "", ""
    if(subject.is_blank):
        sub = translate_blank_node(subject, 's')
    else:
        sub = translate_real_node(subject, 's')
    conversion_value += sub
    conversion_value += predicate
    if(object.is_blank):
        obj = translate_blank_node(object, 'o')
    else:
        obj = translate_real_node(object, 'o')
    conversion_value += obj
    return conversion_value
###############################################################


###############################################################
# Searches for the blank node tree structures
# in given RDF database. Marks each blank node with a number
# denoting the tree substructure it belongs to.
# This additional metadata does not interfere with the
# structure of the database. This method does not return anything.

def tree_marking(RDF_database, return_as_subgraphs=False):

    # Create an undirected blank graph
    structure = nx.Graph()

    for blank in RDF_database.blanks.values():
        structure.add_node(blank.name)
    for blank in RDF_database.blanks.values():
        for blank_neighbour in blank.blank_neighbours:
            structure.add_edge(blank.name, blank_neighbour)

    # Generate connected components of undirected graph (they
    # are exactly the weakly connected components of RDF graph).
    components = [structure.subgraph(c).copy() for c in nx.connected_components(structure)]

    # For each node in selected component, mark it.
    for i in range(len(components)):
        for node in list(components[i].nodes()):
            RDF_database.blanks[node].structure_number = i

    # If subgraphs are requested as return value, return them,
    # otherwise end the procedure.
    if return_as_subgraphs:
        return components
    else:
        return
###############################################################

###############################################################
# Prepare single component for hashing. This procedure has to
# be ran twice to yield expected results. First execution labels
# every node with its level in the DAG structure of blank graph.
# Second execution turns component into a string, ready for hashing.

def prepare_single_component(RDFGraph, component, preparing, Debug=False):
    blanks = RDFGraph.blanks
    value_for_component = ""
    priority_queue = QQ.PriorityQueue()

    for node in list(component.nodes()):
        name = node
        if(Debug == True):
            print(name, blanks[name].blank_in_degree)
        blanks[name].temporary_degree = blanks[name].blank_in_degree
        if blanks[name].blank_in_degree == 0:  # place vertices with blank in_degree equal to 0 on queue
            priority_queue.put((blanks[name].generate_priority_tuple(RDFGraph), blanks[name]))
            blanks[name].structure_level = 0  # mark nodes added initially on the queue as tier_0

    while not priority_queue.empty():
        node = priority_queue.get()
        node = node[1]
        ###############
        # Get all edges coming out of node for hashing
        if(not preparing):
            miniqueue = QQ.PriorityQueue()
            for neighbour in node.blank_neighbours:
                miniqueue.put((blanks[neighbour].generate_priority_tuple(RDFGraph), blanks[neighbour]))
            while not miniqueue.empty():
                neighbour = miniqueue.get()[1]
                for predicate in sorted(node.blank_neighbours[neighbour.name]):
                    value_for_component += prepare_single_triplet(node, predicate, neighbour)

        ################
        # Proceed with handling subsequent parts of our DAG.
        for neighbour in node.blank_neighbours:
            # decrease the in_degree by a number
            blanks[neighbour].temporary_degree -= len(node.blank_neighbours[neighbour])
            # of edges from node to neighbour
            if(blanks[neighbour].temporary_degree == 0):  # if the in-degree of vertex is 0
                if(preparing):
                    blanks[neighbour].structure_level = node.structure_level+1  # update its node level
                priority_queue.put((blanks[neighbour].generate_priority_tuple(RDFGraph),
                                   blanks[neighbour]))  # place it at the end of queue

    return value_for_component
###############################################################

###############################################################
# Prepares a hash from a given string, according to the
# selected hashing algorithm. Returns value in hexadecimal.

def hashstring(string, Hashtype='md5'):
    # Some other variants can be easily implemented as well.
    total_hash = None
    if (Hashtype == 'md5'):
        total_hash = hlib.md5()
    elif (Hashtype == 'sha256'):
        total_hash = hlib.sha256()
    elif (Hashtype == 'sha512'):
        total_hash = hlib.sha512()
    elif (Hashtype == 'sha1'):
        total_hash = hlib.sha1()
    elif (Hashtype == 'blake2b'):
        total_hash = hlib.blake2b()
    elif (Hashtype == 'blake2s'):
        total_hash = hlib.blake2s()
    elif (Hashtype == 'sha3_256'):
        total_hash = hlib.sha3_256()
    elif (Hashtype == 'sha3_512'):
        total_hash = hlib.sha3_512()
    
    total_hash.update(string.encode('utf-8'))
    return total_hash.hexdigest()

###############################################################


###############################################################
# Hashes whole RDF database. The general principle is based on
# Sopek, M., Gradzki, P., Kosowski, W., Kuziński, D., Trójczak,
# R., & Trypuz, R. (2018). "GraphChain. Companion of the The
# Web Conference 2018" on The Web Conference 2018.
###
# The difference between our approach and the one presented
# in the paper above is the way we approach the problem of
# hashing blank nodes structures.

def hash_database(RDF_database, Hashtype='md5', Debug=False):
    hash_value_for_database = 0

    # Mark weakly connected components of RDF database and get them:
    weakly_cc = tree_marking(RDF_database, return_as_subgraphs=True)
    RDF_database.weakly_cc = weakly_cc

    for component in weakly_cc:
        lead_node = list(component.nodes())[0]
        # Assign proper structure levels to all blank nodes
        prepare_single_component(RDF_database, component, preparing=True)
        q = hashstring(prepare_single_component(RDF_database, component, preparing=False), Hashtype)
        RDF_database.component_hashvalue[RDF_database.blanks[lead_node].structure_number] = int(q, 16)
        # we want to remember the exact value of hash of the given connected component
        hash_value_for_database += RDF_database.component_hashvalue[RDF_database.blanks[lead_node].structure_number]
        if (Debug):
            print("Substructure hashed to the value ", str(RDF_database.component_hashvalue[RDF_database.blanks[lead_node].structure_number]),
                  " for a total hashvalue of ", str(hash_value_for_database),
                  sep='\t')

    for triplet in RDF_database.triplets_collection:
        if (triplet[0].is_blank and triplet[2].is_blank):
            continue
        else:
            if (Debug):
                print(triplet[0].name, triplet[1], triplet[2].name, sep='\t')
            q = int(hashstring(prepare_single_triplet(triplet[0], triplet[1], triplet[2]), Hashtype), 16)
            hash_value_for_database += q
            if (Debug):
                print("Triplet hashed to the value ", str(q),
                      " for a total hashvalue of ", str(hash_value_for_database), sep='\t')

    RDF_database.hash_value = hex(hash_value_for_database % (2**256))
    return RDF_database.hash_value
###############################################################

###############################################################

def readRDFLibGraph(RDFLibGraph: rdflib.Graph):
    triples = []
    for s, p, o in RDFLibGraph:
        subject = s.n3()
        if isinstance(s, rdflib.BNode):
            subject = "blank" + subject[2:]
        else:
            subject = subject[1:-1]
        pred = p.n3()[1:-1]
        object = o.n3()
        if isinstance(o, rdflib.BNode):
            object = "blank" + object[2:]
        elif isinstance(o, rdflib.URIRef):
            object = object[1:-1]
        else:
            object = object.replace("\t", " ")
        triples.append(subject + "\t" + pred + "\t" + object)
    return triples



def printwcc(wcc, RDF_graph):
    for node in wcc.nodes():
        print(translate_blank_node(RDF_graph.blanks[node], 's') + '\t' + node)

parser = argparse.ArgumentParser(
    description='RDF files compare', add_help=False, prog='rdfcompare')
informative = parser.add_argument_group('Informative arguments')
informative.add_argument("-h", "--help", help='show this help message and exit', action="help")

required = parser.add_argument_group('Required arguments')
required.add_argument("-f1", "--format1", choices=['turtle', 'ttl', 'n3', 'notation3', 'ntriples', 'nt', 'n-triples', 'rdfxml', 'xml', 'jsonld', 'json-ld', 'json'],
    help="first input format: N-Triples: nt, ntriples, n-triples | Turtle: turtle, ttl, n3, notation3 | RDF/XML: xml, rdfxml | JSON-LD: json, json-ld, jsonld", required=False, default='ttl')
required.add_argument("-f2", "--format2", choices=['turtle', 'ttl', 'n3', 'notation3', 'ntriples', 'nt', 'n-triples', 'rdfxml', 'xml', 'jsonld', 'json-ld', 'json'],
    help="first input format: N-Triples: nt, ntriples, n-triples | Turtle: turtle, ttl, n3, notation3 | RDF/XML: xml, rdfxml | JSON-LD: json, json-ld, jsonld", required=False, default='ttl')
required.add_argument("-a", "--algorithm", choices=['md5', 'sha1', 'sha256', 'sha512', 'sha3_256', 'sha3_512', 'blake2b', 'blake2s'],
    help="hash function: MD5, SHA1, SHA2 (SHA256, SHA512), SHA3 (SHA3 256, SHA 512), BLAKE2 (BLAKE2b, BLAKE2s)", required=False, default='md5')
required.add_argument('file1', type=str, help='First RDF file')
required.add_argument('file2', type=str, help='Second RDF file')

args = parser.parse_args()

if args.format1 == 'turtle' or args.format1 == 'ttl' or args.format1 == 'n3' or args.format1 == 'notation3' or args.format1 == 'ntriples' or args.format1 == 'nt':
    serialization1 = 'turtle'
elif args.format1 == 'xml' or args.format1 == 'rdfxml':
    serialization1 = 'xml'
elif args.format1 == 'json' or args.format1 == 'json-ld' or args.format1 == 'jsonld':
    serialization1 = 'json-ld'

if args.format2 == 'turtle' or args.format2 == 'ttl' or args.format2 == 'n3' or args.format2 == 'notation3' or args.format2 == 'ntriples' or args.format2 == 'nt':
    serialization2 = 'turtle'
elif args.format2 == 'xml' or args.format2 == 'rdfxml':
    serialization2 = 'xml'
elif args.format2 == 'json' or args.format2 == 'json-ld' or args.format2 == 'jsonld':
    serialization2 = 'json-ld'

start_global_time = process_time()
if args.file1:
    # Time for some test:
    g = rdflib.Graph()
    print("==================================")

    print("Rdflib parsing:", end=" ")
    start_time = process_time()
    g.parse(args.file1, format=serialization1)
    print("%s seconds" % (process_time() - start_time))
    print(f"Graph has {len(g)} triples.")

    print("Conveting to abstract triples:", end=" ")
    start_time = process_time()
    triples = readRDFLibGraph(g)
    print("%s seconds" % (process_time() - start_time))

    print("Reading triples into abstract graph:", end=" ")
    start_time = process_time()
    RDFgraph = read_RDF_graph(triples)
    print("%s seconds" % (process_time() - start_time))

    #f = open(".\\testfiles\\problematic_graph.txt", "r")
    #RDFgraph = read_RDF_graph(f.read().split('\n'))

    print("Deepcopy of blank nodes:", end=" ")
    start_time = process_time()
    BG = copy.deepcopy(RDFgraph.blanks)
    print("%s seconds" % (process_time() - start_time))

    print("Cycle detection:", end=" ")
    start_time = process_time()
    cycle = cycle_detection(BG)
    print("%s seconds" % (process_time() - start_time))
    print(f"Graph has {'no' if not cycle else ''} vicious circles")

    print("Hashing graph:", end=" ")
    start_time = process_time()
    hash1 = hash_database(RDFgraph, args.algorithm)
    print("%s seconds" % (process_time() - start_time))
    print("Graph hash:", hash1)


if args.file2:
    # Time for some test:
    g = rdflib.Graph()
    print("==================================")

    print("Rdflib parsing:", end=" ")
    start_time = process_time()
    g.parse(args.file2, format=serialization2)
    print("%s seconds" % (process_time() - start_time))
    print(f"Graph has {len(g)} triples.")

    print("Conveting to abstract triples:", end=" ")
    start_time = process_time()
    triples = readRDFLibGraph(g)
    print("%s seconds" % (process_time() - start_time))

    print("Reading triples into abstract graph:", end=" ")
    start_time = process_time()
    RDFgraph = read_RDF_graph(triples)
    print("%s seconds" % (process_time() - start_time))

    #f = open(".\\testfiles\\problematic_graph.txt", "r")
    #RDFgraph = read_RDF_graph(f.read().split('\n'))

    print("Deepcopy of blank nodes:", end=" ")
    start_time = process_time()
    BG = copy.deepcopy(RDFgraph.blanks)
    print("%s seconds" % (process_time() - start_time))

    print("Cycle detection:", end=" ")
    start_time = process_time()
    cycle = cycle_detection(BG)
    print("%s seconds" % (process_time() - start_time))
    print(f"Graph has {'no' if not cycle else ''} vicious circles")

    print("Hashing graph:", end=" ")
    start_time = process_time()
    hash2 = hash_database(RDFgraph, args.algorithm)
    print("%s seconds" % (process_time() - start_time))
    print("Graph hash:", hash2)

    print("\n==================================\n")

print("Global time: %s seconds" % (process_time() - start_global_time))
if hash1 == hash2:
    print("The RDF files are the same")
else:
    print("The RDF files are different")