# Python program to print all paths from a source to destination.
# source: https://www.geeksforgeeks.org/find-paths-given-source-destination/
from collections import defaultdict
from copy import deepcopy


def get_sources_sinks(streams=None, operators=None):
    """
    get the list of source and sink operators
    Args:
        streams: the vector of streams
        operators: the vector of all operators
    Returns:
        a vector of source and another of sink operators
    """
    if operators is None:
        operators = []

    if streams is None:
        streams = []

    sinks = []
    sources = []
    for o in range(len(operators)):
        i = 0
        j = 0
        for s in range(len(streams)):
            if streams[s][0] == operators[o]:
                i += 1

            if streams[s][1] == operators[o]:
                j += 1

        if i == 0 and j > 0:
            sinks.append(operators[o])

        if i > 0 and j == 0:
            sources.append(operators[o])
    return sources, sinks


# This class represents a directed graph
# using adjacency list representation
class Graph:

    def __init__(self, vertices):
        # No. of vertices
        self.V = vertices

        # default dictionary to store graph
        self.graph = defaultdict(list)

        self.all_paths = []

    # function to add an edge to graph
    def addEdge(self, u: int, v: int):
        if int(v) not in self.graph[u]:
            self.graph[u].append(int(v))

    '''A recursive function to print all paths from 'u' to 'd'. 
    visited[] keeps track of vertices in current path. 
    path[] stores actual vertices and path_index is current 
    index in path[]'''

    def printAllPathsUtil(self, u: int, d: int, visited, path):

        # Mark the current node as visited and store in path
        visited[u] = True
        path.append(u)

        # If current vertex is same as destination, then print
        # current path[]
        if u == d:
            # print(path)
            self.all_paths.append(deepcopy(path))
        else:
            # If current vertex is not destination
            # Recur for all the vertices adjacent to this vertex
            for i in self.graph[u]:
                if not visited[i]:
                    self.printAllPathsUtil(i, d, visited, path)

                    # Remove current vertex from path[] and mark it as unvisited
        path.pop()
        visited[u] = False

    # Prints all paths from 's' to 'd'
    def printAllPaths(self, s, d):

        # Mark all the vertices as not visited
        visited = [False] * (self.V)

        # Create an array to store paths
        path = []

        # Call the recursive helper function to print all paths
        self.printAllPathsUtil(s, d, visited, path)

    def get_paths(self, src_device_id=-1, dst_device_id=-1, virtual_nodes=[]):
        paths = []

        self.printAllPaths(src_device_id, dst_device_id)

        # remove virtual operators
        if len(virtual_nodes) > 0:
            for i in range(len(self.all_paths)):
                p = []
                for j in range(len(self.all_paths[i])):
                    if self.all_paths[i][j] not in virtual_nodes:
                        p.append(self.all_paths[i][j])
                if len(p) > 0:
                    paths.append(p)

            self.all_paths = paths

        return self.all_paths

