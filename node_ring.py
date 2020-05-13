import hashlib
import struct
import sys

from pickle_hash import hash_code_hex
from server_config import NODES
from ConsistentHash import ConsistentHash


class NodeRing():
    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes

    def weight(self, port, key):
        a = 1103515245
        b = 12345
        key = key.strip()
        hash = hash_code_hex(key.encode())
        hex = int(hash, 16)
        return (a * ((a * port + b) ^ hex) + b) % (2 ^ 31)

    def get_node(self, key_hex):
        key = int(key_hex, 16)
        node_index = key % len(self.nodes)
        return self.nodes[node_index]

    def get_hrw_node(self, key_hex):
        weights = []
        for node1 in self.nodes:
            w = self.weight(int(node1.port), key_hex)
            weights.append((w, node1))
        _, node = max(weights)
        return node

    def get_ch_node(self, key):
        ch = ConsistentHash(len(self.nodes), 8, 2)
        #print("(Actual_node,Virtual_node,Hash_value):")
        """for (j, k, h) in ch.hash_tuples:
            print("(%s,%s,%s)" % (j, k, h))
        print("Hash Value of the sent Key" + str(
            int((int(hash_code_hex(key.encode()), 16) % 1000000) / 10000.0)))
        print("NODE NUMMMMM" + str(ch.get_machine(key) - 1))
        print("Going to node" +
              str(self.nodes[(ch.get_machine(key)) - 1].port))"""
        index = ch.get_machine(key)
        #print("Indexxxx" + str(index))
        return self.nodes[int(index)]


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
# test()
