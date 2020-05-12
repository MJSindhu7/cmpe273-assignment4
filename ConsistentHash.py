import bisect
import hashlib
from pickle_hash import hash_code_hex


class ConsistentHash:
    def __init__(self, num_machines=1, v_nodes=1, replica_factor=1):
        self.num_machines = num_machines
        self.v_nodes = v_nodes
        self.replica_factor = replica_factor

        hash_tuples = [(j,k,int((int(hash_code_hex((str(j)+"_"+str(k)).encode()), 16) % 1000000) / 10000.0)) \
                       for j in range(self.num_machines) \
                       for k in range(self.v_nodes)]
        hash_tuples.sort(key=lambda x: x[2])
        self.hash_tuples = hash_tuples

    def get_machine(self, key):
        h = int((int(hash_code_hex(key.encode()), 16) % 1000000) / 10000.0)
        if h > self.hash_tuples[-1][2]:
            return self.hash_tuples[0][0]
        hash_values = map(lambda x: x[2], self.hash_tuples)
        index = bisect.bisect_left(list(hash_values), h)
        return self.hash_tuples[index][0]
