import sys
import socket

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, serialize_DELETE
from node_ring import NodeRing

BUFFER_SIZE = 1024


class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):
    client_ring = NodeRing(udp_clients)
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        get_ch_node = client_ring.get_ch_node(key)
        response = get_ch_node.send(data_bytes)
        replica = replica_map[get_ch_node]
        print("Replicating in next node")
        replica.send(data_bytes)
        print(response)
        hash_codes.add(str(response.decode()))

    print(
        f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}"
    )

    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        get_ch_node = client_ring.get_ch_node(key)
        response = get_ch_node.send(data_bytes)
        replica = replica_map[get_ch_node]
        print("Response from corresponding node")
        print(response)
        print("Getting response from replica node")
        rep_response = replica.send(data_bytes)
        print(rep_response)


if __name__ == "__main__":
    clients = [UDPClient(server['host'], server['port']) for server in NODES]
    replica_map = {}
    for i in range(len(clients)):
        if (i != len(clients) - 1):
            replica_map[clients[i]] = clients[i + 1]
        else:
            replica_map[clients[i]] = clients[0]

    process(clients)
