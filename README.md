# Consistent Hashing and RHW Hashing

The distributed cache you implemented in the midterm is based on naive modula hashing to shard the data.

## Part I.

Implement Rendezvous hashing to shard the data.

### Implementation screenshots and explanation

Based on weight of the node, request is routed to highest weighed node as shown below


Any further request with that hash value will be routed to same node as shown below

![](HRW2.png)

## Part II.

Implement consistent hashing to shard the data.

Features:

* Add virtual node layer in the consistent hashing.
* Implement virtual node with data replication. 
