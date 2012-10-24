This project implemented a real-time distributed frequent pattern mining algorithm using Storm - the distributed and fault-tolerant realtime computating platform

The idea of this algorithm is the following steps:

1. Calculate transaction set for each single item count, find frequent single item

2. Get <transaction, item> pairs of the frequent single item, build candidate item set of each transaction

3. Calculate transaction set for each candidate item set, find frequent item set