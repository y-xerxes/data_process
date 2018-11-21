from pyspark import RDD


class BroadcastJoin(object):
    """
    RDD和一个hashmap之间做join，基于mapPartition做
    """
    @staticmethod
    def join(rdd: RDD, hash_value: dict) -> RDD:
        def map_partitions(rows, local_hash_value=None):
            rows = list(rows)
            result = []

            if local_hash_value is None:
                local_hash_value = {}

            for row in rows:
                key = row[0]
                value = row[1]
                result.append((key, (value, local_hash_value.get(key))))

            return result

        return rdd.mapPartitions(lambda rows: map_partitions(rows=rows, local_hash_value=hash_value))
