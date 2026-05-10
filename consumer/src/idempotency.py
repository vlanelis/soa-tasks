from cassandra_client import CassandraClient

_client = CassandraClient()

def is_processed(event_id):
    return _client.is_event_processed(event_id)

def mark_processed(event_id):
    _client.mark_processed(event_id)
