from work_queue.keys import stream_key
from work_queue.producer import JobProducer


def test_producer_creates_a_valid_stream_entry(redis_client, sample_job):
    producer = JobProducer(redis_client)

    entry_id = producer.enqueue(sample_job)

    entries = redis_client.xrange(stream_key())
    assert len(entries) == 1
    stored_id, fields = entries[0]
    assert stored_id == entry_id
    assert fields == sample_job.to_stream_fields()
