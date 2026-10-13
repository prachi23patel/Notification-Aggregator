import requests
import uuid

for i in range(10):
    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": "TEST",
        "user_id": 1,
        "source": "test_script",
        "payload": {"index": i}
    }
    resp = requests.post("http://localhost:8000/events", json=event)
    print(resp.status_code, event["event_id"])