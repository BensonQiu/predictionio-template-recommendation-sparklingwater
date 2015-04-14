# Script to test engine queries
import predictionio
engine_client = predictionio.EngineClient(url="http://localhost:8000")
print engine_client.send_query({"circuit_id": 50, "time": "1422572400"})
