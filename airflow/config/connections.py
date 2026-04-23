from airflow.models import Connection
from airflow import settings
import json

# Path to your credentials file
FILE_PATH = "connections.json"

def sync_connections():
    session = settings.Session()

    # Load connections from JSON
    with open(FILE_PATH) as f:
        data = json.load(f)

    for conn_id, conn_info in data.items():
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if existing_conn:
            print(f"Updating existing connection: {conn_id}")
            existing_conn.conn_type = conn_info.get("conn_type")
            existing_conn.host = conn_info.get("host")
            existing_conn.login = conn_info.get("login")
            existing_conn.password = conn_info.get("password")
            existing_conn.schema = conn_info.get("schema")
            existing_conn.extra = conn_info.get("extra")
        else:
            print(f"Creating new connection: {conn_id}")
            new_conn = Connection(
                conn_id=conn_id,
                conn_type=conn_info.get("conn_type"),
                host=conn_info.get("host"),
                login=conn_info.get("login"),
                password=conn_info.get("password"),
                schema=conn_info.get("schema"),
                extra=conn_info.get("extra")
            )
            session.add(new_conn)

    session.commit()
    print("Connections synced successfully!")

if __name__ == "__main__":
    sync_connections()