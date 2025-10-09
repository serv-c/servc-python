from servc.server import start_server

from src.config import QUEUE_NAME
from src import resolvers

def main():
    start_server(
        resolver=resolvers,
        route=QUEUE_NAME,
    )

if __name__ == "__main__":
    main()