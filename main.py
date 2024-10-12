from servc.com.server.server import start_server

def main():
    return start_server(
        resolver={},
        eventResolver={},
    )

if __name__ == '__main__':
    main()