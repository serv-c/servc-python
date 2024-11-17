from servc.server import start_server


def test_resolver(id, bus, cache, payload, _c):
    if not isinstance(payload, list):
        return False
    for x in payload:
        if not isinstance(x, str):
            return False
    return True


def main():
    return start_server(
        resolver={
            "test": test_resolver,
        },
        # route="test",
    )


if __name__ == "__main__":
    main()
