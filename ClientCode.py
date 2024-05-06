from scripts.ConcreteCreator import (
    Creator, CreatorExtraction, CreatorReporting, CreatorProcessing)


def client_code(creator: Creator) -> None:
    """
    The client code works with an instance of a concrete creator, albeit through
    its base interface. As long as the client keeps working with the creator via
    the base interface, you can pass it any creator's subclass.
    """

    print(f"Client: I'm not aware of the creator's class, but it still works.\n"
          f"{creator.some_operation()}", end="")


if __name__ == "__main__":
    print("App: Launched with the CreatorExtraction.")
    client_code(CreatorExtraction())
    print("\n")

    print("App: Launched with the CreatorReporting.")
    client_code(CreatorReporting())
    print("\n")

    print("App: Launched with the CreatorProcessing.")
    client_code(CreatorProcessing())
    print("\n")