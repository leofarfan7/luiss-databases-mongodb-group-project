from pymongo import MongoClient
from pymongo import errors
import dbhandling, queries


def print_text(level: int, text: str):
    if level == 1:
        print("\n\n\n\n\n" + "=" * 51)
        print(text)
        print("=" * 51)
    elif level == 2:
        print("\n\n\n\n\n===", text, "===")
    elif level == 3:
        print("\n\n\n---", text, "---")
    elif level == 4:
        print("\n>>>", text)
    elif level == 5:
        print(">", text, "<")
    else:
        print("\n", text)


def select_query():
    print_text(2, "Database Querying")
    queries_list = queries.queries_list()
    for query in queries_list:
        print(query)
    query_choice = int(input("\n> Please indicate the number of the query to execute: "))
    return query_choice


def main():
    print_text(1, "Welcome to the Popular Videogames 1980-2023 Dataset")
    print_text(3, "Created by")
    group_members = {
        "Shefik Memedi",
        "Yassir El Arrag",
        "Martina Fagnani",
        "Leonardo Farfan",
        "Edoardo Brown",
    }
    for member in group_members:
        print(f"• {member}")

    print_text(2, "Initialization")

    initialize = False
    while initialize not in ("Y", "N"):
        initialize = input("\n> Would you like to initialize the database from zero? (Y/N): ").capitalize()

    port = False
    while port == False:
        port = input("\n> Please type your MongoDB port (press enter for 27017):")
        if port == "":
            port = 27017
        try:
            client = MongoClient("localhost", port)
            client.admin.command("ping")
            print("\nConnection established successfully!")
        except errors.ServerSelectionTimeoutError as err:
            print(f"\nError in connection: {err}")
            print("\nDid you start your MongoDB server? Is the port correct?")
            port = False
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return

    if initialize == "N":
        with MongoClient("localhost", port) as client:
            db_exists = False
            for database in client.list_databases():
                if database["name"] == "popular_videogames":
                    db_exists = True
            if db_exists == False:
                print("\nThe database doesn't exist.")
                initialize = "Y"

    if initialize == "Y":
        print_text(4, "Creating database...")
        dbhandling.reset_db(port)
        print_text(4, "Inserting data...")
        dbhandling.insert_data(port)

    while True:
        query_choice = select_query()
        print_text(4, "Executing query...\n")
        queries.execute_query(query_choice, port)
        print_text(3, "End of Query")
        retry = input("\n> Do you want to execute another query? (Y/N): ").capitalize()
        if retry != "Y":
            print_text(4, "Closing program...\n")
            break


if __name__ == "__main__":
    main()
