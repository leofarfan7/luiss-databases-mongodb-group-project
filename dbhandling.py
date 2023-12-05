from pymongo import MongoClient
import csv
import ast


def date_convert(date: str) -> str:
    if date == "" or date[0:3] == "rel":
        return None
    month_dict = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }
    day = date[4:6]
    month = month_dict[date[0:3]]
    year = date[-4:]
    return f"{year}-{month}-{day}"


def num_variable_processing(number: str):
    if number == "":
        return None
    elif "K" in number:
        return int(float(number[:-1]) * 1000)
    elif "." in number:
        return float(number)
    else:
        return int(number)


def multivalued_processing(original_string: str):
    if original_string != "":
        return ast.literal_eval(original_string)
    else:
        return []


def insert_data(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        videogames_collection = db.videogames
        genres_collection = db.genres
        reviews_collection = db.reviews

        with open("./games.csv") as file:
            dataset = csv.reader(file, delimiter=",")
            next(dataset)
            rows_read, rows_inserted = 0, 0
            skipped_rows = 0
            all_game_titles = set()
            for row in dataset:
                rows_read += 1
                skip_to_next_row = False

                # String values
                game_title = row[1]
                summary = row[8]

                # Date values
                release_date = date_convert(row[2])

                # Numeric values
                rating = num_variable_processing(row[4])
                times_listed = num_variable_processing(row[5])
                num_reviews = num_variable_processing(row[6])
                plays = num_variable_processing(row[10])
                playing = num_variable_processing(row[11])
                backlogs = num_variable_processing(row[12])
                wishlist = num_variable_processing(row[13])

                # (Potential) Multivalued Attributes
                developers = multivalued_processing(row[3])
                genres = multivalued_processing(row[7])
                reviews = multivalued_processing(row[9])

                videogame_doc = {
                    "game_title": game_title,
                    "release_date": release_date,
                    "developers": developers,
                    "rating": rating,
                    "times_listed": times_listed,
                    "num_reviews": num_reviews,
                    "summary": summary,
                    "plays": plays,
                    "playing": playing,
                    "backlogs": backlogs,
                    "wishlist": wishlist,
                }

                # Check for repeated rows
                if game_title in all_game_titles:
                    existing_games = videogames_collection.find({"game_title": game_title})
                    for existing_game in existing_games:
                        existing_game_summary = existing_game["summary"]
                        existing_game_id = existing_game["_id"]
                        if summary == existing_game_summary:
                            skip_to_next_row = True
                            reviews_from_dup_game = list(reviews_collection.find({"game_id": existing_game_id}, {"_id": 0, "content": 1}))
                            for review in reviews:
                                if review not in reviews_from_dup_game:
                                    reviews_collection.insert_one({"content": review, "game_id": existing_game_id})

                if skip_to_next_row:
                    skipped_rows += 1
                    continue

                inserted_videogame = videogames_collection.insert_one(videogame_doc)
                all_game_titles.add(game_title)

                for genre in genres:
                    genres_collection.update_one({"name": genre}, {"$addToSet": {"games": inserted_videogame.inserted_id}}, upsert=True)

                for review in reviews:
                    reviews_collection.insert_one({"content": review, "game_id": inserted_videogame.inserted_id})

                rows_inserted += 1

            print(f"{rows_read} row(s) have been read successfully.")
            print(f"{rows_inserted} distinct row(s) have been inserted successfully.")
            print(f"{skipped_rows} row(s) have been skipped because of duplicated data.")


def reset_db(port=27017):
    with MongoClient("localhost", port) as client:
        for database in client.list_databases():
            if database["name"] == "popular_videogames":
                client.drop_database("popular_videogames")
