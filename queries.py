from pymongo import MongoClient
from operator import itemgetter


def queries_list():
    return [
        "1. Storage size of the database",
        "2. Quantity of documents in the database",
        "3. Top 10 videogames by rating",
        "4. Top 10 videogames by playing",
        "5. Rank of genres by number of videogames",
        "6. Rank of genres by average rating",
    ]


def execute_query(choice: int, port=27017):
    match choice:
        case 1:
            database_storage_size(port)
        case 2:
            database_documents_per_collection(port)
        case 3:
            videogames_top_ten_by_rating(port)
        case 4:
            videogames_top_ten_by_playing(port)
        case 5:
            genres_most_games(port)
        case 6:
            genres_average_rating(port)


def database_storage_size(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        database_size = 0
        for collection_name in db.list_collection_names():
            collection_size = db.command({"collStats": collection_name})["storageSize"]
            database_size += collection_size
            print(f"Collection '{collection_name}' | Storage Size: {collection_size / (1024 * 1024):.2f} MB")
        print(f"\nTotal Database Storage Size: {database_size/(1024*1024):.2f} MB")
        print(
            "\nRemark: If you just created the database, please allow MongoDB a minute to calculate the storage size for each collection."
        )


def database_documents_per_collection(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        database_documents = 0
        for collection_name in db.list_collection_names():
            document_count = db[collection_name].count_documents({})
            database_documents += document_count
            print(f"Collection '{collection_name}' | Document Count: {document_count}")
        print(f"\nTotal Documents in Database: {database_documents}")


def videogames_top_ten_by_rating(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        top_rated_games = db.videogames.find().sort("rating", -1).limit(10)
        count = 0
        for game in top_rated_games:
            count += 1
            print(f"{count}. {game['game_title']} | Rating: {game['rating']}")


def videogames_top_ten_by_playing(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        pipeline = [{"$sort": {"playing": -1}}, {"$limit": 10}]
        top_playing_games = db.videogames.aggregate(pipeline)
        count = 0
        for game in top_playing_games:
            count += 1
            print(f"{count}. {game['game_title']} | Active Players: {game['playing']}")


def genres_most_games(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        pipeline = [{"$project": {"_id": 0, "name": 1, "num_games": {"$size": "$games"}}}, {"$sort": {"num_games": -1}}]
        top_genres = db.genres.aggregate(pipeline)
        count = 0
        for genre in top_genres:
            count += 1
            print(f"{count}. {genre['name']} | Number of Videogames: {genre['num_games']}")


def genres_average_rating(port=27017):
    with MongoClient("localhost", port) as client:
        db = client.popular_videogames
        genres = db.genres.find({})
        averages = {}
        for genre in genres:
            genre_name = genre["name"]
            total_rating = 0
            num_ratings = 0
            for game in genre["games"]:
                videogame_info = db.videogames.find_one({"_id": game})
                videogame_rating = videogame_info["rating"]
                if videogame_rating != None:
                    total_rating += videogame_rating
                    num_ratings += 1
            averages[genre_name] = round(total_rating / num_ratings, 2)
        sorted_averages = dict(sorted(averages.items(), key=itemgetter(1), reverse=True))
        count = 0
        for genre, average in sorted_averages.items():
            count += 1
            print(f"{count}. {genre} | Average Rating: {average}")
