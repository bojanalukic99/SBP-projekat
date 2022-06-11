from pymongo import MongoClient
from pprint import pprint


def getClient():
    client = MongoClient('mongodb://localhost:27017')
    db = client.admin
    serverStatusResult = db.command("serverStatus")

    return client

def runtimeAndRatingAbove():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movies_metadata"]


    queryAgg = [{"$lookup": {
                "from": "ratings",
                "localField": "id",
                "foreignField": "movieId",
                "as": "movie_rating"
                }}]

    result = document.aggregate(queryAgg)

    myquery = {"runtime": {"$gt": 200}, "rating": {"$gt": 3}}

    movies = result.find(myquery)

    pprint(movies)

    for x in movies:
        print(x)



def originalLanguageFrAndSort():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movies_metadata"]

    myquery = {"original_language": "fr"}

    movies = document.find(myquery).sort("budget", 1)

    #sort by budget

    pprint(movies)

    for x in movies:
        print(x)

def moreWomenRoles():
    client = getClient()
    mydb = client["movies"]
    document = mydb["credits"]


    myquery = {"$unwind": "$cast"} #, {"$group": {"_id": {"gender": "$cast.gender"}, "conut": {"$sum": 1}}}, {"$match": {"count": {"$gt": 1}}}

    movies = document.aggregate([myquery])

    pprint(movies)

    for x in movies:
        print(x)

def directorWithMostMovies():
    client = getClient()
    mydb = client["movies"]
    document = mydb["credits"]

    myquery = {"crew.job": "Director"}

    movies = document.find(myquery)

    pprint(movies)

    for x in movies:
        print(x)

def groupByRating():
    client = getClient()
    mydb = client["movies"]
    document = mydb["ratings"]

    myquery = [{"$group": {"_id": "$rating", "value": {"$sum": 1}}}, {"$sort": {"_id": -1}}]

    movies = document.aggregate(myquery)

    for x in movies:
        print(x)

def movieWithMostGenres():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movies_metadata"]

    myquery = [{"$group" : {"_id" : "$production_companies", "maximum" : {"$max" : {"$size": "$genres"}}}}, {"$sort": {"maximum": -1}}, {"$limit": 1}]

    movies = document.aggregate(myquery)

   # pprint(len(list(movies)))

    for x in movies:
        print(x)

def mostFamousGenreByYears():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movies_metadata"]

    myquery = [{"$group": {"_id": {"$year": "$release_date"}}}]
    movies = document.aggregate(myquery)

    pprint(movies)

    for x in movies:
        print(x)

def longestMovieByDirector():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movie_metadata"]

    queryAgg = [{"$lookup": {
        "from": "credits",
        "localField": "id",
        "foreignField": "movieId",
        "as": "movie_rating"
    }}]

    result = document.aggregate(queryAgg)

    myquery = {""}

    movies = document.find(myquery)

    pprint(movies)

    for x in movies:
        print(x)

def moreLanguages():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movies_metadata"]

    myquery = {"$unwind": "$spoken_languages"} #, {"$group": {"_id": {"lengusage": "$spoken_languages"}, "conut": {"$sum": 1}}}, {"$match": {"count": {"$gt": 1}}}

    movies = document.aggregate([myquery])

    pprint(movies)

    for x in movies:
        print(x)


def movieVoteCountByProductionCompany():
    client = getClient()
    mydb = client["movies"]
    document = mydb["movies_metadata"]

    myquery = {"$unwind": "$production_companies"} #, {"$group": {"_id": {"company": "$production_companies"}}}, {"$match": {"vote_count": {"$gt": 2000}}}

    movies = document.aggregate([myquery])

    pprint(movies)

    for x in movies:
        print(x)

#runtimeAndRatingAbove();
#originalLanguageFrAndSort();
#groupByRating();
#moreWomenRoles();
#directorWithMostMovies();
#movieWithMostGenres();
#mostFamousGenreByYears();
#movieVoteCountByProductionCompany();