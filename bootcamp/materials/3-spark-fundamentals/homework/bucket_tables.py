from config import create_spark

def bucket_and_save():
    spark = create_spark()

    match_details = spark.read.csv("/home/karthigaiselvan/Documents/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/match_details.csv",header=True)
    matches = spark.read.csv("/home/karthigaiselvan/Documents/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/matches.csv", header=True)
    medals_matches_players = spark.read.csv("/home/karthigaiselvan/Documents/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/medals_matches_players.csv", header=True)

    # Repartition on match_id and write bucketed tables
    match_details.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_match_details")
    matches.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_matches")
    medals_matches_players.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_medals_matches_players")

if __name__ == "__main__":
    bucket_and_save()
