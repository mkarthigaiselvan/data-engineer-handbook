from config import create_spark
from pyspark.sql.functions import col, avg, count, desc, broadcast

def aggregate_analysis():
    spark = create_spark()

    # Load bucketed tables from Hive
    match_details = spark.table("bucketed_match_details")
    matches = spark.table("bucketed_matches")
    medals_matches_players = spark.table("bucketed_medals_matches_players")

    # Load reference CSVs and broadcast
    medals = broadcast(
        spark.read.option("header", True).option("inferSchema", True)
        .csv("/home/karthigaiselvan/Documents/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/medals.csv")
        .withColumnRenamed("name", "medal_name")
    )

    maps = broadcast(
        spark.read.option("header", True).option("inferSchema", True)
        .csv("/home/karthigaiselvan/Documents/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/maps.csv")
    )

    # Join all data
    joined = match_details.join(matches, "match_id") \
                          .join(maps, "mapid") \
                          .join(medals_matches_players, ["match_id", "player_gamertag"], "left") \
                          .join(medals.withColumnRenamed("name", "medal_name"), "medal_id", "left")

    joined.cache()

    # 1. Player with highest avg grenade kills per game
    top_killers = joined.groupBy("player_gamertag") \
        .agg(avg("player_total_grenade_kills").alias("avg_grenade_kills")) \
        .orderBy(desc("avg_grenade_kills"))

    # 2. Most played playlist
    most_played_playlist = matches.groupBy("playlist_id") \
        .agg(count("*").alias("match_count")) \
        .orderBy(desc("match_count"))

    # 3. Most played map
    most_played_map = matches.groupBy("mapid") \
        .agg(count("*").alias("match_count")) \
        .orderBy(desc("match_count"))

    # 4. Map with most Killing Spree medals
    killing_spree = joined.filter(col("medal_name") == "Killing Spree")
    killing_spree_by_map = killing_spree.groupBy("mapid") \
        .agg(count("*").alias("killing_spree_count")) \
        .orderBy(desc("killing_spree_count"))

    # 5. Try sortWithinPartitions
    sorted_by_playlist = joined.sortWithinPartitions("playlist_id")
    sorted_by_map = joined.sortWithinPartitions("mapid")

    # Save outputs as parquet
    top_killers.write.mode("overwrite").parquet("output/top_killers")
    most_played_playlist.write.mode("overwrite").parquet("output/most_played_playlist")
    most_played_map.write.mode("overwrite").parquet("output/most_played_map")
    killing_spree_by_map.write.mode("overwrite").parquet("output/killing_spree_by_map")

if __name__ == "__main__":
    aggregate_analysis()
