from pyspark import pipelines as dp

@dp.table(name="spotify_catalog.silver.dimuser")
def dimuser():
    return(
            spark.readStream.format('delta')\
                .load('abfss://silver@spotifyprojadls.dfs.core.windows.net/DimUser/data')
    )

@dp.table(name="spotify_catalog.silver.dimartist")
def dimuser():
    return(
            spark.readStream.format('delta')\
                .load('abfss://silver@spotifyprojadls.dfs.core.windows.net/DimArtist/data')
    )

@dp.table(name="spotify_catalog.silver.dimdate")
def dimuser():
    return(
            spark.readStream.format('delta')\
                .load('abfss://silver@spotifyprojadls.dfs.core.windows.net/DimDate/data')
    )

@dp.table(name="spotify_catalog.silver.dimtrack")
def dimuser():
    return(
            spark.readStream.format('delta')\
                .load('abfss://silver@spotifyprojadls.dfs.core.windows.net/DimTrack/data')
    )

@dp.table(name="spotify_catalog.silver.factstream")
def dimuser():
    return(
            spark.readStream.format('delta')\
                .load('abfss://silver@spotifyprojadls.dfs.core.windows.net/FactStream/data')
    )

dp.create_streaming_table(name='spotify_catalog.gold.dimuser')
dp.create_auto_cdc_flow(
  target = "spotify_catalog.gold.dimuser",
  source = "spotify_catalog.silver.dimuser",
  keys = ["user_id"],
  sequence_by = "updated_at",
  except_column_list = ["_rescued_data"],
  stored_as_scd_type = "2"
)

dp.create_streaming_table(name='spotify_catalog.gold.dimartist')
dp.create_auto_cdc_flow(
  target = "spotify_catalog.gold.dimartist",
  source = "spotify_catalog.silver.dimartist",
  keys = ["artist_id"],
  sequence_by = "updated_at",
  except_column_list = ["_rescued_data"],
  stored_as_scd_type = "2"
)

dp.create_streaming_table(name='spotify_catalog.gold.dimdate')
dp.create_auto_cdc_flow(
  target = "spotify_catalog.gold.dimdate",
  source = "spotify_catalog.silver.dimdate",
  keys = ["date_key"],
  sequence_by = "date",
  except_column_list = ["_rescued_data"],
  stored_as_scd_type = "2"
)

dp.create_streaming_table(name='spotify_catalog.gold.dimtrack')
dp.create_auto_cdc_flow(
  target = "spotify_catalog.gold.dimtrack",
  source = "spotify_catalog.silver.dimtrack",
  keys = ["track_id"],
  sequence_by = "updated_at",
  except_column_list = ["_rescued_data"],
  stored_as_scd_type = "2"
)

dp.create_streaming_table(name='spotify_catalog.gold.factstream')
dp.create_auto_cdc_flow(
  target = "spotify_catalog.gold.factstream",
  source = "spotify_catalog.silver.factstream",
  keys = ["stream_id", "user_id", "track_id", "date_key"],
  sequence_by = "stream_timestamp",
  except_column_list = ["_rescued_data"],
  stored_as_scd_type = "2"
)
