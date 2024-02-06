import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px

# Configuration de la connexion MongoDB (commune aux trois applications)
client = MongoClient("mongodb://localhost:27017/")
db = client.animeDB

# Fonctions communes (si elles existent)...

# Définition de la structure de l'application avec des onglets
st.title('Application')

tab1, tab2, tab3 = st.tabs(["User", "Analyst", "Admin"])

with tab3:
    st.header("Admin")
    # Create a function to get database stats
    def get_collection_stats(collection_name):
        return db.command("collStats", collection_name)

    def get_cluster_state():
        try:
            # Ensure the command is called on the 'admin' database
            admin_db = client['admin']

            # Get the list of shards
            shards_info = admin_db.command('listShards')
            shards = shards_info.get('shards', [])
            
            # Prepare cluster state information
            cluster_state = {}
            for shard in shards:
                shard_id = shard.get('_id')
                replicas = shard.get('host', '').split('/')
                num_replicas = len(replicas) - 1 if len(replicas) > 1 else 1
                cluster_state[shard_id] = num_replicas
            
            return cluster_state
        except Exception as e:
            st.error(f'Error fetching cluster state: {e}')
            return {}

    # Function to get indexes information
    def get_indexes_info():
        indexes_info = {}
        for collection_name in db.list_collection_names():
            indexes_info[collection_name] = db[collection_name].index_information()
        return indexes_info


    # Streamlit interface
    st.title('MongoDB Cluster Administration')
    col1, col2, col3, col4 = st.columns([1,1,1,1])

    with col1:
        if st.button('Get Database Stats'):
            results1 = "ex1"

    with col2:
        if st.button('Get Indexes Info'):
            indexes_info = get_indexes_info()
            stats_df = pd.DataFrame(indexes_info)
            transposed_df = stats_df.T
            result_to_display = transposed_df

    with col3:
        if st.button('Performances of the queries'):
            result_to_display = pd.read_csv("queries_performance.csv")
        

    if 'results1' in locals():
        for collection_name in db.list_collection_names():
            stats = db.command("collStats", collection_name)
            relevant_stats = {
                "Collection": collection_name,
                "Size": stats["size"],
                "Document Count": stats["count"],
                "Average Object Size": stats["avgObjSize"],
                "Storage Size": stats["storageSize"],
                "Number of Indexes": stats["nindexes"],
                "Total Index Size": stats["totalIndexSize"],
            }
            stats_df = pd.DataFrame(list(relevant_stats.items()), columns=["Statistic", "Value"])
            st.dataframe(stats_df, use_container_width=True)

    if 'result_to_display' in locals():
        st.dataframe(result_to_display,use_container_width=True)

with tab2:
    st.header("Analyst")
    # Formatting function to remove the thousands separator
    def format_without_commas(x):
        if isinstance(x, float):
            return "{:.2f}".format(x)
        elif isinstance(x, int):
            return str(x)
        else:
            return x

    def plot_data_with_plotly(data, query_type):
        df = pd.DataFrame(data)

        if query_type == 'query_5':
            x_column = 'Type'
            y_column = 'Average Popularity'
            title = 'Average Popularity Of Anime Within Each Type'
        elif query_type == 'query_6':
            x_column = 'Studio'
            y_column = 'Average Rank'
            title = 'Top 5 Studios by Average Anime Rankings'
        elif query_type == 'query_7':
            df = df.groupby('Year')['Average Popularity'].mean().reset_index()
            x_column = 'Year'
            y_column = 'Average Popularity'
            title = 'Average Popularity of Anime by Year'
        fig = px.bar(df, x=x_column, y=y_column, 
                    title=title,
                    labels={y_column: y_column, x_column: x_column})
        fig.update_layout(autosize=False, width=800, height=600)
        st.plotly_chart(fig)

    # Function to execute Query 1 with dynamic genre
    def execute_query_1(selected_genre):
        genre_id = db.genres_l.find_one({"genres_de": selected_genre})['genres_id']
        genre_id = str(genre_id)
        result = db.anime_ranking_table.aggregate([
            {"$addFields": {"genres_id_array": {"$split": ["$genres_id", ","]}}},
            {"$match": {"genres_id_array": genre_id, "popularity": {"$gt": 500}}},
            {"$project": {"_id": 0, "title": 1, "popularity": 1, "genre": selected_genre}}])
        return list(result)

    # Function to execute Query 2 with dynamic studio
    def execute_query_2(selected_studio):
        studio_id = db.studios_l.find_one({"studio_de": selected_studio})['studio_id']
        result = db.anime_table.aggregate([
            {"$match": {"studio_id": studio_id, "status": "finished_airing"}},
            {"$lookup": {"from": "studios_l","localField": "studio_id","foreignField": "studio_id","as": "studio_info"}},
            {"$unwind": "$studio_info"},
            {"$group": {"_id": "$title","studio": {"$first": "$studio_info.studio_de"}}},
            {"$project": {"_id": 0,"Title": "$_id","studio": 1,"status": 1}}])
        return list(result)

    def execute_query_3(selected_titles):
        if not selected_titles:
            return []
        result = db.anime_ranking_table.aggregate([
            {"$match": {"title": {"$in": selected_titles}}},
            {"$group": {"_id": "$title", "rank": {"$first": { "$toInt": "$rank" }}, "popularity": {"$first": "$popularity"}}},
            {"$sort": {"rank": 1}},
            {"$project": {"_id": 0, "Title": "$_id", "rank": 1, "popularity": 1}}])
        return list(result)

    def execute_query_4(selected_titles):
        if not selected_titles:
            return[]
        result = db.anime_ranking_table.aggregate([
            {"$match": {"title": {"$in": selected_titles}}},
            {"$lookup": {"from": "demo_l","localField": "demo_id","foreignField": "demo_id","as": "demo_data"}},
            {"$unwind": {"path": "$demo_data","preserveNullAndEmptyArrays": True}},
            {"$group": {"_id": "$title","demo_de": {"$first": "$demo_data.demo_de"}}},
            {"$project": {"_id": 0,"Title": "$_id","Type": {"$ifNull": ["$demo_de", "No demographic data"]}}}])
        return list(result)

    # Analyst Queries
    def execute_query_5():
        result = db.anime_ranking_table.aggregate([
            {"$lookup": {"from": "demo_l","localField": "demo_id","foreignField": "demo_id","as": "demo_data"}},
            {"$unwind": "$demo_data"},
            {"$group": {"_id": "$demo_data.demo_de","averagePopularity": {"$avg": "$popularity"}}},
            {"$sort": {"averagePopularity": -1}},
            {"$project": {"_id": 0,"Type": "$_id","Average Popularity": {"$round": ["$averagePopularity", 2]}}}])
        return list(result)

    def execute_query_6():
        result = db.anime_ranking_table.aggregate([
            {"$group": {"_id": "$studio_id","averageRank": {"$avg": "$rank"}}},
            {"$sort": {"averageRank": 1}},
            {"$limit": 5},
            {"$lookup": {"from": "studios_l","localField": "_id","foreignField": "studio_id","as": "studio_info"}},
            {"$unwind": "$studio_info"},
            {"$group": {"_id": "$studio_info.studio_de","averageRank": {"$first": "$averageRank"}}},
            {"$sort": {"averageRank": 1}},
            {"$project": {"_id": 0,"Studio": "$_id","Average Rank": {"$round": ["$averageRank", 2]}}}])
        return list(result)

    def execute_query_7():
        result = db.anime_ranking_table.aggregate([
            {"$lookup": {"from": "anime_table","localField": "mal_id","foreignField": "mal_id","as": "anime_details"}},
            {"$unwind": "$anime_details"},
            {"$group": {"_id": {"year": { "$toInt": "$anime_details.start_season.year"},"season": "$anime_details.start_season.season"},"average_popularity": {"$avg": "$popularity"}}},
            {"$sort": {"_id.year": 1, "_id.season": 1}},
            {"$project": {"_id": 0,"Year": "$_id.year","Season": "$_id.season","Average Popularity": {"$round": ["$average_popularity", 2]}}}])
        return list(result)

    def execute_query_8():
        result = db.anime_table.aggregate([
            {"$lookup": {"from": "genres_l","localField": "genres_id","foreignField": "genres_id","as": "genres_data"}},
            {"$unwind": "$genres_data"},
            {"$lookup": {"from": "anime_ranking_table","localField": "mal_id","foreignField": "mal_id","as": "ranking_data"}},
            {"$unwind": "$ranking_data"},
            {"$group": {"_id": "$genres_data.genres_de",
                "episodesCompletedPerUser": {
                    "$avg": {"$multiply": [ {"$divide": ["$ranking_data.statistics_completed", "$ranking_data.statistics_num_scoring_users"]},100]}},
                "episodesOnHoldPerUser" : {
                    "$avg": {"$multiply": [ {"$divide": ["$ranking_data.statistics_on_hold", "$ranking_data.statistics_num_scoring_users"]},100]}},
                "episodesDroppedPerUser": {
                    "$avg": {"$multiply": [{"$divide": ["$ranking_data.statistics_dropped", "$ranking_data.statistics_num_scoring_users"]},100]}}}},
            {"$project": {"_id": 0,"Genre": "$_id",
                "Episodes Completed Per User": {"$concat": [{"$toString": {"$round": ["$episodesCompletedPerUser", 2]}}, "%"]},
                "Episodes On Hold Per User" : {"$concat": [{"$toString": {"$round": ["$episodesOnHoldPerUser", 2]}}, "%"]},
                "Episodes Dropped Per User": {"$concat": [{"$toString": {"$round": ["$episodesDroppedPerUser", 2]}}, "%"]}}}])
        return list(result)

    # Streamlit interface
    st.title('Anime Database Analyst Interface')

    # Define the layout
    col1, space1, col2, space2, col3 = st.columns([17, 1, 5, 1, 4])

    # Column 1: ComboBoxes for selecting queries
    with col1:
        query_choice_1 = st.selectbox(
            'Choose a query from User view', 
            ("Retrieve Popular Anime of a Specific Genre", 
            "Retrieve Finished Airing Anime Titles for a Specific Studio", 
            "Retrieve and Display Rank and Popularity of Specific Anime Titles", 
            "Check Availability of Demographic Information for a Specific Anime Title"),
            key='comboBox1'
        )

        query_choice_2 = st.selectbox(
            'Choose a query from Analyst view', 
            ("Average Popularity Of Anime Within Each Type", 
            "Top 5 Studios by Average Anime Rankings", 
            "Analyze Anime Title Popularity Over Time by Season",
            "Calculate Average Episodes Stats by Anime Genre per User"),
            key='comboBox2'
        )

    # Column 2: Dynamic user inputs based on selected query
    with col2:
        if query_choice_1 == "Retrieve Popular Anime of a Specific Genre":
            genres = [genre["genres_de"] for genre in db.genres_l.find({}, {"genres_de": 1})]
            selected_genre = st.selectbox('Select a genre', genres)
        elif query_choice_1 == "Retrieve Finished Airing Anime Titles for a Specific Studio":
            studios = [studio["studio_de"] for studio in db.studios_l.find({}, {"studio_de": 1})]
            selected_studio = st.selectbox('Select a studio', studios)
        elif query_choice_1 == "Retrieve and Display Rank and Popularity of Specific Anime Titles":
            anime_titles = db.anime_ranking_table.distinct("title")
            selected_titles = st.multiselect('Select anime titles', anime_titles)
        elif query_choice_1 == "Check Availability of Demographic Information for a Specific Anime Title":
            anime_titles = db.anime_ranking_table.distinct("title")
            selected_titles = st.multiselect('Select anime titles', anime_titles)

    # Execute queries based on selections and display results
    with col3:
        st.write("\n")
        st.write("\n")
        if st.button('User'):
            if query_choice_1 == "Retrieve Popular Anime of a Specific Genre":
                results = execute_query_1(selected_genre)
                line = (f"Results for genre '{selected_genre}': {len(results)}")
            elif query_choice_1 == "Retrieve Finished Airing Anime Titles for a Specific Studio":
                results = execute_query_2(selected_studio)
                line = (f"Results for studio '{selected_studio}': {len(results)}")
            elif query_choice_1 == "Retrieve and Display Rank and Popularity of Specific Anime Titles":
                results = execute_query_3(selected_titles)
                line = (f"Results for selected titles sorted by Rank")
            elif query_choice_1 == "Check Availability of Demographic Information for a Specific Anime Title":
                results = execute_query_4(selected_titles)
                line = (f"Results for selected titles: {len(results)}")
        st.write("\n")
        st.write("\n")
        if st.button('Analyst'):
            if query_choice_2 == "Average Popularity Of Anime Within Each Type":
                graph5 = ""
                results = execute_query_5()
                line = (f"Number of results : {len(results)}")
            elif query_choice_2 == "Top 5 Studios by Average Anime Rankings":
                graph6 = ""
                results = execute_query_6()
                line = (f"Number of results : {len(results)}")
            elif query_choice_2 == "Analyze Anime Title Popularity Over Time by Season":
                graph7 = ""
                results = execute_query_7()
                line = (f"Number of results : {len(results)}, sorted by Year")
            elif query_choice_2 == "Calculate Average Episodes Stats by Anime Genre per User":
                results = execute_query_8()
                line = (f"Number of results : {len(results)}")

    # Display the result into a table
    if 'results' in locals():
        st.write(line)
        df = pd.DataFrame(results)
        formatted_df = df.applymap(format_without_commas)
        st.dataframe(formatted_df, use_container_width=True)

    if 'graph5' in locals():
        data = execute_query_5()
        plot_data_with_plotly(data, 'query_5')

    if 'graph6' in locals():
        data = execute_query_6()
        plot_data_with_plotly(data, 'query_6')

    if 'graph7' in locals():
        data = execute_query_7()
        plot_data_with_plotly(data, 'query_7')

with tab1:
    st.header("User")
        # Fonction pour exécuter les requêtes
    def execute_query(query_number):
        if query_number == "Fetch popular 'Adventure' anime with a popularity over 500":
            adventure_genre_id = db.genres_l.find_one({"genres_de": "Adventure"})['genres_id']
            adventure_genre_id = str(adventure_genre_id)
            result = db.anime_ranking_table.aggregate([
            {"$addFields": {"genres_id_array": {"$split": ["$genres_id", ","]}}},
            {"$match": {"genres_id_array": adventure_genre_id, "popularity": {"$gt": 500}}},
            {"$group": {"_id": "$title", "popularity": {"$max": "$popularity"}}},
            {"$sort": {"popularity": -1}},
            {"$project": {"_id": 0, "title": "$_id", "popularity": 1, "genre": "Adventure"}}])
            return list(result)
        elif query_number == "Retrieve all 'Madhouse' studio anime titles that have finished airing":
            madhouse_studio_id = db.studios_l.find_one({"studio_de": "Madhouse"})['studio_id']
            result = db.anime_table.aggregate([
            {"$match": {"studio_id": madhouse_studio_id, "status": "finished_airing"}},
            {"$lookup": {
                "from": "studios_l",
                "localField": "studio_id",
                "foreignField": "studio_id",
                "as": "studio_info"}},
            {"$unwind": "$studio_info"},
            {"$group": {
                "_id": "$title",
                "studio": {"$first": "$studio_info.studio_de"}}},
            {"$project": {
                "_id": 0,
                "title": "$_id",
                "studio": 1,
                "status": 1}}
            ])
            return list(result)
        elif query_number == "Retrieve and display the rank and popularity of specific anime titles":
            result = db.anime_ranking_table.aggregate([
            {"$match": {
                "$or": [
                    {"title": "Yakitate!! Japan"},
                    {"title": "Haikyuu!!"},
                    {"title": "Gakuen Alice"},
                    {"title": "Magi: The Kingdom of Magic"}
                ]}},
            {"$group": {
                "_id": "$title",
                "rank": {"$first": "$rank"},
                "popularity": {"$first": "$popularity"}}},
            {"$sort": {"rank": 1}},
            {"$project": {
                "_id": 0,
                "title": "$_id",
                "rank": 1,
                "popularity": 1}}
            ])
            return list(result)
        elif query_number == "Check for demographic information availability for a specific anime":
            result = db.anime_ranking_table.aggregate([
            {"$match": {
                "$or": [
                    {"title": "Trigun"}, #27
                    {"title": "Gleipnir"}, #42
                    {"title": "Shirokuma Cafe"}, #43
                    {"title": "Gakuen Alice"}, #25
                    {"title": "Heybot!"} #15
                ]}},
            {"$lookup": {
                "from": "demo_l",
                "localField": "demo_id",
                "foreignField": "demo_id",
                "as": "demo_data"}},
            {"$unwind": {
                "path": "$demo_data",
                "preserveNullAndEmptyArrays": True}},
            {"$group": {
                "_id": "$title",
                "demo_de": {"$first": "$demo_data.demo_de"}}},
            {"$project": {
                "_id": 0,
                "title": "$_id",
                "demo_de": {"$ifNull": ["$demo_de", "No demographic data"]}}}
            ])
            return list(result)

    # Streamlit interface
    st.title('Anime Database User Queries')

    # ComboBoxes for selecting queries
    query_choice = st.selectbox(
        'Choose a query', 
        ("Fetch popular 'Adventure' anime with a popularity over 500", 
        "Retrieve all 'Madhouse' studio anime titles that have finished airing", 
        "Retrieve and display the rank and popularity of specific anime titles", 
        "Check for demographic information availability for a specific anime")
    )
    # Execute queries
    if st.button('Execute the query'):
        results = execute_query(query_choice)

    # Display the result into a table
        if results:
            st.write(f"Number of results : {len(results)}")
            df = pd.DataFrame(results)
            st.dataframe(df, use_container_width=True)
        else:
            st.write("No result found")

    
