# YOUTUBE API LIBRARIES:
import googleapiclient.discovery
from googleapiclient.discovery import build

# SQL LIBRARIES:
import mysql.connector
import isodate
from datetime import datetime

# STREAMLIT LIBRARIES:
from streamlit_option_menu import option_menu
import streamlit as st
from googleapiclient.errors import HttpError
import pandas as pd

# API key connection:
def Api_connect():
    Api_Id = "AIzaSyBTaG4aewYbTVjwnhfjuJ4AvmavrX1IbIw"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube

youtube = Api_connect()

# FETCHING THE CHANNEL ID:
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    data = {
        "channel_name": response['items'][0]['snippet']['title'],
        "channel_id": channel_id,
        "channel_dec": response['items'][0]['snippet']['description'],
        "channel_pid": response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "channel_vc": response['items'][0]['statistics']['videoCount'],
        "channel_subc": response['items'][0]['statistics']['subscriberCount']
    }
    return data
#channel_details=get_channel_info(channel_id)
#channel_details

# FETCHING THE VIDEO ID:
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        response1 = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids
#Video_Ids=get_videos_ids(channel_id)
#Video_Ids

# FETCHING THE VIDEO INFORMATION:
def duration_to_seconds(video_duration):
    duration = isodate.parse_duration(video_duration)
    hours = duration.days * 24 + duration.seconds // 3600
    minutes = (duration.seconds % 3600) // 60
    seconds = duration.seconds % 60
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def published_date(video_publisheddate):
    iso_date = datetime.fromisoformat(video_publisheddate.replace("Z", "+00:00"))
    sql_date_str = iso_date.strftime('%Y-%m-%d %H:%M:%S')
    return sql_date_str

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item.get('tags'),
                        Description=item.get('description'),
                        video_Publisheddate=published_date(item['snippet'].get('publishedAt')),
                        video_duration=duration_to_seconds(item['contentDetails'].get('duration')),
                        Views=item['statistics'].get('viewCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_status=item['contentDetails']['caption'],  
                    ) 
            video_data.append(data)
    return video_data
#video_details=get_video_info(Video_Ids)
#video_details

# FETCHING THE COMMENT INFORMATION:
def comment_date(comment_published):
    iso_date = datetime.fromisoformat(comment_published.replace("Z", "+00:00"))
    sql_date_str = iso_date.strftime('%Y-%m-%d %H:%M:%S')
    return sql_date_str

def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=comment_date(item['snippet']['topLevelComment']['snippet'].get('publishedAt'))
                )
                Comment_data.append(data)
    except HttpError as e:
        st.error(f"An HTTP error occurred: {e}")
    return Comment_data
#comment_details=get_comment_info(Video_Ids)
#comment_details

# FETCHING THE PLAYLIST DETAILS:
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails', 
            channelId=channel_id,                      
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
    
        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

# SQL CONNECTION:
# Connect to MySQL server
mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="Sarvesh@24"
)
mycursor = mydb.cursor()

# CREATE DATABASE:
mycursor.execute("CREATE DATABASE IF NOT EXISTS sqldatabase")

# CREATE CHANNEL TABLE AND INSERT DATA :
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Sarvesh@24",
    database="youtubedatabase"
    )
mycursor = mydb.cursor()
mycursor.execute('''CREATE TABLE IF NOT EXISTS channeldetails(
                        Channel_Id VARCHAR(100) PRIMARY KEY,
                        Channel_Name VARCHAR(100), 
                        Channel_Playlist_Id VARCHAR(255), 
                        Channel_Description TEXT, 
                        Channel_Videocount INT,
                        Channel_Subscription INT)
                    ''')
mydb.commit()

# query = "INSERT INTO channeldetails (Channel_Id, Channel_Name, Channel_Playlist_Id, Channel_Description, Channel_Videocount, Channel_Subscription) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE channel_Name=VALUES(channel_Name), Channel_Playlist_Id=VALUES(Channel_Playlist_Id), Channel_Description=VALUES(Channel_Description), Channel_Videocount=VALUES(Channel_Videocount), Channel_Subscription=VALUES(Channel_Subscription)"
# values = tuple(channel_details.values())
# mycursor.execute(query, values)
# mydb.commit()


mycursor.execute('''CREATE TABLE IF NOT EXISTS videodetails(
                        Video_Id VARCHAR(100) PRIMARY KEY,
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Title VARCHAR(100),
                        Tags VARCHAR(255),
                        Description TEXT, 
                        video_Publisheddate VARCHAR(100), 
                        Video_duration INT,
                        Views INT,
                        Comments INT, 
                        Favorite_count INT, 
                        Definition VARCHAR(100),
                        Caption_status VARCHAR(100))
                    ''')
mydb.commit()

# query = "INSERT INTO videodetails (Video_Id, Channel_Name, Channel_Id, Title, Tags, Description, video_Publisheddate, Video_duration, Views, Comments, Favorite_count, Definition, Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Channel_Name=VALUES(Channel_Name), Channel_Id=VALUES(Channel_Id), Title=VALUES(Title), Tags=VALUES(Tags), Description=VALUES(Description), video_Publisheddate=VALUES(video_Publisheddate), Video_duration=VALUES(Video_duration), Views=VALUES(Views), Comments=VALUES(Comments), Favorite_count=VALUES(Favorite_count), Definition=VALUES(Definition), Caption_status=VALUES(Caption_status)"
# temp = [tuple(item.values()) for item in video_details]
# mycursor.executemany(query, temp)
# mydb.commit()

mycursor.execute('''
    CREATE TABLE IF NOT EXISTS commentdetail(
        Comment_ID VARCHAR(255) PRIMARY KEY,
        Video_ID VARCHAR(255),
        Comment_Text TEXT,
        Comment_Author VARCHAR(255),
        Comment_Published VARCHAR(255)
        )
    ''')
mydb.commit()

# query = "INSERT INTO commentdetail (Comment_ID, Video_ID, Comment_Text, Comment_Author, Comment_Published) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Video_ID=VALUES(Video_ID), Comment_Text=VALUES(Comment_Text), Comment_Author=VALUES(Comment_Author), Comment_Published=VALUES(Comment_Published)"
# com = [tuple(item.values()) for item in comment_details]
# mycursor.executemany(query, com)
# mydb.commit()


    # SETUP STREAMLIT UI:
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", layout="wide")

# SQL CONNECTION:
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Sarvesh@24",
    database="youtubedatabase"
)
mycursor = mydb.cursor()

def run_query(query):
    mycursor.execute(query)
    result = mycursor.fetchall()
    columns = [i[0] for i in mycursor.description]
    return pd.DataFrame(result, columns=columns)

# SETUP SIDEBAR:
with st.sidebar:
    st.header("NAVIGATION")
    section = st.selectbox("SELECT SECTION", ["HOME", "DATA COLLECTION", "DATA ANALYSIS"])

# HOME SECTION:# TITLE & DESCRIPTION:


if section == "HOME":
    st.title(':rainbow[YOUTUBE DATA HARVESTING and WAREHOUSING USING MYSQL AND STREAMLIT]')
    st.markdown("## :red[DOMAIN] : Social Media")
    st.markdown("## :red[SKILLS] : Python scripting, API connection, Data collection, Table Creation, Data Insertion, Streamlit")
    st.markdown("## :red[OVERALL VIEW] : Creating an UI with Streamlit, retrieving data from YouTube API, storing the data in SQL as a WH, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")
    st.markdown("## :red[DEVELOPED BY] : Indumathi.S")
    st.header("Welcome to the YouTube Data Harvesting and Warehousing App!")
    st.markdown("Use the sidebar to navigate through different sections of the app.")

# DATA COLLECTION SECTION:
def main(): 
    if section == "DATA COLLECTION":
        st.header("DATA COLLECTION")
        st.markdown("Enter the YouTube Channel ID to collect and store data.")
    
        channel_id = st.text_input("Enter YouTube Channel ID:")
        if st.button("Collect and Store Data"):
            st.success(f"Data for channel ID collected and stored successfully.")
            channel_data = get_channel_info(channel_id)
            video_ids=get_videos_ids(channel_id)
            video_data = get_video_info(channel_id)
            comment_data = get_comment_info(video_ids)
            query = "INSERT IGNORE INTO channeldetails (Channel_Id, Channel_Name, Channel_Playlist_Id, Channel_Description, Channel_Videocount, Channel_Subscription) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE channel_Name=VALUES(channel_Name), Channel_Playlist_Id=VALUES(Channel_Playlist_Id), Channel_Description=VALUES(Channel_Description), Channel_Videocount=VALUES(Channel_Videocount), Channel_Subscription=VALUES(Channel_Subscription)"
            values = tuple(channel_data.values())
            mycursor.execute(query, values)
            mydb.commit()
            query = "INSERT IGNORE INTO videodetails (Video_Id, Channel_Name, Channel_Id, Title, Tags, Description, video_Publisheddate, Video_duration, Views, Comments, Favorite_count, Definition, Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Channel_Name=VALUES(Channel_Name), Channel_Id=VALUES(Channel_Id), Title=VALUES(Title), Tags=VALUES(Tags), Description=VALUES(Description), video_Publisheddate=VALUES(video_Publisheddate), Video_duration=VALUES(Video_duration), Views=VALUES(Views), Comments=VALUES(Comments), Favorite_count=VALUES(Favorite_count), Definition=VALUES(Definition), Caption_status=VALUES(Caption_status)"
            temp = [tuple(item.values()) for item in video_data]
            mycursor.executemany(query, temp)
            mydb.commit()
            query = "INSERT IGNORE INTO commentdetail (Comment_ID, Video_ID, Comment_Text, Comment_Author, Comment_Published) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Video_ID=VALUES(Video_ID), Comment_Text=VALUES(Comment_Text), Comment_Author=VALUES(Comment_Author), Comment_Published=VALUES(Comment_Published)"
            com = [tuple(item.values()) for item in comment_data]
            mycursor.executemany(query, com)
            mydb.commit()

            if channel_data:
                st.write("Channel Data:")
                st.write(channel_data)
if __name__=="__main__":
    main()                

# DATA ANALYSIS SECTION:

if section == "DATA ANALYSIS":
    st.header("DATA ANALYSIS")

    questions = [
        "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?",
    ]
    query = ""   
    selected_question = st.selectbox("Select a Question to Query", questions)
    # Function to execute SQL queries and return the results as a DataFrame

    
    if st.button("Run Query"):
        if selected_question == questions[0]:
            query = "SELECT Title AS Video_Name, Channel_Name FROM videodetails"
        elif selected_question == questions[1]:
            query = "SELECT Channel_Name, COUNT(Video_Id) AS Number_of_Videos FROM videodetails GROUP BY Channel_Name ORDER BY Number_of_Videos DESC"
        elif selected_question == questions[2]:
            query = "SELECT Title AS Video_Name, Channel_Name, Views FROM videodetails ORDER BY Views DESC LIMIT 10"
        elif selected_question == questions[3]:
            query = "SELECT Title AS Video_Name, Comments FROM videodetails"
        elif selected_question == questions[4]:
            query = "SELECT Title AS Video_Name, Channel_Name, Likes FROM videodetails ORDER BY Likes DESC LIMIT 10"
        elif selected_question == questions[5]:
            query = "SELECT Channel_Name, SUM(Views) AS Total_Views FROM videodetails GROUP BY Channel_Name"
        elif selected_question == questions[6]:
            query = "SELECT Channel_Name FROM videodetails WHERE video_Publisheddate LIKE '2022%' GROUP BY Channel_Name"
        elif selected_question == questions[7]:
            query = "SELECT Channel_Name, AVG(Video_duration) AS Average_Duration FROM videodetails GROUP BY Channel_Name"
        elif selected_question == questions[8]:
            query = "SELECT Title AS Video_Name, Channel_Name, Comments FROM videodetails ORDER BY Comments DESC LIMIT 10"
        else:
            query = ""

        if query:
            result_df = run_query(query)
            st.write(result_df)
        else:
            st.error("Invalid query selected. Please try again.")



# UCZikuVCya6icZj5mW-nVwEA-Vaanga Samaikkalam @ Meenu's Aduppadi        
# UCR8Sgs3nievmg2EFBcFRr8g-Software Engineer Tutorials
# UC3LD42rjj-Owtxsa6PwGU5Q-streamlit 
# UCTobknDmJWuwrf7pI15QBdg-praba murugesan 
# UCQqmjKQBKQkRbWbSntYJX0Q-shabarinath Premlal
# UCEfkbcwk-Y6Vel5zMEQhU1Q-Under an Hour - Projects with Aryen
# UCvSZUp8XCT4Zlga2ctSOTMQ-Cyber Nanban
#UCPYC5ihCdPmB-GuBGtX_qAw-Learn Photography in Tamil
# UCq6-gHLaaLJSiyT8h-yh2Cg-PKCHELP
#UCY6KjrDBN_tIRFT_QNqQbRQ-Madan Gowri
