# CAPSTONE PROJECT-1:YOUTUBE DATA HARVESTING AND WAREHOUSING USING SQL AND STREAMLIT

# YOUTUBE API LIBRARIES:
from googleapiclient.discovery import build

# SQL LIBRARIES:
import mysql.connector
from datetime import datetime,timedelta

# STREAMLIT LIBRARIES:
from streamlit_option_menu import option_menu
import streamlit as st
import pandas as pd
import json

# SQL CONNECTION:
# Connect to MySQL server
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Sarvesh@24",
    database="youtubedatabase"
    )
cursor = mydb.cursor()

#API KEY CONNECTION:
def Api_connect():
    api_key = "AIzaSyBTaG4aewYbTVjwnhfjuJ4AvmavrX1IbIw"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

utube_call = Api_connect()

# FETCHING THE CHANNEL ID:

def Channel_Info(channel_id):

    #Channel table creation:
    cursor.execute("""CREATE TABLE IF NOT EXISTS channel_info (
                        channel_name VARCHAR(255),
                        channel_id VARCHAR(255) PRIMARY KEY,
                        subscribe INT,
                        views INT,
                        total_videos INT,
                        channel_description TEXT,
                        playlist_id VARCHAR(255)
                    )""")
    
    request = utube_call.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    
    for item in response.get('items',[]):
        details = dict(Channel_Name= item['snippet']['title'],
            Channel_Id= item['id'],
            Subscribers= item['statistics']['subscriberCount'],
            Views= item['statistics']['viewCount'],
            Total_Videos= item['statistics']['videoCount'],
            Channel_Description= item['snippet']['description'],
            Playlist_Id=item['contentDetails']['relatedPlaylists']['uploads']
        )
        #Channel Table Insertion:   
        cursor.execute("INSERT INTO channel_info (channel_name, channel_id, subscribe, views, total_videos, channel_description, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (details['Channel_Name'], details['Channel_Id'], details['Subscribers'], details['Views'], details['Total_Videos'], details['Channel_Description'], details['Playlist_Id']))
        
        mydb.commit()
    return details

# FETCHING THE VIDEO ID:
def Get_Video_Id(video_id):
    Video_ID=[]
    response=utube_call.channels().list(id=video_id,
                                        part='contentDetails').execute()
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    Next_Page_Token=None

    while True:
        request_1=utube_call.playlistItems().list(
                                                part='snippet',
                                                playlistId=Playlist_ID,
                                                maxResults=50,
                                                pageToken=Next_Page_Token).execute()
        for i in range(len(request_1['items'])):
            Video_ID.append(request_1['items'][i]['snippet']['resourceId']['videoId'])
        Next_Page_Token=request_1.get('nextPageToken')

        if Next_Page_Token is None:
            break
      
    return Video_ID

# FETCHING THE VIDEO INFORMATION:

#Duration:
def parse_duration(duration_str):
    try:
        duration_seconds = int(duration_str[2:-1])
        return duration_seconds
    except ValueError:
        return None

def Get_Video_Details(Video_id):
    Video_List = []
    for v_id in Video_id:
        request = utube_call.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id
        )
        response = request.execute()

        #Video Table Creation:
        cursor.execute("""CREATE TABLE IF NOT EXISTS video_details(
                    channel_name VARCHAR(255),
                    channel_id VARCHAR(255) ,
                    video_id VARCHAR(255) PRIMARY KEY ,
                    title TEXT,
                    tags TEXT,
                    thumbnail TEXT,
                    description TEXT,
                    published_date DATETIME,
                    duration TIME,
                    views BIGINT,
                    likes INT,
                    dislikes INT,
                    comments INT
                )"""
                     )

        for item in response['items']:
            Data = dict(
                channel_Name=item['snippet']['channelTitle'],
                Channel_Id= item['snippet']['channelId'],
                Video_Id= item['id'],
                Title= item['snippet']['title'],
                Tags= json.dumps(item.get('tags')),
                Thumbnail=json.dumps(item['snippet']['thumbnails']),
                Description=item['snippet'].get('description', ''),
                Publish_Date= item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount', 0),
                Likes= item['statistics'].get('likeCount', 0),
                Dislikes=item['statistics'].get('dislikeCount'),
                Comments= item['statistics'].get('commentCount', 0)
                )
            Video_List.append(Data)

            #Duration To Seconds:
            duration_seconds = parse_duration(Data['Duration'])
            if duration_seconds is not None:
               duration = timedelta(seconds=duration_seconds)
            else:
               duration = timedelta(seconds=0) 
            current_datetime = datetime.now()
            updated_datetime = current_datetime + duration
            sql_duration = updated_datetime.strftime('%Y-%m-%d %H:%M:%S')

            #Published Date:
            iso_datetime = Data['Publish_Date']
            parsed_datetime = datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
            mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

        #Video Data Insertion:
        cursor.execute("INSERT INTO video_details(channel_name, channel_id, video_id, title ,tags ,thumbnail , description, published_date, duration, views, likes, dislikes, comments) VALUES (%s, %s, %s, %s, %s, %s,  %s, %s, %s, %s,%s,%s,%s)",
               (Data['channel_Name'], Data['Channel_Id'], Data['Video_Id'],Data['Title'],Data['Tags'], Data['Thumbnail'], Data['Description'],mysql_published_date, sql_duration, Data['Views'], Data['Likes'], Data['Dislikes'], Data['Comments']))

        mydb.commit()        
    return Video_List

# FETCHING THE COMMENT INFORMATION:
def get_comment_Details(get_Comment):
    comment_List=[]
    #Comment Table Creation:
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS comment_details (
                            comment_id VARCHAR(255) PRIMARY KEY ,
                            video_id VARCHAR(255)  ,
                            comment_text TEXT,
                            author VARCHAR(255),
                            published_date DATETIME
                        )""")
        for Com_Det in get_Comment:
            request=utube_call.commentThreads().list(
                part="snippet",
                videoId=Com_Det,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                    Comment_Det=dict(Comment_ID=item['snippet']['topLevelComment']['id'],
                                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Author_Name=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                    comment_List.append(Comment_Det)

                    # Published Date:
                    iso_datetime = Comment_Det['Published_Date']
                    parsed_datetime = datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
                    mysql_published_dates = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

                    #Comment Data Insertion:
                    cursor.execute("INSERT INTO comment_details (comment_id, video_id, comment_text, author, published_date) VALUES (%s, %s, %s, %s,%s)",
                               (Comment_Det['Comment_ID'], Comment_Det['Video_Id'], Comment_Det['Comment_Text'], Comment_Det['Author_Name'],mysql_published_dates))
                    mydb.commit()
                    
    except Exception as e:
        print(f"Error: {e}")
         
    return comment_List

# FETCHING THE PLAYLIST DETAILS:

def get_playlist_details(channel_id):
    Playlist_Data=[]
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS playlist_details (
                            playlist_id VARCHAR(255),
                            title VARCHAR(255),
                            channel_id VARCHAR(255),
                            published_date DATETIME,
                            video_count INT
                        )""")
        Next_Page_Token=None
       
        while True:
            request=utube_call.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=Next_Page_Token
        )
            response=request.execute()

            for item in response['items']:
                PlayList_Det=dict(Playlist_Id=item['id'],
                            Title= item['snippet']['title'],
                            Channel_Id=item['snippet']['channelId'],
                            Published_Date=item['snippet']['publishedAt'],
                            Video_Count=item['contentDetails']['itemCount']
                            )
            Playlist_Data.append(PlayList_Det)

            Next_Page_Token=response.get('nextPageToken')
            if Next_Page_Token is None:
               break

        #Published Date:
        iso_datetime = PlayList_Det['Published_Date']
        parsed_datetime = datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
        mysql_published_datee = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

        #Playlist Data Insertion:
        cursor.execute("INSERT INTO playlist_details (playlist_id, title, channel_id, published_date, video_count) VALUES (%s, %s, %s, %s, %s)",
        (PlayList_Det['Playlist_Id'], PlayList_Det['Title'], PlayList_Det['Channel_Id'], mysql_published_datee, PlayList_Det['Video_Count']))
        
        mydb.commit()

    except Exception as e:
        print(f"Error: {e}")

    return Playlist_Data

#DEFINING OVERALL FUNCTION TO FETCH ALL DETAILS:

def fetch_all_data(channel_id):
    channel_info = Channel_Info(channel_id)
    video_id=Get_Video_Id(channel_id)
    playlist_details = get_playlist_details(channel_id)
    video_details = Get_Video_Details(video_id)
    comment_details = get_comment_Details(video_id)

# CONVERT DICTIONARY TO DATAFRAME:
    channel_df = pd.DataFrame([channel_info])
    video_df = pd.DataFrame(video_id)
    playlist_df = pd.DataFrame(playlist_details)
    video_detail_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)
    
    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "comment_details": comment_df,
        "playlist_details": playlist_df,
        "video_data": video_detail_df
    }

#SETUP STREAMLIT-UI:
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", layout="wide")

# MAIN FUNCTION
def main():
    
    st.sidebar.header('NAVIGATION')
    option=st.sidebar.radio("Select Option",['HOME','DATA COLLECTION','DATA ANALYSIS'])

    #Home:
    if option == "HOME":
        st.title(':rainbow[YOUTUBE DATA HARVESTING and WAREHOUSING USING MYSQL AND STREAMLIT]')
        st.markdown("## :red[DOMAIN] : Social Media")
        st.markdown("## :red[SKILLS] : Python scripting, API connection, Data collection, Table Creation, Data Insertion, Streamlit")
        st.markdown("## :red[OVERALL VIEW] : Creating an UI with Streamlit, retrieving data from YouTube API, storing the data in SQL, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")
        st.markdown("## :red[DEVELOPED BY] : Indumathi.S")
        st.header("Welcome to the YouTube Data Harvesting and Warehousing App!")
        st.markdown("Use the sidebar to navigate through different sections of the app.")

    #Data Collection:
    elif option =="DATA COLLECTION":
            st.subheader(':rainbow[YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING USING MYSQL AND STREAMLIT]', divider='blue')
            channel_id = st.text_input("Enter the YouTube Channel ID to collect and store data")
            
            if st.button("Collect and store Data"):
                st.success(f"Data for channel ID collected and stored successfully.")
                details = fetch_all_data(channel_id)
                
                st.subheader('Channel Details')
                st.write(details["channel_details"])

                st.subheader('Video Details')
                st.write(details["video_data"])

                st.subheader('Comment Details')
                st.write(details["comment_details"])

                st.subheader('Playlist Details')
                st.write(details["playlist_details"])

    #Data Analysis:       
    elif option == "DATA ANALYSIS":
        st.header("DATA ANALYSIS")

        questions = [
                   "1. What are the names of all the videos and their corresponding channels?",
                   "2. Which channels have the most number of videos, and how many videos do they have?",
                   "3. What are the top 10 most viewed videos and their respective channels?",
                   "4. How many comments were made on each video, and what are their corresponding video names?",
                   "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                   "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                   "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                   "8. What are the names of all the channels that have published videos in the year 2022?",
                   "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                   "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                   ]

        selected_questions = st.multiselect("Select Questions To Execute The Query", questions)
        if st.button("Run Query"):

            for selected_question in selected_questions:
        
                if selected_question == questions[0]:
                    cursor.execute("SELECT channel_name,title FROM video_details")
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel Name', 'Title'])
                    st.write(df)

                          
                elif selected_question == questions[1]:
                    cursor.execute("SELECT channel_name, COUNT(*) as video_count FROM video_details GROUP BY channel_name ORDER BY video_count DESC")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel Name', 'Counts'])
                    st.write(df)

                    
                elif selected_question == questions[2]:
                    cursor.execute("SELECT channel_name,title,views FROM video_details ORDER BY views DESC LIMIT 10")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel Name', 'Title', 'Views'])
                    st.write(df)

                elif selected_question == questions[3]:
                    cursor.execute("SELECT title,comments FROM video_details")
                    data=cursor.fetchall()
                    df=df=pd.DataFrame(data, columns=['Title','Comments'])
                    st.write(df)

                elif selected_question == questions[4]:
                    cursor.execute("SELECT channel_name,MAX(likes) as max_likes FROM video_details GROUP BY channel_name")
                    data=cursor.fetchall()
                    df=pd.DataFrame(data, columns=['Channel_Name','Likes'])
                    st.write(df)

                elif selected_question == questions[5]:
                    cursor.execute("SELECT title, SUM(likes) as total_likes, SUM(dislikes) as total_dislikes FROM video_details GROUP BY title")
                    data=cursor.fetchall()
                    df=pd.DataFrame(data, columns=['Title','Likes','Dislikes'])
                    st.write(df)

                elif selected_question == questions[6]:
                    cursor.execute("SELECT channel_name, SUM(views) as total_views FROM video_details GROUP BY channel_name")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel_Name', 'Views'])
                    st.write(df)

                elif selected_question == questions[7]:
                    cursor.execute("SELECT DISTINCT channel_name FROM video_details WHERE YEAR(published_date) = 2022;")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel_Name'])
                    st.write(df)

                elif selected_question == questions[8]:
                    cursor.execute("SELECT channel_name, AVG(duration) AS avg_duration FROM video_details GROUP BY channel_name ")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel_Name', 'Avg_Duration'])
                    st.write(df)

                elif selected_question == questions[9]:
                    cursor.execute("""SELECT title, channel_name, SUM(comments) as comments
                            FROM video_details 
                            GROUP BY title, channel_name 
                            ORDER BY comments DESC 
                            LIMIT 1
                        """)
                    data = cursor.fetchall()
                    df=pd.DataFrame(data,columns=['Title','Channel_Name','Comments'])
                    st.write(df)
               
if __name__ == "__main__":
    main()

    mydb.close()


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

#Demo video:https://screenrec.com/share/uMx0tliTek
# https://github.com/indusur/INDUMATHI.S
