from googleapiclient.discovery import build
import streamlit as st
import plotly.express as px
import mysql.connector 
from datetime import datetime
import pandas as pd
import pymongo 
import time
from streamlit_option_menu import option_menu
from PIL import Image
# SETTING PAGE CONFIGURATIONS

st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing ",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': "This project is about Youtube data harvesting using mongoDB and mysql"})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})


# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)

#..............genereting api key..............# 
api_key='AIzaSyB8zDUJhRmT9IOWW1X0zBm4eO84iCEwv1Q'
youtube= build('youtube','v3',developerKey=api_key)

#..........Getting channel details from corresponding channel.........
@st.cache_data
def channel_statistics(_youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_ids)
    response = request.execute()
    for i in range(len(response["items"])):
        data = dict(channel_id = response["items"][i]["id"],
                    channel_name = response["items"][i]["snippet"]["title"],
                    channel_views = response["items"][i]["statistics"]["viewCount"],
                    subscriber_count = response["items"][i]["statistics"]["subscriberCount"],
                    total_videos = response["items"][i]["statistics"]["videoCount"],
                    playlist_id = response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"])
        all_data.append(data)
    return all_data
#............getting playList details...........
@st.cache_data
def get_playlist_data(df):
    playlist_ids = []
     
    for i in df["playlist_id"]:
        playlist_ids.append(i)
    return playlist_ids
#..........to get video_id.....................
@st.cache_data
def get_video_ids(_youtube,playlist_id_data):
    video_id = []
    for i in playlist_id_data:
        next_page_token = None
        more_pages = True
        while more_pages:
            request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = i,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
            
            for j in response["items"]:
                video_id.append(j["contentDetails"]["videoId"])
        
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                more_pages = False
    return video_id
        
#........................Function to get Video details.....................:
@st.cache_data
def get_video_details(_youtube,video_id):
    all_video_stats = []
    for i in range(0,len(video_id),50):
        
        request = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id = ",".join(video_id[i:i+50]))
        response = request.execute()
        
        for video in response["items"]:
            published_dates = video["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates,'%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d')
            videos = dict(
                          video_id = video["id"],
                          channel_id = video["snippet"]["channelId"],
                          video_name = video["snippet"]["title"],
                          published_date = format_date ,
                          view_count = video["statistics"].get("viewCount",0),
                          like_count = video["statistics"].get("likeCount",0),
                          comment_count= video["statistics"].get("commentCount",0),
                          duration = video["contentDetails"]["duration"])
            all_video_stats.append(videos)
    return (all_video_stats)
#.........................Function to get comment details
@st.cache_data
def get_comments(_youtube,video_ids):
    comments_data= []
    try:
        next_page_token = None
        for i in video_ids:
            while True:
                request = youtube.commentThreads().list(
                    part = "snippet,replies",
                    videoId = i,
                    textFormat="plainText",
                    maxResults = 100,
                    pageToken=next_page_token)
                response = request.execute()
                for item in response["items"]:
                    published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    parsed_dates = datetime.strptime(published_date,'%Y-%m-%dT%H:%M:%SZ')
                    format_date = parsed_dates.strftime('%Y-%m-%d')
                    
                    comments = dict(comment_id = item["id"],
                                    video_id = item["snippet"]["videoId"],
                                    comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    comment_published_date = format_date)
                    comments_data.append(comments) 
                
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break       
    except Exception as e:
        print("An error occured",str(e))          
            
    return comments_data
  
#................User Input.............................:
channel_ids = st.text_input("Enter the channel Id")
channel_list = [channel_ids]

submit = st.button("Fetch Channel details and Upload to MongoDB Database")
#.............................. MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
#.................Creating database in mongodb
db  = client["Youtube_Database"]
#..............Create Collections in mongodb.................:
col1 = db["channel_data"]
col2 = db["video_data"]
col3 = db["comment_data"]


#............mysql connection........................
connection = mysql.connector.connect(host="localhost",
                                     user="root",
                                     password="sujitha@1988",
                                     database="youtube_db")

if submit:
    
    if channel_ids:
        channel_details = channel_statistics(youtube,channel_ids)
        df = pd.DataFrame(channel_details) 
        playlist_id_data = get_playlist_data(df)
        video_id = get_video_ids(youtube,playlist_id_data)
        video_details = get_video_details(youtube,video_id)
        get_comment_data = get_comments(youtube,video_id)
        
        with st.spinner('Please wait '):
            time.sleep(5)
            st.success('Done!, Data Fetched Successfully')
            
            if channel_details:
            #Insert the data : 1
                col1.insert_many(channel_details) 
            if video_details:
            #Insert the data : 2
                col2.insert_many(video_details)
            if get_comment_data:
            #Insert the data : 3
                col3.insert_many(get_comment_data)
        with st.spinner('Please wait '):
            time.sleep(5)
            st.success('Done!, Data Uploaded Successfully')
            st.snow()

#..................... user input...............
def channel_names():   
    ch_name = []
    for i in db.channel_data.find():
        ch_name.append(i['channel_name'])
    return ch_name

st.subheader(":orange[Inserting Data into MySQL for further Data Analysis] ⌛")
user_input =st.multiselect("Select the channel to be inserted into MySQL Tables",options = channel_names())

submit1 = st.button("Upload data into MySQL")


#...........inserting channel details into mysql.......................
def insert_into_channels():
                for i in db.channel_data.find({"channel_name":{"$in":user_input}}):
                 query = """INSERT INTO channel_data(channel_id,channel_name,channel_views,subscriber_count,total_videos,playlist_id)VALUES(%s,%s,%s,%s,%s,%s)"""
                 values = (i['channel_id'],i['channel_name'],i['channel_views'],i['subscriber_count'],i['total_videos'],i['playlist_id'])
                 cursor.execute(query,values)
                 connection.commit()
  #...............inserting video details into mysql....................               
def insert_into_videos():
        for chn in db.channel_data.find({"channel_name":{"$in":user_input}}):
            for i in db.video_data.find({"channel_id":chn["channel_id"]}).limit(25):

                query1 = """INSERT INTO video_data(video_id,channel_id,video_name,published_date,view_count,like_count,comment_count,duration) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                values=(i["video_id"],
                     i["channel_id"],
                     i["video_name"],
                     i["published_date"],
                     i["view_count"],
                     i["like_count"],
                     i["comment_count"],
                     i["duration"]
                    )
                cursor.execute(query1,values)
        connection.commit()
            
#..... Getting video _id from video table.................
video_ids = col2.distinct("video_id")
vd=[]
vd.append(video_ids)


#insert comments into mysql.............................

def insert_into_comments():
    for vid in db.channel_data.find({"channel_name":{"$in":user_input}}):
        for i in db.video_data.find({"channel_id": vid['channel_id']}).limit(25):
         for com in db.comment_data.find({"video_id":i['video_id']}).limit(50):
            query="""INSERT INTO comment_data (comment_id,video_id,comment_text,comment_author,comment_published_date)
                VALUES (%s, %s, %s, %s, %s)"""
            values=(com['comment_id'],
                 com['video_id'],
                 com['comment_text'],
                 com['comment_author'],
                 com['comment_published_date'])
            cursor.execute(query,values)
    connection.commit()
questions = st.selectbox("Select any questions given below:",
                        ['Click the question that you would like to query',
                        '1. What are the names of all the videos and their corresponding channels?',
                        '2. Which channels have the most number of videos, and how many videos do they have?',
                        '3. What are the top 10 most viewed videos and their respective channels?',
                        '4. How many comments were made on each video, and what are their corresponding video names?',
                        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                        '6. What is the total number of likes for each video, and what are their corresponding video names?',
                        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                        '8. What are the names of all the channels that have published videos in the year 2022?',
                        '9. Which videos have the highest number of comments, and what are their corresponding channel names?'  
                        ]
                        )    
cursor = connection.cursor()
if questions == '1. What are the names of all the videos and their corresponding channels?':
    query1 = "select channel_name as Channel_name ,video_name as Video_names from channel_data c join video_data v on c.channel_id = v.channel_id;"
    cursor.execute(query1)
    result = cursor.fetchall()
    table1 = pd.DataFrame(result,columns=cursor.column_names)
    st.table(table1)
elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select channel_name,count(video_name) as Most_Number_of_Videos from video_data v join channel_data c on c.channel_id = v.channel_id group by channel_name order by count(video_name) desc;"
    cursor.execute(query2)
    result1 = cursor.fetchall()
    table2 = pd.DataFrame(result1,columns =cursor.column_names)
    st.table(table2)
    st.bar_chart(table2.set_index("channel_name"))
elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = "select channel_name as Channel_name,video_name as Video_name,view_count as Top_10_Viewed_Videos from channel_data c join video_data v on c.channel_id = v.channel_id order by view_count desc limit 10;"
    cursor.execute(query3)
    result2 = cursor.fetchall()
    table3 = pd.DataFrame(result2,columns=cursor.column_names)
    st.table(table3) 
elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select channel_name as Channel_name, video_name as Video_name,comment_count as Comments_Count from video_data v join channel_data c on c.channel_id = v.channel_id order by comment_count desc;"
    cursor.execute(query4)
    result3 = cursor.fetchall()
    table4 = pd.DataFrame(result3,columns=cursor.column_names)
    st.table(table4)
elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = "select channel_name as Channel_name,video_name as Video_name,like_count as Number_of_likes from video_data v join channel_data c on c.channel_id = v.channel_id order by like_count desc;"
    cursor.execute(query5)
    result4 = cursor.fetchall()
    table5 = pd.DataFrame(result4,columns=cursor.column_names)
    st.table(table5)   
elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
    query6 = "select channel_name as Channel_name,video_name as Video_name,like_count as Like_count from video_data v join channel_data c on c.channel_id = v.channel_id order by like_count desc;"
    cursor.execute(query6)
    result5 = cursor.fetchall()
    table6 = pd.DataFrame(result5,columns=cursor.column_names)
    st.table(table6)
elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select c.channel_name as Channel_name,c.channel_views as Total_No_of_views from video_data v join channel_data c on c.channel_id = v.channel_id group by c.channel_name,c.channel_views order by channel_views desc;"
    cursor.execute(query7)
    result6 = cursor.fetchall()
    table7 = pd.DataFrame(result6,columns=cursor.column_names)
    st.table(table7)
    st.bar_chart(table7.set_index("Channel_name"))
elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = "select distinct c.channel_name as Channel_name,year(published_date) as Published_year from channel_data c join video_data v on c.channel_id = v.channel_id where year(published_date) = 2022;"
    cursor.execute(query8)
    result7 = cursor.fetchall()
    table8 = pd.DataFrame(result7,columns=cursor.column_names)
    st.table(table8)
elif questions =='9. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query9 = "select channel_name as Channel_name,video_name as Video_name,comment_count as Highest_No_of_comments from channel_data c join video_data v on c.channel_id = v.channel_id order by comment_count desc limit 10;"
    cursor.execute(query9)
    result8 = cursor.fetchall()
    table9 = pd.DataFrame(result8,columns=cursor.column_names)
    st.table(table9)    
submit3 = st.button("enter")


if submit1:
 with st.spinner('Please wait '):
     time.sleep(5)
 cursor = connection.cursor()
 try:
    insert_into_channels()
    insert_into_videos()
 except Exception as error:
    print("Channel details already transformed !!",type(error).__name__, "–", error) 
 try:
    insert_into_comments()
    st.success("Transformation to MySQL Successful !!")
 except Exception as error:
    print("Comment details already transformed !!", type(error).__name__, "–", error) 
 st.success("Data Uploaded Successfully")
#Closing the Connection:
cursor.close() 
# analysis part................................
connection.close()
