
from urllib import request
import json
import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import mysql.connector as mysql
import datetime

class ytdata:
        def __init__(self,youtube,Channel_id,playlist_id,video_ids):
              self.youtube=youtube
              self.channel_id=Channel_id
              self.playlistid=playlist_id
              self.videolist=video_ids


         ## Function to get Channel details     
        def get_ytchannel_stats(self):
             bind_data=[]
             request=self.youtube.channels().list(
                   part='snippet,contentDetails,statistics,status',
                   id=self.channel_id)
            # id=','.join(Channel_ids))
             response=request.execute()
             for i in range(len(response['items'])):
                   data=dict(
                         Channel_Name=response['items'][i]['snippet']['title'],
                    Channel_Id=response['items'][i]['id'],                    
                   Channel_Views=response['items'][i]['statistics']['viewCount'],
               Channel_Description=response['items'][i]['snippet']['description'],
                Channel_Status=response['items'][i]['status']['privacyStatus'],
                Channel_Type="null",
                PlayList_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
              Total_Videos=response['items'][i]['statistics']['videoCount'])
                   bind_data.append(data)
             return bind_data
        

        ## function to get playlist
        
## function to get Video _playlistitems_ Ids
        def get_videos_playList_ids(self):   
              video_ids=[] 
              request_playlist=self.youtube.playlistItems().list(
              part='snippet,contentDetails',
              playlistId =self.playlistid,
              maxResults = 4)
              response_playlist=request_playlist.execute()
              for i in range(len(response_playlist['items'])):
               Video_ID=dict(
                    PlaylistId=response_playlist['items'][i]['snippet']['playlistId'],
                         Title=response_playlist['items'][i]['snippet']['title'],
                          ChannelID=response_playlist['items'][i]['snippet']['channelId'],
                          videoID=response_playlist['items'][i]['contentDetails']['videoId'])
               video_ids.append(Video_ID)

              next_page_token=response_playlist.get('nextPageToken')
              more_pages=True

              while more_pages:
                    if next_page_token is None:
                         more_pages = False
                    else:
                         request_playlist=self.youtube.playlistItems().list(
                              part='snippet,contentDetails',
                              playlistId =self.playlistid,
                              maxResults = 4,
                              pageToken=next_page_token)
                         response_playlist=request_playlist.execute()
                         for i in range(len(response_playlist['items'])):
                              Video_ID=Video_ID=dict(
                                   PlaylistId=response_playlist['items'][i]['snippet']['playlistId'],
                                  Title=response_playlist['items'][i]['snippet']['title'],
                                  ChannelID=response_playlist['items'][i]['snippet']['channelId'],
                                  videoID=response_playlist['items'][i]['contentDetails']['videoId'])
                              video_ids.append(Video_ID)
            
                         next_page_token=response_playlist.get('nextPageToken')

              
              return video_ids           
##function to get video details
               
        def get_video_Details(self): 
             all_video_details=[]
             request_videolist=self.youtube.videos().list(
                       part='snippet,statistics,contentDetails',
                       id =','.join(self.videolist))
             response_videolist=request_videolist.execute()
             for i in range(len(response_videolist['items'])):
                   
                   video_details=dict(
                        # Channel_Name=response['items'][i]['snippet']['title'],
                         Title=response_videolist['items'][i]['snippet']['title'],
                         ChannelID=response_videolist['items'][i]['snippet']['channelId'],
                         ID=response_videolist['items'][i]['id'],
                         Video_Description=response_videolist['items'][i]['snippet']['description'],
                         Published_date=response_videolist['items'][i]['snippet']['publishedAt'],
                         Views=response_videolist['items'][i]['statistics']['viewCount'],
                         Likes=response_videolist['items'][i]['statistics']['likeCount'],
                         Dislikes='0',
                         Favorites=response_videolist['items'][i]['statistics']['favoriteCount'],
                         Comments=response_videolist['items'][i]['statistics']['commentCount'],
                         Duration=response_videolist['items'][i]['contentDetails']['duration'],
                         ThumbNailUrl=response_videolist['items'][i]['snippet']['thumbnails']['default']['url'],
                         captionstatus=response_videolist['items'][i]['contentDetails']['caption']
                         )
                   all_video_details.append(video_details)
             return all_video_details
        

             #get comment details
        def get_comment_details(self): 
             all_comment_details=[]
             for comment_video_id in self.videolist:
                    request_commentlist=self.youtube.commentThreads().list(
                    part="snippet",
                    videoId =comment_video_id,
                    maxResults=100)
                    response_commentlist=request_commentlist.execute()
                    for i in  range(len(response_commentlist['items'])):
                          comment_details=dict(
                                Comment_Id=response_commentlist['items'][i]['id'],
                                comment_Video_Id=response_commentlist['items'][i]['snippet']['videoId'],
                                comment_text=response_commentlist['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author=response_commentlist['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_Published_date=response_commentlist['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                         )
                          all_comment_details.append(comment_details)

             return all_comment_details
        
        def get_youtube_data(self):
              API_KEY="AIzaSyAyXZl_Dih88EU0dlMZEyU4faUHrJEgA4Q"
              conn=mysql.connect(host='localhost', password='Thayar@123',user='root', database='youtube_data')
              if conn.is_connected():
                st.write("connection established....")
                youtube=build('youtube','v3',developerKey=API_KEY)
                data=ytdata(youtube,Channel_id,None,None)
                channeldata=data.get_ytchannel_stats()
                channeldf=pd.DataFrame(channeldata)
                mycursor=conn.cursor()
                for item in channeldata :
                    channel_id=item["Channel_Id"]
                    channel_name=item["Channel_Name"]
                    channel_type=item["Channel_Type"]
                    channel_views=item["Channel_Views"]
                    channel_description=item["Channel_Description"]
                    channel_status=item["Channel_Status"]
                    query="insert into channel values("+"'"+channel_id+"'"+","+"'"+channel_name+"'"+","+"'"+channel_type+"'"+","+channel_views+","+"'"+channel_description+"'"+","+"'"+channel_status+"'"+")"
                    mycursor.execute(query)
                    conn.commit()
                    st.write("channel data inserted")
               #VideoIds from Playlist item
                playlistid=channeldf["PlayList_Id"].values[0]
                data=ytdata(youtube,None,playlistid,None)
                video_ids=data.get_videos_playList_ids()
                unique_playlist=list({v['PlaylistId']:v for v in video_ids }.values())
              #videos_list_df=pd.DataFrame(video_ids)
                for item in unique_playlist :
                    playlist_id=item["PlaylistId"]
                    channel_id=item["ChannelID"]
                    playlist_name=item["Title"]
                    query="insert into playlist values("+"'"+playlist_id+"'"+","+"'"+channel_id+"'"+","+"'"+playlist_name+"'"+")"
                    mycursor.execute(query)
                    conn.commit()
                    st.write("Playlist data inserted")

                ##Retrieve Video details
              videolist=[]
              for item in video_ids:
                 videoId=item['videoID']
                 videolist.append(videoId)

              video_playlistid= unique_playlist[0]['PlaylistId']  
              data=ytdata(youtube,None,None,videolist)
              videodetails=data.get_video_Details()
              commentdetails=data.get_comment_details()
              videodetailsdf=pd.DataFrame(videodetails)
              commentdetailsdf=pd.DataFrame(commentdetails)
              for item in videodetails :
                 video_id=item["ID"]
                 playlist_id=video_playlistid
                 video_name=item["Title"]
                 video_description=item["Video_Description"]
                 published_date=item["Published_date"]
                 view_count=item["Views"]
                 like_count=item["Likes"]
                 dislike_count=item["Dislikes"]
                 favourite_count=item["Favorites"]
                 comment_count=item["Comments"]
                 duration=item["Duration"]
                 thumbnail=item["ThumbNailUrl"]
                 caption_status=item["captionstatus"]
                 sql = """INSERT INTO videos (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count,dislike_count,favourite_count,comment_count,duration,thumbnail,caption_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"""
                 mycursor.execute(sql, (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count,dislike_count,favourite_count,comment_count,duration,thumbnail,caption_status))
                 conn.commit()
              st.write("Videos data inserted")
              for item in commentdetails :
               comment_id=item["Comment_Id"]
               video_id=item["comment_Video_Id"]
               comment_text=item["comment_text"]
               comment_author=item["comment_author"]
               comment_published_date=item["comment_Published_date"]
               sql = """INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_published_date)
                     VALUES (%s, %s, %s, %s, %s)"""
               mycursor.execute(sql, (comment_id, video_id, comment_text, comment_author, comment_published_date))
               conn.commit()
              st.write("comments data data inserted")
              conn.close()

        def get_channelids(self):
          conn=mysql.connect(host='localhost', password='Thayar@123',user='root', database='youtube_data')
          if conn.is_connected():
            print("connection established....")
            mycursor=conn.cursor()
            query="select channel_id from youtube_data.channel"
            mycursor.execute(query)
            records=mycursor.fetchall()
            conn.close()
          return [row[0] for row in records]
                       
        def getresult_dropdownvalue(self):
          conn=mysql.connect(host='localhost', password='Thayar@123',user='root', database='youtube_data')
          if conn.is_connected():
               print("connection established....")
               mycursor=conn.cursor()
               query="select * from  youtube_data.playlist where channel_id="+"'"+self.channel_id+"'"+""
               mycursor.execute(query)
               playlist_records=mycursor.fetchall()
               st.dataframe(playlist_records)
               value=[row[0] for row in playlist_records]
               playlist_id=str(value[0])

               query="select * from youtube_data.videos where playlist_id="+"'"+playlist_id+"'"+""
               mycursor.execute(query)
               videos_records=mycursor.fetchall()
               st.dataframe(videos_records)

               query="select * from youtube_data.comments"
               mycursor.execute(query)
               comments_records=mycursor.fetchall()
               conn.close()
               st.dataframe(comments_records)
          st.write("Retreived values")     



               


##API_KEY="AIzaSyDTEuSGeDbe-0cHF1TtIFn5QpinQxDbH4M"

# apikey "AIzaSyDsV3XamndmMCZF0XQrmOrxDLXo6e1eCH4"
#Channel_id="UCzh5hQc_O3r3xjh9sXrM7-A"
#Guvi email
st.title("YouTube Channel Details")
channelid=st.text_input("Enter Channel ID")
#channelid='UCTLRQJFrtiDPxl_yoOv-jWw'

if channelid:
      Channel_id=channelid.strip()
      st.write(Channel_id)
      data=ytdata(None,Channel_id,None,None)
      data.get_youtube_data()
      channelidsfromsql=data.get_channelids()
      
      dropdown_selected_value=st.selectbox("select the channel ID",channelidsfromsql)
      if dropdown_selected_value:
          data=ytdata(None,dropdown_selected_value,None,None)
          data.getresult_dropdownvalue()
     
     





   



                       
