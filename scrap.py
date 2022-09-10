import json
import os
from selenium import webdriver
import time
import gridfs
from urllib.request import urlopen as urReq
import re
from pytube import YouTube
import mysql.connector as conn
import base64
from bs4 import BeautifulSoup as bs
from requests_html import HTMLSession
import pymongo
from youtube_comment_scraper_python import youtube
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = os.environ.get("GOOGLE-CHROME-BIN")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
wd = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)
client = pymongo.MongoClient(
    "mongodb+srv://<username:passwd>@cluster0.k72vk3w.mongodb.net/?retryWrites=true&w=majority")
db = client.imagedb
print(db)

my_db = conn.connect(host='localhost', user='root', passwd='password')
cursor = my_db.cursor()
cursor.execute('create database if not exists yt_project')
cursor.execute(
    'create table if not exists yt_project.Youtubers_Table(`Name` varchar(90),Title varchar(600), video_link nvarchar(600),likes int,no_comments varchar(90))')


def fetch_urls(query: str, max_links_to_fetch: int, wd: webdriver, sleep_between_interactions: int = 10):
  
    def scroll(wd):
        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep_between_interactions)

    query = query.replace(" ", "+")
    html_link = urReq("https://www.youtube.com/results?search_query=" + query)
    video_id = re.findall(r"watch\?v=\S{11}", html_link.read().decode())
    video_ids = []
    for i in video_id:
        if i not in video_ids:
            video_ids.append(i)
    video_play = []
    for i in range(0, len(video_ids)):
        video_play.append("https://www.youtube.com/" + video_ids[i])
        i += 1
    result = {}
    session = HTMLSession()
    for i in range(0, max_links_to_fetch):
        wd.get(video_play[i])
        subscribe = WebDriverWait(wd, 5).until(EC.visibility_of_element_located((By.XPATH, "//yt-formatted-string[text()='Subscribe']")))
        wd.execute_script("arguments[0].scrollIntoView(true);", subscribe)
        no_comments = WebDriverWait(wd, 10).until(EC.visibility_of_element_located((By.XPATH, "//h2[@id= 'count']/yt-formatted-string"))).text

        time.sleep(1)
        response = session.get(video_play[i])
        response.html.render(timeout=120)
        soup = bs(response.html.html, "html.parser")
        result["title"] = soup.find("meta", itemprop="name")["content"]

        data = re.search(r"var ytInitialData = ({.*?});", soup.prettify()).group(1)
        data_json = json.loads(data)
        videoPrimaryInfoRenderer = data_json['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0][
            'videoPrimaryInfoRenderer']
        videoSecondaryInfoRenderer = \
            data_json['contents']['twoColumnWatchNextResults']['results']['results']['contents'][1][
               'videoSecondaryInfoRenderer']
    # number of likes
        likes_label = \
            videoPrimaryInfoRenderer['videoActions']['menuRenderer']['topLevelButtons'][0]['toggleButtonRenderer'][
                'defaultText']['accessibility']['accessibilityData']['label']  # "No likes" or "###,### likes"
        likes_str = likes_label.split(' ')[0].replace(',', '')
        result["likes"] = '0' if likes_str == 'No' else likes_str
        channel_name = soup.find("span", itemprop="author").next.next['content']
        link = response.html.base_url
        title = result["title"]
        likes = result["likes"]
        cursor.execute(f"insert into yt_project.Youtubers_Table(`Name`, Title, no_comments, likes, video_link) values('{channel_name}', '{title}', '{no_comments}', '{likes}', '{link}')")
        my_db.commit()
    wd.close()

    target_folder = os.path.join('./videos', '_'.join(query.lower().split(' ')))
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    for i in range(0, max_links_to_fetch):
        try:
            links = YouTube(video_play[i])
            links.streams.filter(file_extension="mp4").get_by_itag(18).download(target_folder)
        except:
            print("There is some error!")
        print("video downloaded successfully")

    for i in range(max_links_to_fetch):
        yt_comments = []
        data1 = []
        link = video_play[i]
        youtube.open(link)
        youtube.keypress("pagedown")
        current_page = youtube.get_page_source()
        lastpagesource = ''
        while True:
            if lastpagesource == current_page:
                break
            lastpagesource = current_page
            response = youtube.video_comments()
            for c in response['body']:
                if c not in data1:
                    data1.append(c)
            youtube.scroll()
            current_page = youtube.get_page_source()

        for j in range(len(data1)):
            if type(data1[j]) == dict:
                if data1[j]["Comment"] not in yt_comments:
                    yt_comments.append(data1[j]["Comment"])
                    record = {query + " comments": yt_comments}

        print(record)
        database = client["youtube_data"]
        collection = database[query]
        collection.insert_one(record)

        # get all image thumbnail results
        exp = "^.*((youtu.be\/)|(v\/)|(\/u\/\w\/)|(embed\/)|(watch\?))\??v?=?([^#&?]*).*"
        s = re.findall(exp, video_play[i])[0][-1]
        image = f"https://i.ytimg.com/vi/{s}/maxresdefault.jpg"
        image_string = base64.b64encode(urReq(image).read())
        thumbnail = gridfs.GridFS(database)
        thumbnail.put(image_string, content_type='image/jpeg')


def search_and_download(search_link: str, driver_path: str, target_path='./videos', number_videos=2):
    target_folder = os.path.join(target_path, '_'.join(search_link.lower().split(' ')))

    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    with webdriver.Chrome(executable_path=driver_path) as wd:
        fetch_urls(query=search_link, max_links_to_fetch=number_videos, wd=wd, sleep_between_interactions=400)


DRIVER_PATH = r'chromedriver.exe'
search_term = "hitesh chaudhary"
search_and_download(search_link=search_term, driver_path=DRIVER_PATH, number_videos=2)
