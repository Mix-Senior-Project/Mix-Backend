import sys
import logging
import rds_config
import pymysql
import json
import boto3
import os
import math

#rds settings
rds_host  = os.environ['rdsHost']
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def is_s3(url): 
    return 's3.amazonaws.com' in url or 's3://mixbucket/' in url

def create_presigned_url(bucket_name, object_name, expiration=600):
    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3',region_name="us-east-1",config=boto3.session.Config(signature_version='s3v4',))
    try:
        response = s3_client.generate_presigned_url('get_object',Params={'Bucket': bucket_name,'Key': object_name},ExpiresIn=expiration)
    except Exception as e:
        print(e)
        logging.error(e)
        return "Error"

    return response

def is_banned(userID, groupID): 
    with conn.cursor() as cur: 
        conn.commit()
        cur.execute(("SELECT banned FROM group_table WHERE group_id =%s"), groupID)
        bannedResult = cur.fetchone()
    conn.commit()
    
    if bannedResult is None or bannedResult == "null" or bannedResult[0] is None or bannedResult[0] == "null": 
        return False
    
    bannedList = json.loads(bannedResult[0])
    for user in bannedList["banned"]:
        if user["userID"] == userID: 
            return True
        
    return False;     

def lambda_handler(event, context):
    """
    This function fetches content from MySQL RDS instance
    """
    
    try:
        nameType = event['queryStringParameters']['nameType']
        id = event['queryStringParameters']['id']
        pageStr = event['queryStringParameters']['page']
        page = int(pageStr) - 1
    except:
        return {
            'statusCode': 400,
            'body': json.dumps("Bad request: incorrect parameters", default=str)
        }
    
    # username = event['queryStringParameters']['username']
    
    queryString = ""
    banned = ""
    if nameType == "user": 
        queryString = "SELECT * FROM post WHERE poster_id =%s ORDER BY creation_date DESC"
    elif nameType == "group":
        queryString = "SELECT * FROM post WHERE group_id =%s ORDER BY creation_date DESC"
    else: 
        return {
            'statusCode': 400, 
            'body': json.dumps("Bad request: nameType must be either 'user' or 'group.'", default=str)
        }
    
    if page < 0:
        return {
            'statusCode': 400, 
            'body': json.dumps("Bad request: page must be a positive integer.", default=str)
        }
    
    if id == "null" or id is None: 
        return {
            'statusCode': 400,
            'body': json.dumps("Bad request: user/group name could not be found", default=str)
        }
    
    with conn.cursor() as cur:
        conn.commit()
        cur.execute(queryString, id)
        result = cur.fetchall()
    conn.commit()
    
    numPosts = len(result)
    print("Number of posts: ")
    print(numPosts)
    
    postsPerPage = 10
    pagePosts = page * postsPerPage
    
    numPages = math.ceil(numPosts / postsPerPage)
    
    data = []
    for i in range(pagePosts, (pagePosts + postsPerPage)):
        try:
            testVar = result[i]
        except IndexError:
            print("Out of bounds, no more posts!")
            break
        
        postID = result[i][0]
        
        #update views for each post rendered 
        with conn.cursor() as cur:
            conn.commit()
            cur.execute("UPDATE post SET views = views + 1 WHERE guid =%s",postID)
        conn.commit()
        
        s3URL = result[i][1]
        
        createDate = json.dumps(result[i][2], default=str)
        createDate = createDate.replace("\\","")
        createDate = createDate.replace("\"","")
        
        posterID = result[i][3]
        
        if nameType == "group": 
            if is_banned(posterID, id):
                continue
        
        groupID = result[i][4]
        
        caption = result[i][5]
        
        edited = json.dumps(result[i][6], default=str)
        
        tmp = json.dumps(result[i][7], default=str)
        if tmp != "null": 
            comments = json.loads(result[i][7])
        else: 
            comments = "null"
            
        commentList = []
        if comments != "null" and comments is not None: 
            for comment in comments["comments"]:
                text = comment["text"]
                commenter = comment["username"]
                
                commentData = {
                    "text": text,
                    "username": commenter
                }
                commentList.append(commentData)    
        
        if len(commentList) == 0:
            commentList = "null"
        
        # likes = json.dumps(result[i][8], default=str)
        # likes = likes.replace("\\","")
        # likes = likes.replace("\"","")
        likes = json.dumps(result[i][8], default=str)
        if likes != "null":
            likes = json.loads(result[i][8])
        else:
            likes = "null"
        
        dislikes = json.dumps(result[i][9], default=str)
        if dislikes != "null":
            dislikes = json.loads(result[i][9])
        else:
            dislikes = "null"

        dislikeList = []
        if dislikes == "null" or dislikes is None: 
            dislikeList = "null"
        else: 
            for dislike in dislikes["dislikes"]:
                dislikeList.append(dislike)
        
        views = result[i][10]
        
        tmp = s3URL
        purl = ""
        if tmp == None or str(tmp) == "null": 
            purl = "null"
        else: 
            if is_s3(tmp): 
                obj = tmp.replace("s3://mixbucket/","")
                purl = create_presigned_url('mixbucket',obj,3600)
                if purl == "Error": 
                    return {
                        'statusCode': 403, 
                        'body': "unable to make S3 pre-signed URL"
                    }
            else: 
                purl = tmp
                
        with conn.cursor() as cur:
            conn.commit()
            cur.execute("SELECT username FROM user_table WHERE user_id =%s", posterID)
            posterName = cur.fetchone()
        conn.commit()
        
        with conn.cursor() as cur:
            conn.commit()
            cur.execute("SELECT group_name FROM group_table WHERE group_id =%s", groupID)
            groupName = cur.fetchone()
        conn.commit()
        
        dataList = ""
        if likes == "null" or likes is None:
            likes = "null"
            dataList = {
                "ID": postID,
                "s3_url": purl,
                "timestamp": createDate, 
                "posterID": posterID,
                "username": posterName[0],
                "groupID": groupID, 
                "groupName": groupName[0],
                "caption": caption,
                "edited": edited, 
                "comments": commentList,
                "dislikes": dislikeList,
                "views": views,
                "likes": likes
            }
        else:
            dataList = {
                "ID": postID,
                "s3_url": purl,
                "timestamp": createDate, 
                "posterID": posterID,
                "username": posterName[0],
                "groupID": groupID, 
                "groupName": groupName[0],
                "caption": caption,
                "edited": edited, 
                "comments": commentList,
                "dislikes": dislikeList,
                "views": views
            }
            dataList.update(likes)
        data.append(dataList)
    
    print("Data: ")
    print(data)
    if not data: 
        print("No results on this page.")
        return {
            'statusCode': 400,
            'body': "There are no posts on this page. Please try a lower page number"
        }
        
    if result == None:
         return {
            'statusCode': 500,
            'body': json.dumps("Failed to get posts. No posts found for given user or group. src: rds-batch-posts-made", default=str)
        }
    
    finalData = {
        "numPages": numPages,
        "posts": data
    }
    
    return {
        'statusCode': 200,
        'body': json.dumps(finalData,default=str)
    }
