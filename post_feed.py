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
    return 's3.amazonaws.com' in url or 's3://**********/' in url

def group_exists(groupID): 
    query = "SELECT COUNT(*) FROM group_table WHERE group_id = %s"
    
    with conn.cursor() as cur: 
        conn.commit()
        cur.execute(query, groupID)
        result = cur.fetchone()
    conn.commit()
    if result == 0 or result[0] == 0: 
        return False 

    return True
    
def user_exists(userID): 
    query = "SELECT COUNT(*) FROM user_table WHERE user_id = %s"
    
    with conn.cursor() as cur: 
        conn.commit()
        cur.execute(query, userID)
        result = cur.fetchone()
    conn.commit()
    if result == 0 or result[0] == 0: 
        return False 

    return True

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

def not_blocked(userID, posterID):
    checkBlockedQuery = "SELECT blocked FROM user_table WHERE user_id = %s"
    
    posterList = [posterID]
    posterJsonStr = json.dumps(posterList)

    with conn.cursor() as cur: 
        conn.commit()
        cur.execute(checkBlockedQuery, userID)
        blockedUserResult = cur.fetchone()
    conn.commit()
    
    if blockedUserResult[0] is None or blockedUserResult[0] == "null": 
        return True
        
    blockedUsers = json.loads(blockedUserResult[0])
    
    if blockedUsers is None:
        return True
    for blockedUser in blockedUsers: 
        if blockedUser == posterID:
            return False
            
    return True 

def is_public_group(groupID): 
    checkPrivate = "SELECT private FROM group_table WHERE group_id = %s"
    
    with conn.cursor() as cur: 
        conn.commit()
        cur.execute(checkPrivate, groupID)
        publicityResult = cur.fetchone()
    conn.commit()

    if publicityResult[0] == 0: 
        return True
        
    return False 
    
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

def calculate_ratio(post):
    # print(f"Post for ratio: {post}")
    try: 
        likes = json.loads(post[8])
    except: 
        likes = "null"
    
    try: 
        dislikes = json.loads(post[9])
    except: 
        dislikes = "null"
        
    # print(f"Likes: {likes} \n Dislikes: {dislikes}")
    likeCount = 0
    dislikeCount = 0
    
    if likes is not None and likes != "null": 
        likeCount = len(likes["likes"])

    if dislikes is not None and dislikes != "null": 
        dislikeCount = len(dislikes["dislikes"])
        
        
    # print(f"LikeCount: {likeCount} \n DislikeCount: {dislikeCount}")
    if dislikeCount == 0: 
        return likeCount
    else: 
        return (likeCount / dislikeCount)

def lambda_handler(event, context):
    """
    This function fetches content from MySQL RDS instance
    """
    
    try:
        userID = event['queryStringParameters']['userID']
        pageStr = event['queryStringParameters']['page']
        page = int(pageStr) - 1
    except:
        return {
            'statusCode': 400,
            'body': json.dumps("Bad request: incorrect parameters", default=str)
        }
    
    if page < 0:
        return {
            'statusCode': 400, 
            'body': json.dumps("Bad request: page must be a positive integer.", default=str)
        }
    
    with conn.cursor() as cur:
        conn.commit()
        cur.execute("SELECT username FROM user_table WHERE user_id = %s", userID)
        checkUserResult = cur.fetchone()
    conn.commit()

    
    if checkUserResult is None: 
        return {
            'statusCode': 404, 
            'body': "Error, could not find user with the given ID."
        }
    
    #get joined groups 
    with conn.cursor() as cur:
        conn.commit()
        cur.execute("SELECT groups_joined FROM user_table WHERE user_id = %s", userID)
        groupResults = cur.fetchone()
    conn.commit()
    
    groups = json.loads(groupResults[0])
    
    feedPosts = []
    
    #get posts from joined groups within the past 3 days 
    if groups is not None and groups != "null": 
        for group in groups["groups"]: 
            with conn.cursor() as cur:
                conn.commit()
                cur.execute("SELECT * FROM post WHERE group_id = %s AND creation_date >= DATE_SUB(NOW(), INTERVAL 3 DAY) ORDER BY creation_date DESC", group)
                groupPostsResults = cur.fetchall()
            conn.commit()
        
            for post in groupPostsResults: 
                posterID = post[3]
                groupID = post[4]
                if not group_exists(groupID) or not user_exists(posterID): 
                    continue
                if is_banned(posterID, groupID): 
                    continue
                if not_blocked(userID, posterID) and not_blocked(posterID, userID): 
                    feedPosts.append(post)
        
    print("POSTS FROM JOINED GROUPS: ")
    print(feedPosts)
    
    feedPosts = sorted(feedPosts, key=lambda x: x[2], reverse=True)
    
    with conn.cursor() as cur:
        conn.commit()
        cur.execute("SELECT interests FROM user_table WHERE user_ID = %s", userID)
        interestsResult = cur.fetchone()
    conn.commit()
    
    interests = json.loads(interestsResult[0])
    recommendedPosts = []
    if interests is not None and interests != "null": 
        for interest in interests["interests"]:
            getInterestGroupQuery = """
            SELECT group_id
            FROM group_table
            WHERE JSON_CONTAINS(group_interests->'$."group_interests"', %s)
            """
            
            interestList = [interest]
            interestJsonStr = json.dumps(interestList)
            
            #gets groups that have common interest with user's list 
            with conn.cursor() as cur: 
                conn.commit()
                cur.execute(getInterestGroupQuery, (interestJsonStr,))
                interestGroupResult = cur.fetchall()
            conn.commit()
            
            if interestGroupResult is not None: 
                for interestGroup in interestGroupResult: 
                    with conn.cursor() as cur: 
                        conn.commit()
                        cur.execute("SELECT * FROM post WHERE group_id = %s ORDER BY creation_date DESC LIMIT 1", interestGroup[0])
                        recentGroupPost = cur.fetchone()
                    conn.commit()

                    if recentGroupPost is not None and recentGroupPost != "null":
                        if not group_exists(recentGroupPost[4]) or not user_exists(recentGroupPost[3]): 
                            continue
                        if (not_blocked(userID, recentGroupPost[3]) and not_blocked(recentGroupPost[3], userID)) and recentGroupPost not in feedPosts and recentGroupPost not in recommendedPosts and is_public_group(recentGroupPost[4]):
                            if is_banned(recentGroupPost[3], recentGroupPost[4]): 
                                continue
                            recommendedPosts.append(recentGroupPost)

    print("RECOMMENDED POSTS: ")
    print(recommendedPosts)
    
    if len(recommendedPosts) > 0: 
        recommendedPosts = sorted(recommendedPosts, key=lambda x: x[2], reverse=True)
        feedPosts.extend(recommendedPosts)
        
    print("FEED SO FAR: ")
    print(feedPosts)
    queryString = "SELECT * FROM post ORDER BY creation_date DESC"
    
    with conn.cursor() as cur:
        conn.commit()
        cur.execute(queryString)
        result = cur.fetchall()
    conn.commit()
    
    
    sortedPosts = sorted(result, key=lambda post: calculate_ratio(post), reverse=True)
    # print(f"Pre-sort: {result}\nPost-sort:{sortedPosts}")
    for post in sortedPosts: 
        if not group_exists(post[4]) or not user_exists(post[3]): 
            continue
        if (not_blocked(userID, post[3]) and not_blocked(post[3], userID)) and post not in feedPosts and is_public_group(post[4]):
            if is_banned(post[3], post[4]):
                continue
            feedPosts.append(post)
    
    numPosts = len(feedPosts)
    print("Number of posts: ")
    print(numPosts)
    
    postsPerPage = 10
    pagePosts = page * postsPerPage
    
    numPages = math.ceil(numPosts / postsPerPage)
    
    if page > numPages: 
        print("No results on this page.")
        return {
            'statusCode': 400,
            'body': "There are no posts on this page. Please try a lower page number"
        }
    
    data = []
    for i in range(pagePosts, (pagePosts + postsPerPage)):
        try:
            testVar = feedPosts[i]
        except IndexError:
            print("Out of bounds, no more posts!")
            break
        
        postID = feedPosts[i][0]
        
        #update views for each post rendered 
        with conn.cursor() as cur:
            conn.commit()
            cur.execute("UPDATE post SET views = views + 1 WHERE guid =%s",postID)
        conn.commit()
        
        s3URL = feedPosts[i][1]
        
        createDate = json.dumps(feedPosts[i][2], default=str)
        createDate = createDate.replace("\\","")
        createDate = createDate.replace("\"","")
        
        posterID = feedPosts[i][3]
        
        groupID = feedPosts[i][4]
        
        if is_banned(posterID, groupID): 
            continue
        
        caption = feedPosts[i][5]
        
        edited = json.dumps(feedPosts[i][6], default=str)
        
        tmp = json.dumps(feedPosts[i][7], default=str)
        if tmp != "null": 
            comments = json.loads(feedPosts[i][7])
        else: 
            comments = "null"
        
        commentList = []
        
        if comments != "null" and comments is not None and comments != 0: 
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
            
        likes = json.dumps(feedPosts[i][8], default=str)
        if likes != "null":
            likes = json.loads(feedPosts[i][8])
        else:
            likes = "null"

        dislikes = json.dumps(feedPosts[i][9], default=str)
        if dislikes != "null": 
            dislikes = json.loads(feedPosts[i][9])
        else: 
            dislikes = "null"
            
        dislikeList = []
        if dislikes == "null" or dislikes is None: 
            dislikeList = "null"
        else: 
            for dislike in dislikes["dislikes"]:
                dislikeList.append(dislike)
        
        views = feedPosts[i][10]
        
        tmp = s3URL
        purl = ""
        if tmp == None or str(tmp) == "null": 
            purl = "null"
        else: 
            if is_s3(tmp): 
                obj = tmp.replace("s3://********/","")
                purl = create_presigned_url('********',obj,3600)
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
        
    if feedPosts == None:
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
