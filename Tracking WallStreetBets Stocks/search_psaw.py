from psaw import PushshiftAPI
import datetime as dt
import config
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)  #Use for execute quries against DB

cursor.execute("""
    SELECT * FROM stock
""")

rows = cursor.fetchall()

stocks = {}
for row in rows:
    stocks['$' + row['symbol']] = row['id']  #match $ & sybol to stock id

api = PushshiftAPI()

start_epoch = int(dt.datetime(2021, 4, 1).timestamp())

submissions = list(api.search_submissions(after=start_epoch,
                            subreddit='wallstreetbets',
                            filter=['url','author', 'title', 'subreddit'])) # limit=10  can set limit

for submission in submissions:
    words = submission.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))

    if len(cashtags) > 0:
        print(cashtags)
        print(submission.title)

        for cashtag in cashtags:
            post_time = dt.datetime.fromtimestamp(submission.created_utc).isoformat()

            try:
                cursor.execute("""
                    INSERT INTO mention (dt, stock_id, message, source, url)
                    VALUES (%s, %s ,%s ,'wallstreetbets', %s)
                """,(post_time, stocks[cashtag], submission.title, submission.url))

                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()