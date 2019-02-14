# !/usr/bin/env python3

#importing postgresql database adapter
import psycopg2

#Query for most popular articles
most_popular_articles_query = """SELECT articles.title, count(log.path) as\
                    views FROM articles, log WHERE\
                    log.path=('/article/' || articles.slug)\
                    GROUP BY articles.title ORDER BY views desc limit 3"""

#Query for most popular authors
most_popular_authors_query = """SELECT authors.name, count(log.path) as views FROM\
                 articles, log, authors\
                 WHERE log.path=('/article/'||articles.slug)\
                 AND articles.author = authors.id \
                 GROUP BY authors.name ORDER BY views desc"""

#Query for finding error days
retrive_errors_days_query = """ SELECT total.day,
          ROUND(((errors.error_requests*1.0) / total.requests), 3) AS percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS error_requests
          FROM log
          WHERE status LIKE '404%'
          GROUP BY day
        ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS requests
          FROM log
          GROUP BY day
          ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.error_requests*1.0) / total.requests), 3) > 0.01)
        ORDER BY percent DESC; """

# connection to our database
def database_connection(db_name="news"):
    """Instance for connecting to postgresql database"""
    try:
        conn_obj=psycopg2.connect("dbname={}".format(db_name))
        return conn_obj
    except:
        print("Failed  to connect {} database".foramt(db_name))


#Most popular articles of all time
def most_popular_articles(Query,S):
    """Instance for finding most popular articles of all time"""
    print(S)
    conn_obj=database_connection()
    cur_obj = conn_obj.cursor()
    cur_obj.execute(most_popular_articles_query)
    popular_articles = cur_obj.fetchall()
    for articles in popular_articles:
        print ('''"%s" __ %s views''' % (articles[0], articles[1]))
    cur_obj.close()
    return popular_articles




#Most popualar authors
def most_popular_authors(Query,S):
    """Instance for finding most popular authors of all time"""
    print(S)
    conn_obj=database_connection()
    cur_obj = conn_obj.cursor()
    cur_obj.execute(most_popular_authors_query)
    popular_authors = cur_obj.fetchall()
    for authors in popular_authors:
        print("%s __ %s views" % (authors[0], authors[1]))
    cur_obj.close()
    return popular_authors


#Days on which more than 1% requests lead to errors
def errors_percent(Query,S):
    """Instance for days on which more than 1% requests leads to errors"""
    print(S)
    conn_obj=database_connection()
    cur_obj = conn_obj.cursor()
    cur_obj.execute(retrive_errors_days_query)
    error_days = cur_obj.fetchall()
    for day in error_days:
        d = day[0].strftime('	%B %d, %Y')
        e = str(round(day[1]*100, 1)) + "%" + "  errors"
        print(d+ '__ '+e)
    cur_obj.close()
    return error_days

#closing database connection
conn_obj=database_connection()
conn_obj.close()

if __name__ == '__main__':
    most_popular_articles(most_popular_articles_query,'Most popular articles of all time'+'\n'+'---------------------------------')
    most_popular_authors(most_popular_authors_query,'\n'+'\n'+'Most popular authors of all time'+'\n'+'--------------------------------')
    errors_percent(retrive_errors_days_query,'\n'+'\n'+'Days did more than 1% of requests leads to errors'+'\n'+'-------------------------------------------------')
