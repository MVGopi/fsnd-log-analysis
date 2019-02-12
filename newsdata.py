#importing postgresql database adapter
import psycopg2
#datetime module for printing date properly
from datetime import datetime
#Most popular three articles of all time query
popular_articles_query = (
    "select articles.title, count(*) as views "
    "from articles inner join log on log.path "
    "like concat('%', articles.slug, '%') "
    "where log.status like '%200%' group by "
    "articles.title, log.path order by views desc limit 3")

#Most popular article authors of all time query
popular_authors_query = (
    "select authors.name, count(*) as views from articles inner "
    "join authors on articles.author = authors.id inner join log "
    "on log.path like concat('%', articles.slug, '%') where "
    "log.status like '%200%' group "
    "by authors.name order by views desc")

#Days on which more than 1% of requests lead to errors query
errors_days_query = (
    "select day, perc from ("
    "select day, round((sum(requests)/(select count(*) from log where "
    "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
    "perc from (select substring(cast(log.time as text), 0, 11) as day, "
    "count(*) as requests from log where status like '%404%' group by day)"
    "as log_percentage group by day order by perc desc) as final_query "
    "where perc >= 1")

#postgresql database connection
def database_connection(db_name="news"):
    """Postgresql database connection from python"""
    try:
        connetion_obj = psycopg2.connect("dbname={}".format(db_name))
        cursor_obj = db.cursor()
        return connetion_obj,cursor_obj
    except:
        print ("Failed to connect {} database".format(db_news))
#displaying output
def retrive_results(Query):
    """Retrives data based on given query from postgresql database"""
    connection_obj,cursor_obj = database_connection()
    cursor_obj.execute(Query)
    return cursor_obj.fetchall()
    connection_obj.close()


def display_results(results):
    """Displays the data retrived based on queries executed"""
    print (results[1])
    for i,r in enumerate(results[0]):
        print ("\t"+str(i+1)+"."+str(r[0])+" - "+str(r[1])+" views")


def display_error_results(results):
    print (results[1])
    for r in results[0]:
        D= results[0]
        date_obj = datetime.strptime(D, "%Y-%m-%d")
        date_formatted = datetime.strftime(date_obj, "%B %d, %Y")        
        print ("\t"+str(date_formatted)+" - "+str(results[1]) + "% errors")


if __name__ == '__main__':
    #retriving results
    articles = retrive_results(popular_articles_query)
    authors = retrive_results(popular_authors_query)
    errors = retrive_results(errors_days_query)

    #display results
    display_results(articles)
    display_results(authors)
    display_error_results(days)
    f=open('newsdata.txt','w')
    f.write("Most popular articles"+"\n"+str(articles)+"\n"+"\n"+"Most popular authors"+"\n"+str(authors)+"\n"+"\n"+"Days on whih more than 1% requests lead to errors"+"\n"+str(errors))
    f.close()
