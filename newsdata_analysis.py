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
    """Method for conneting to postgresql database"""
    try:
        connection_obj = psycopg2.connect("dbname={}".format(db_name))
        cursor_obj = connection_obj.cursor()
        return connection_obj, cursor_obj
    except:
        print ("Failed to connect {} database".format(db_name))

#retriving newsdata
def retrive_results(Query):
    """Retrives data based on given query from postgresql database"""
    connection_obj, cursor_obj = database_connection()
    cursor_obj.execute(Query)
    return cursor_obj.fetchall()
    connection_obj.close()

#displaying newsdata
def display_results(results,s):
    """Displays the data retrived based on queries executed"""
    print(s)
    for i, r in enumerate(results[0]):
        print (str(i+1)+"."+str(results[0])+" - "+str(results[1]))

#displaying error days 
def display_error_results(results,s):
    print(s)
    for r in results[0]:
        d = r[0]
        date = datetime.strptime(d, "%Y-%m-%d")
        date_format = datetime.strftime(date, "%B %d, %Y")
        print (str(date_format)+" - "+str(r[1]) + "% errors")


if __name__ == '__main__':
    #retrive results
    articles = retrive_results(popular_articles_query)
    authors = retrive_results(popular_authors_query)
    errors = retrive_results(errors_days_query)

    #display results
    display_results(articles,"Most popular articles"+"\n"+"-----------------------")
    display_results(authors,"Most popular authors"+"\n"+"------------------------")
    display_error_results(errors,"Days on which more than 1% errors occured"+"\n"+"----------------------------------------------------")
    #writing retrived newsdata report in to text file
    f=open('newsdata.txt','w')
    f.write("Most popular articles"+"\n"+str(articles)+"\n"+"\n"+"Most popular authors"+"\n"+str(authors)+"\n"+"\n"+"Days on whih more than 1% requests lead to errors"+"\n"+str(errors))
    f.close()
