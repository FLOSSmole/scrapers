# usage: fixRfRegDates.py

import MySQLdb
import re

# the pattern we are looking for is like this:
# Registered:&nbsp;2009-04-07 15:14
patternDate = re.compile(ur'Registered:&nbsp;(.+?)\-(.+?)\-(.+?)\s(\d\d)\:(\d\d)', re.UNICODE)

# Open local database connection
db1 = MySQLdb.connect(host="local",\
    user="user", \
    passwd="pass", \
    db="rubyforge", \
    use_unicode=True, \
    charset="utf8")
cursor1 = db1.cursor()
db1.autocommit(True)

# Open remote database connection
db2 = MySQLdb.connect(host="remote",\
    user="uer", \
    passwd="pass", \
    db="rubyforge", \
    use_unicode=True, \
    charset="utf8")
cursor2 = db2.cursor()
db2.autocommit(True)

# 1. pull list of projects with NULL registration dates
cursor1.execute('SELECT rp.proj_unixname, MAX( rp.date_registered ) \
                 FROM rf_projects rp \
                 GROUP BY 1 \
                 HAVING MAX( rp.date_registered ) IS NULL')
rows = cursor1.fetchall()
for row in rows:
    projectName = row[0]
    
    # 2. for that project, pull highest datasource_id we have for it
    cursor2 = db1.cursor()
    cursor2.execute('SELECT max(datasource_id) FROM rf_projects WHERE proj_unixname = %s',(projectName,))
    maxds = cursor2.fetchone()
    cursor2.close()
    
    # 3. for that project and that datasource_id, pull the indexhtml
    cursor3 = db1.cursor()
    print 'fetching index for proj [', projectName, '] and ds [', maxds[0], ']'
    cursor3.execute('SELECT indexhtml FROM rf_project_indexes WHERE proj_unixname = %s AND datasource_id = %s',(projectName,str(maxds[0])))
    indexhtml = cursor3.fetchone()
    if indexhtml != None:
        htmlcode = indexhtml[0]
        cursor3.close()
    
        # 4. Look for date_registered and write that to the rf_projects table
        try:
            yyyy = patternDate.search(htmlcode).group(1)
            mm = patternDate.search(htmlcode).group(2)
            dd = patternDate.search(htmlcode).group(3)
            hh = patternDate.search(htmlcode).group(4)
            minute = patternDate.search(htmlcode).group(5)
        
            newDateTime = yyyy + '-' + mm + '-' + dd + ' ' + hh + ':' + minute + ':00'
            print newDateTime
            cursor4 = db1.cursor()
            cursor4.execute('UPDATE rf_projects SET date_registered = %s WHERE proj_unixname = %s and datasource_id = %s',(newDateTime, projectName, str(maxds[0])))
            cursor4.close()
            cursor5 = db2.cursor()
            cursor5.execute('UPDATE rf_projects SET date_registered = %s WHERE proj_unixname = %s and datasource_id = %s',(newDateTime, projectName, str(maxds[0])))
            cursor5.close()
        except:
            pass
    
db1.close()
db2.close()
