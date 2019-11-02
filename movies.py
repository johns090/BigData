
sqlContext.sql("use stagedb;")

sqlContext.sql("insert overwrite ratings select distinct * from ratings")

cnt_df = sqlContext.sql("select distinct movieid from ratings where movieid not in (select movieid from movies)")

output = cnt_df.collect()
cnt=output[0][0]

if cnt > 0:
    print "Integration violation"
    exit(1)
else:
    print "Integration check passed"


# We can use LOAD DATA command as well.
sqlContext.sql("insert into targetdb.ratings select r.* from ratings r left join targetdb.ratings t on r.userid = t.userid and r.movieid = t.movieid and r.rating = t.rating and r.t_stamp = t.t_stamp where t.movieid is  null;")

