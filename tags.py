sqlContext.sql("insert overwrite tags select distinct * from tags")

cnt_df = sqlContext.sql("select distinct movieid from tags where movieid not in (select movieid from movies)")

output = cnt_df.collect()
cnt=output[0][0]

if cnt > 0:
    print "Integration violation"
    exit(1)
else:
    print "Integration check passed"


# We can use LOAD DATA command as well.
sqlContext.sql("insert into targetdb.tags select r.* from tags r left join targetdb.tags t on r.userid = t.userid and r.movieid = t.movieid and r.tag = t.tag and r.t_stamp = t.t_stamp where t.movieid is  null;")
