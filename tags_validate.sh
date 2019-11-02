
-- this can be used:  insert overwrite table tags select distinct * from tags;

impala-shell -d stagedb -q "select distinct movieid from tags where movieid not in (select movieid from movies)" -o output.out -B --quiet --output_delimiter "," 


cnt=`cat output.out`



if [ "$cnt" != "" ]

then

    echo "integration check between movies and tags violation within stage table"

    exit 1

else

    echo "integration check between movies and tags check passed within stage table"

    rm -f output.out

fi




impala-shell -d stagedb -q "insert into targetdb.tags select r.* from tags r left join targetdb.tags t on r.userid = t.userid and r.movieid = t.movieid and r.tag = t.tag and r.t_stamp = t.t_stamp where t.movieid is  null;" -o output.out -B --quiet --output_delimiter ","
