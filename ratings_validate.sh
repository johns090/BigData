


impala-shell -d stagedb -q "select distinct movieid from ratings where movieid not in (select movieid from movies)" -o output.out -B --quiet --output_delimiter "," 


cnt=`cat output.out`



if [ "$cnt" != "" ]

then

    echo "integration check between movies and ratings violation within stage table"

    exit 1

else

    echo "integration check between movies and ratings check passed within stage table"

    rm -f output.out

fi




impala-shell -d stagedb -q "insert into targetdb.ratings select r.* from ratings r left join targetdb.ratings t on r.userid = t.userid and r.movieid = t.movieid and r.rating = t.rating and r.t_stamp = t.t_stamp where t.movieid is  null;" -o output.out -B --quiet --output_delimiter ","
