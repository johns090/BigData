
impala-shell -d stagedb -q "select count(*) from movies where movieid is null" -o output.out -B --quiet

cnt=`cat output.out`

if [ $cnt -gt 0 ]
then
    echo "Null violation"
    exit 1
else
    echo "Null check passed"
    rm -f output.out 
fi

impala-shell -d stagedb -q "select movieid, count(*) from movies group by movieid having count(*)>1" -o output.out -B --quiet --output_delimiter "," 

cnt=`cat output.out`

if [ "$cnt" != "" ]
then
    echo "UPI violation within stage table"
    exit 1
else
    echo "UPI check passed within stage table"
    rm -f output.out
fi


impala-shell -d stagedb -q "select count(*) from movies m1 join targetdb.movies m2 on m1.movieid = m2.movieid" -o output.out -B --quiet --output_delimiter ","

cnt=`cat output.out`

if [ $cnt -gt 0 ]
then
    echo "UPI violation with the target table"
    exit 1
else
    echo "UPI check passed with the target table"
    rm -f output.out
fi

# We can use LOAD DATA command as well.
impala-shell -d stagedb -q "insert into targetdb.movies select * from movies" -o output.out -B --quiet --output_delimiter ","

