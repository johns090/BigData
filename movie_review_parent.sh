step = 'tail -l logfile.log | cut -d ',' -f 2 '

if [ $step -le 10]
then 
	impala-shell -f dedupe.hql
	if [$> -lt 0]
	then 
		echo "dedupe fails"
		exit 1
		
	fi
fi

step = 10
echo "load_movie_review, $step"  >> logfile.log
if [ $step -le 20]
then 
	sh movies.sh

	if [$> -lt 0]
	then 
		echo "movie load fails"
		exit 1
		
	fi
fi

step = 20
echo "load_movie_review, $step"  >> logfile.log

if [ $step -lt 30]
then 
sh ratings.sh

if [$> -ne 0]
then 
	echo "ratings load fails"
	exit 1
fi

step = 30
echo "load_movie_review, $step"  >> logfile.log

sh tags.sh

if [$step -lt 40]
then 
	echo "tags load fails"
	exit 1
fi

step = 40
echo "load_movie_review, $step"  >> logfile.log

step = 0

