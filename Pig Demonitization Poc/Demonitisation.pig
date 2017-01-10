load_tweets = LOAD '/usr/pig_assignment/demonetization-tweets.csv' USING PigStorage(',');
extract_details = FOREACH load_tweets GENERATE $0 as id,$1 as text;

tokens = foreach extract_details generate id,text, FLATTEN(TOKENIZE(text)) As word;

dictionary = load '/usr/pig_assignment/AFINN.txt' using PigStorage('\t') AS(word:chararray,rating:int);

word_rating = join tokens by word left outer, dictionary by word using 'replicated';

describe word_rating;

rating = foreach word_rating generate tokens::id as id,tokens::text as text, dictionary::rating as rate;
 	
word_group = group rating by (id,text);

avg_rate = foreach word_group generate group, AVG(rating.rate) as tweet_rating;

positive_tweets = filter avg_rate by tweet_rating>=0;

STORE positive_tweets INTO '/usr/pig_assignment/output/positive' using PigStorage(',');

negative_tweets = filter avg_rate by tweet_rating<0;

STORE negative_tweets INTO '/usr/pig_assignment/output/negative' using PigStorage(',');

