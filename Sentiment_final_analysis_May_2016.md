

```python
%run /Users/admin/pycommon
```

# Data Sampling


```python
# Execute SQL to sample posts tagged 'politics', 'life', 'design', 'education' or 'tech', their full text, timestamp, total reading time (ttr), drafting time, reccomends, responses and tags

# TABLESAMPLE function should be available in Postgresql 9.5 - go/sql doesn't recognize it, neither does databricks. TABLESAMPLE BERNOULLI (10) REPEATABLE(200) would also be great, tho similarly unavailable. 

# 236632 total articles across these 5 categories. Random sample of %30 is about 71000. 32472 'Politics' posts, 56002 'Tech, 28601 'Education', 41684 'Design', 78049 'Life'.  Sampling random %30 should be representative for each genre, though not necessarily over time, since we can't sample a given percentage by genre by day. Since bulk (87.4%) of articles seem to be after Aug 1, 2015, query below samples 80000 random articles across all 5 genres from Aug 1 2015 to present date. From 8-1-2015 to 4-2-2016, 206845 total articles across all genres, 29672 'Politics', 46388 'Tech', 25775 'Education', 35694 'Design', 69361 'Life'.  This is a random sample of about 36% of population, n=206845. 

query = """
-- Select relevant posts, their text and reading features 
WITH raw_posts AS (
  SELECT
    posts.post_id AS ID
  , latest_versions.display_title
  , TIMESTAMP 'epoch' + posts.first_published_at / 1000 * INTERVAL '1 Second' AS first_published_at
  , JSON_EXTRACT_PATH_TEXT(latest_versions.content, 'bodyModel', 'paragraphs') AS paragraphs
  FROM posts
  -- Full inner join with users, latest_versions and post_features to
  -- make sure we only consider posts that have all data that we need
  JOIN users ON users.user_id = posts.creator
  JOIN latest_versions ON latest_versions.post_id = posts.post_id
  WHERE
    -- Filter posts that are actually published
    posts.first_published_at > 0
    AND (posts.visibility = 0 OR posts.visibility = '')
    AND posts.deleted_at = 0
    AND posts.blacklisted_at = 0
    AND users.blacklisted_at = 0
    AND latest_versions.is_published
    -- Filter posts that are in English
    AND posts.detected_language = 'en'
    -- Filter posts with tags related to 'politics'
    AND posts.post_id IN( 
      SELECT p2.post_id 
      FROM posts AS p2 
      JOIN tag_post_relations as tog ON p2.post_id = tog.post_id
      WHERE tog.tag_slug SIMILAR TO '%politics%|%life%|%design%|%education%|%tech%'
      )
  -- Remove posts that are responses
  AND (posts.in_response_to_post_id = '' OR posts.in_response_to_post_id IS NULL)
), 

-- Generate a series of indices, use LIMIT N to specify the length
seq_1_to_N AS (
  SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS i FROM raw_posts
  LIMIT 100
),
-- Explode the paragraphs-array
exploded_posts AS (
  SELECT
    rp.ID AS ID2
  , rp.first_published_at
  , rp.display_title
  , seq.i AS paragraph
  , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(paragraphs, seq.i::INT - 1), 'text') AS text
  FROM raw_posts AS rp, seq_1_to_N AS seq
  WHERE seq.i <= JSON_ARRAY_LENGTH(paragraphs)
),

-- Aggregate the text again
aggregated_posts AS (
  SELECT
    ep.ID2 AS ID3
  , ep.first_published_at
  , ep.display_title
  , LISTAGG(text, ' ') WITHIN GROUP (ORDER BY paragraph) AS text
  FROM exploded_posts AS ep
  WHERE text <> ''
  GROUP BY 1,2,3
),

-- Select features into a new table
text_features AS (
SELECT DISTINCT
  p3.post_id AS ID4
, post_features.ttr_received_total AS total_ttr
, post_features.recommends_received_total AS total_reccos
, post_features.responses_received_total AS total_responses
, post_features.drafting_time AS drafting_time
, LISTAGG(tag_post_relations.tag_slug, ',') WITHIN GROUP (ORDER BY tag_post_relations.tag_slug) OVER (PARTITION BY p3.post_id) AS tags
FROM posts AS p3
JOIN post_features ON  post_features.post_id = p3.post_id
JOIN tag_post_relations ON tag_post_relations.post_id = p3.post_id
  )

-- Aggregate features into final table
SELECT ft.ID4 AS post_ID, ft.first_published_at, ft.text, ft.total_ttr, ft.total_reccos, ft.total_responses, ft.drafting_time, ft.tags,
  CASE 
    WHEN (ft.tags LIKE '%politics%') THEN 'Politics'
    WHEN (ft.tags LIKE '%life%') THEN 'Life'
    WHEN (ft.tags LIKE '%tech%') THEN 'Tech'
    WHEN (ft.tags LIKE '%design%') THEN 'Design'
    WHEN (ft.tags LIKE '%education%') THEN 'Education'
  END AS Genre
FROM (aggregated_posts
JOIN text_features ON text_features.ID4 = aggregated_posts.ID3) AS ft

-- Randomly sample about %30 (75000)
-- SET seed to .25 for some reason, SET isn't recognized
WHERE first_published_at >= '2015-08-01 00:00:00'
ORDER BY random()
LIMIT 80000
"""
```


```python
# Occassionally throws "java.sql.SQLException: [Amazon](500310) Invalid operation: table 1846335 dropped by concurrent transaction;"
# Takes 5.8 min to run for 500 records, 10.6 min for 75000 records

dataraw = sqlContext.read.format("com.databricks.spark.redshift")\
  .option("url", redshiftUrl)\
  .option("tempdir", s3Temp)\
  .option("query", query)\
  .load()
```


```python
# Convert publish timestamp to date and write as managed table "genre_NLP_comparison". 

from pyspark.sql.functions import to_date  

data = dataraw.select('post_ID', to_date('first_published_at').alias('date'), 'text', 'total_ttr', 'total_reccos', 'total_responses', 'drafting_time', 'tags', 'genre')

data.write.saveAsTable("genre_NLP_comparison", mode='overwrite')
```


```python
# Check that sampling succeeded and the table contains all necessary features
all_data_raw = sqlContext.sql("SELECT * FROM genre_NLP_comparison")

print all_data_raw.show(5)
print all_data_raw.printSchema()
print all_data_raw.describe().show()
```


```python
all_data_raw.groupBy('genre').count().collect()
```

# Generate Sentiment Score Feature


```python
# Exploratory analysis showed a significant dip in sentiment from October 2015 to January 2016 across all genres, though Politics articles seemed to have dipped lower for some reason.  To compare Politics to other genres, we subset our sample twice to separate Politics. 

# Separate politics articles from rest of corpus
Oct_Jan_politics_dip_text = sqlContext.sql("SELECT date, post_ID, text from genre_NLP_comparison WHERE date BETWEEN '2015-10-19' AND '2016-01-01' AND genre = 'Politics'")

# Sample articles that are not politics but are from the same time period for comparison
Oct_Jan_dip_text = sqlContext.sql("SELECT date, genre, post_ID, text from genre_NLP_comparison WHERE date BETWEEN '2015-10-19' AND '2016-01-01' AND genre NOT IN ('Politics')")
```


```python
# Create user-defined function from VADER module to map compound sentiment score function to the distributed dataframe

import pyspark
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment

def total_sent(x):
  y = x.encode('utf-8')
  sentan = vaderSentiment(y)
  l = sentan['compound']
  return l

totsent = udf(lambda x: total_sent(x), pyspark.sql.types.FloatType())
```


```python
# Add 'Total_Sentmt' compound sentiment score as a new column to our dataframe and write this as a new managed table, 'snt_dip'.  This table contains a text sample that excludes politics articles. 

# Takes about 1 hour to run 
Oct_Jan_2015_Sent = Oct_Jan_dip_text.withColumn('Total_Sentmt', totsent(Oct_Jan_dip_text.text))
Oct_Jan_2015_Sent.write.saveAsTable("snt_dip", mode='overwrite')
```


```python
# Create a separate table for only politics articles during the dip period
snt_dip_p = Oct_Jan_dip_text.withColumn('Total_Sentmt', totsent(Oct_Jan_politics_dip_text.text))
snt_dip_p.write.saveAsTable('snt_dip_politics_only', mode='overwrite')

```


```python
# This table contains a text sample that includes all five genres across the full time period, August 1 2015 to May 1 2016. 

# Takes 1 hour 45 min to run
five_genre_sent = raw_dat.withColumn('Total_Sent', totsent(raw_dat.text))
five_genre_sent.write.saveAsTable("five_genre_sentiment", mode='overwrite')
```

# Visualize Results

## Initial Test Plot


```python
# Execute SQL to select all posts tagged 'politics', their full text, timestamp, total reading time (ttr), reccomends, responses and tags
query = """
 -- Select relevant posts, their text and reading features 
WITH raw_posts AS (
SELECT
  posts.post_id AS ID
, latest_versions.display_title
, TIMESTAMP 'epoch' + posts.first_published_at / 1000 * INTERVAL '1 Second' AS first_published_at
, JSON_EXTRACT_PATH_TEXT(latest_versions.content, 'bodyModel', 'paragraphs') AS paragraphs
FROM posts
-- Full inner join with users, latest_versions and post_features to
-- make sure we only consider posts that have all data that we need
JOIN users ON users.user_id = posts.creator
JOIN latest_versions ON latest_versions.post_id = posts.post_id
WHERE
  -- Filter that are actually published
  posts.first_published_at > 0
  AND (posts.visibility = 0 OR posts.visibility = '')
  AND posts.deleted_at = 0
  AND posts.blacklisted_at = 0
  AND users.blacklisted_at = 0
  AND latest_versions.is_published
  -- Filter posts that are in English
  AND posts.detected_language = 'en'
  -- Filter posts with tags related to 'politics'
  AND posts.post_id IN( 
    SELECT p2.post_id 
    FROM posts AS p2 
    JOIN tag_post_relations as tog ON p2.post_id = tog.post_id
    WHERE tog.tag_slug LIKE '%politics%'
    )
  -- Remove posts that are responses
  AND (posts.in_response_to_post_id = '' OR posts.in_response_to_post_id IS NULL)
), 
-- Generate a series of indices, use LIMIT N to specify the length
seq_1_to_N AS (
  SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS i FROM raw_posts
  LIMIT 100
),
-- Explode the paragraphs-array
exploded_posts AS (
  SELECT
    rp.ID AS ID2
  , rp.first_published_at
  , rp.display_title
  , seq.i AS paragraph
  , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(paragraphs, seq.i::INT - 1), 'text') AS text
  FROM raw_posts AS rp, seq_1_to_N AS seq
  WHERE seq.i <= JSON_ARRAY_LENGTH(paragraphs)
),
-- Aggregate the text again
aggregated_posts AS (
  SELECT
    ep.ID2 AS ID3
  , ep.first_published_at
  , ep.display_title
  , LISTAGG(text, ' ') WITHIN GROUP (ORDER BY paragraph) AS text
  FROM exploded_posts AS ep
  WHERE text <> ''
  GROUP BY 1,2,3
),
text_features AS (
SELECT DISTINCT
  p3.post_id AS ID4
, post_features.ttr_received_total AS total_ttr
, post_features.recommends_received_total AS total_reccos
, post_features.responses_received_total AS total_responses
, LISTAGG(tag_post_relations.tag_slug, ',') WITHIN GROUP (ORDER BY tag_post_relations.tag_slug) OVER (PARTITION BY p3.post_id) AS tags
FROM posts AS p3
JOIN post_features ON  post_features.post_id = p3.post_id
JOIN tag_post_relations ON tag_post_relations.post_id = p3.post_id
  )

SELECT ft.ID AS Post_ID, ft.first_published_at, ft.text, ft.total_ttr, ft.total_reccos, ft.total_responses
FROM (aggregated_posts
JOIN text_features ON text_features.ID4 = aggregated_posts.ID3) AS ft
ORDER BY first_published_at DESC
  , total_ttr DESC 
  , total_reccos DESC
  , total_responses DESC
"""
```


```python
sent_test = sqlContext.read.format("com.databricks.spark.redshift")\
  .option("url", redshiftUrl)\
  .option("tempdir", s3Temp)\
  .option("query", query)\
  .load()
  
sent_test.write.saveAsTable("sent_test", mode='overwrite')
```


```python
%r

# Initial plot from 'sent_test' table containing full sample of 27,000 articles

sntmnt_p_mavg <- collect(sql(sqlContext, "SELECT date, AVG(AVG(Total_Sentmt)) OVER (ORDER BY date ROWS 6 PRECEDING) AS avg_sntmnt FROM sent_test WHERE date >= '2015-08-01' GROUP BY date ORDER BY date ASC"))

library(ggplot2)

mn <- min(sntmnt_p_mavg$date) 
mx <- max(sntmnt_p_mavg$date)

p_initial <- ggplot(sntmnt_p_mavg, aes(date, avg_sntmnt)) + 
  scale_x_date(date_breaks="2 week", limits=c(mn, mx)) +
  geom_point(color="springgreen4") + 
  geom_smooth(span=.25) +
#   geom_hline(yintercept=0, color="blue") + 
#   geom_vline(aes(xintercept=as.numeric(sntmnt_p_mavg$date[105])), color="red", linetype="dashed") +
#   geom_text(aes(sntmnt_p_mavg$date[150],.75,label="November 13 Paris Attacks")) +
#   geom_vline(aes(xintercept=as.numeric(sntmnt$date[78])), color="red", linetype="dashed") +
#   geom_text(aes(sntmnt$date[142],.9,label="June 17 Charleston Shooting")) +
  labs(title="Weekly Moving Average, Articles Tagged 'Politics' Aug 1 2015 to April 1, 2016", x="Date", y="Sentiment") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

p_initial
```

## Final Analytic Plots


```python
%r
# Examine differences between sentiment scores across all articles, all genres over the full time period. 

fiveg <- collect(sql(sqlContext, "SELECT f.post_ID, date, Total_Sent, ln(total_ttr) AS log_ttr, genre, num_words FROM five_genre_sentiment AS f JOIN readability_scores AS r ON f.post_ID = r.post_ID"))
```


```python
%r
# Look at min/max articles for both negative and postive articles for comparison

fiveg$valence[fiveg$Total_Sent > 0] <- "pos"
fiveg$valence[fiveg$Total_Sent < 0] <- "neg"

fg_pos_articles <- fiveg[fiveg$valence == 'pos',]
fg_neg_articles <- fiveg[fiveg$valence == 'neg',]

fiveg$genre <- as.factor(fiveg$genre)
fiveg$valence <- as.factor(fiveg$valence)
```


```python
%r
fg_neg_articles <- fg_neg_articles[order(fg_neg_articles$Total_Sent),]
fg_neg_articles <- na.omit(fg_neg_articles)
head(fg_neg_articles)
```


```python
%r
# Negative articles have higher variance than positive articles. 
library(ggplot2)

both <- ggplot(fiveg, aes(x=date, y=Total_Sent, colour = genre)) +
  geom_smooth(se=FALSE) + 
  facet_grid(.~valence)

both
```


```python
%r 
fg_pos_articles <- fg_pos_articles[order(fg_pos_articles$Total_Sent),]
fg_pos_articles <- na.omit(fg_pos_articles)

head(fg_pos_articles)
```


```python
%r
library(ggplot2)

pos <- ggplot(fg_pos_articles, aes(x=date, y=Total_Sent, colour = genre)) +
  geom_smooth(se=FALSE) + 

pos
```


```python
%r
var(na.omit(fg_pos_articles$Total_Sent))
```


```python
%r
var(na.omit(fg_neg_articles$Total_Sent))
```


```python
%r
library(ggplot2)

neg <- ggplot(fg_neg_articles, aes(x=date, y=Total_Sent, colour = genre)) +
  stat_smooth(se=FALSE)

neg
```


```python
%r

#Articles excluding those tagged politics, published between October 2015 and January 2016

sntmnt_no_politics_mavg <- collect(sql(sqlContext, "SELECT genre, date, AVG(AVG(Total_Sentmt)) OVER (ORDER BY date ROWS 6 PRECEDING) AS avg_sntmnt FROM snt_dip GROUP BY date, genre ORDER BY date ASC"))

# sntmnt_no_politics_mavg$genre <- as.factor(sntmnt_no_politics_mavg$genre) 
```


```python
%r 

#All articles across all five genres published between Oct and Jan

sntmnt_all_mavg <- collect(sql(sqlContext, "SELECT genre, date, AVG(AVG(Total_Sent)) OVER (ORDER BY date ROWS 6 PRECEDING) AS avg_sntmnt FROM five_genre_sentiment WHERE date BETWEEN '2015-10-19' AND '2016-01-01' GROUP BY date, genre ORDER BY date ASC"))
```


```python
%r

#Only articles tagged politics published between Oct and Jan

sntmnt_p_mavg <- collect(sql(sqlContext, "SELECT date, AVG(AVG(Total_Sentmt)) OVER (ORDER BY date ROWS 6 PRECEDING) AS avg_sntmnt FROM snt_dip_politics_only GROUP BY date ORDER BY date ASC"))
```


```python
%r
library(ggplot2)

mn <- min(sntmnt_p_mavg$date) 
mx <- max(sntmnt_p_mavg$date)

p_only <- ggplot(sntmnt_p_mavg, aes(date, avg_sntmnt)) + 
  scale_x_date(date_breaks="2 week", limits=c(mn, mx)) +
  geom_point(color="springgreen4") + 
  geom_smooth(span=.25) +
#   geom_hline(yintercept=0, color="blue") + 
  geom_vline(aes(xintercept=as.numeric(sntmnt_p_mavg$date[105])), color="red", linetype="dashed") +
  geom_text(aes(sntmnt_p_mavg$date[150],.75,label="November 13 Paris Attacks")) +
#   geom_vline(aes(xintercept=as.numeric(sntmnt$date[78])), color="red", linetype="dashed") +
#   geom_text(aes(sntmnt$date[142],.9,label="June 17 Charleston Shooting")) +
  labs(title="Weekly Moving Average, Articles Tagged 'Politics'Oct 2015 to Jan, 2016", x="Date", y="Sentiment") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

```


```python
%r
library(ggplot2)

mn <- min(sntmnt_no_politics_mavg$date) 
mx <- max(sntmnt_no_politics_mavg$date)

no_politics <- ggplot(sntmnt_no_politics_mavg, aes(date, avg_sntmnt, color=genre)) + 
  scale_x_date(date_breaks="2 week", limits=c(mn, mx)) +
  geom_point() + 
  geom_smooth(span=.25, se=FALSE) +
#   geom_hline(yintercept=0, color="blue") + 
#   geom_vline(aes(xintercept=as.numeric(sntmnt_no_politics_mavg$date[105])), color="red", linetype="dashed") +
#   geom_text(aes(sntmnt_no_politics_mavg$date[150],.75,label="November 13 Paris Attacks")) +
#   geom_vline(aes(xintercept=as.numeric(sntmnt$date[78])), color="red", linetype="dashed") +
#   geom_text(aes(sntmnt$date[142],.9,label="June 17 Charleston Shooting")) +
  labs(title="Weekly Moving Average, Articles Excluding 'Politics' Oct 2015 to Jan 2016", x="Date", y="Sentiment") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))



```


```python
%r
library(ggplot2)
# Are we looking at a reaction to a discrete event, or is this merely seasonal depression? Or is this the cumulative effect of several bad things happening in a row? How to weight by TTR? 

mn <- min(sntmnt_all_mavg$date) 
mx <- max(sntmnt_all_mavg$date)

all_genres <- ggplot(sntmnt_all_mavg, aes(date, avg_sntmnt, color = genre)) + 
  scale_x_date(date_breaks="1 week", limits=c(mn, mx)) +
  geom_point() +  
  stat_smooth(span=.25, se=FALSE) +
#   geom_hline(yintercept=0, color="blue") + 
  geom_vline(aes(xintercept=as.numeric(sntmnt_all_mavg$date[126])), color="red", linetype="dashed") +
  geom_text(aes(sntmnt_all_mavg$date[210],.85, label="November 13 Paris Attacks"), color="black") +
#   geom_vline(aes(xintercept=as.numeric(sntmnt$date[78])), color="red", linetype="dashed") +
#   geom_text(aes(sntmnt$date[142],.9,label="June 17 Charleston Shooting")) +
  labs(title="Weekly Moving Average, All Genres, Oct 19 2015 to Jan 1, 2016", x="Date", y="Sentiment") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
```


```python
%r
all_genres
```


```python
%r
no_politics
```


```python
%r
p_only
```

## Most commonly occuring quadgrams in articles tagged "Politics," Oct 25, 2015 to Jan 1, 2016

|Articles with Positive (>0) Sentiment| Articles with Negative (<0) Sentiment|
|-------------------------------------|--------------------------------------|
|'hillary', 'clinton', 'bernie', 'sanders'| 'congressional', 'lung', 'cancer', 'caucus'|
|'family', 'politico', 'playbook', 'new' | 'conservative', 'americans', 'don't', 'need'|
|'new', 'jersey', 'playbook', 'massachusetts'|  'conservative', 'americans', 'don't', 'overthrow'|
|'presidential', 'candidate', 'donald', 'trump' |  'conservative', 'americans', 'need', 'overthrow' |
|'close', 'digital', 'divide', 'comcast' | 'prisoner', 'disenfranchisement', 'modern', 'racism' |
|'complete', 'muslims', 'entering', 'united' | 'lowest', 'pay', 'permitted', 'law' |
|'complete', 'shutdown', 'entering', 'united' | 'new', 'york', 'daily', 'news' | 
|'complete', 'shutdown', 'muslims', 'entering' | 'syrian', 'refugees', 'united', 'states'|
|'donald', 'trump', 'ben', 'carson' |  |
|'hillary', 'clinton', 'donald', 'trump' |  |

# Sentiment and TTR


```python
%r

#Select TTR and length from sentiment table joined with readability table on post_ID

snt <- collect(sql(sqlContext, "SELECT date, total_ttr, log(total_ttr) AS log_ttr, Total_Sent, num_words, genre AS Genre FROM five_genre_sentiment AS g JOIN readability_scores AS r ON g.post_ID = r.post_ID WHERE date < '2016-05-23'"))

tp_scale <- subset(snt, snt$total_ttr > 0)

tp_scale$Genre <- as.factor(tp_scale$Genre)

```


```python
%r
library(ggplot2)

stmt <- ggplot(tp_scale, aes(x = tp_scale$date, y = tp_scale$Total_Sent, color=Genre)) + 
  scale_y_continuous(breaks=seq(-1,1,.1)) +
  geom_point(alpha=.2) +
  labs(title="Sentiment by Volume, October 2015 to April 2016", x=" ", y="Normalized VADER Compound Sentiment Score") +
  facet_grid(.~Genre) + 
  geom_hline(yintercept=0, color="red", linetype="dashed") +
  theme(legend.position="none")

stmt
```


```python
%r
library(ggplot2)
genre_sent <- ggplot(tp_scale, aes(x = tp_scale$Total_Sent, y = tp_scale$log_ttr, color=Genre)) + 
#   geom_point(alpha=.2) +
  geom_smooth(se=FALSE)  + 
  scale_y_continuous(breaks=seq(11,15,.25)) +
  scale_x_continuous(breaks=seq(-1,1,.1)) +
  geom_vline(xintercept=0, color='red', linetype="dashed") +
  labs(title="Sentiment and Reading Time, October 2015 to April 2016", x="Normalized VADER Compound Sentiment Score", y="Total Time Read (TTR), Natural Log Scale") +
  facet_grid(Genre~.) + 
  theme(strip.text.y = element_blank())

genre_sent
```

## Model Sentiment's Effect on TTR


```python
%r
#Looks like this might be a cosine function 
ttr_sent_neg <- lm(fg_neg_articles$log_ttr~fg_neg_articles$Total_Sent+fg_neg_articles$num_words)
print(summary(ttr_sent_neg))

ttr_sent_pos <- lm(fg_pos_articles$log_ttr~fg_pos_articles$Total_Sent+fg_pos_articles$num_words)
print(summary(ttr_sent_pos))
```


```python
%r
#Need to control for num_words
sent_mod <- lm(log_ttr~Total_Sent+num_words, data=tp_scale)
summary(sent_mod)
```


```python
%r
s_mod <- lm(log_ttr~Total_Sent, data=tp_scale)
summary(s_mod)
```

# Text Cleaning and Tokenization


```python
# Select only post ID and text for cleaning and tokenization
raw_text = sqlContext.sql("SELECT post_ID, text FROM genre_NLP_comparison")
```


```python
general_stopwords = sc.textFile("/tmp/stopwords").collect()

mysqlStopwords = list("a?s, able, about, above, according, accordingly, across, actually, after, afterwards, again, against, ain?t, all, allow, allows, almost, alone, along, already, also, although, always, am, among, amongst, an, and, another, any, anybody, anyhow, anyone, anything, anyway, anyways, anywhere, apart, appear, appreciate, appropriate, are, aren?t, around, as, aside, ask, asking, associated, at, available, away, awfully, be, became, because, become, becomes, becoming, been, before, beforehand, behind, being, believe, below, beside, besides, best, better, between, beyond, both, brief, but, by, c?mon, c?s, came, can, can?t, cannot, cant, cause, causes, certain, certainly, changes, clearly, co, com, come, comes, concerning, consequently, consider, considering, contain, containing, contains, corresponding, could, couldn?t, course, currently, definitely, described, despite, did, didn?t, different, do, does, doesn?t, doing, don?t, done, down, downwards, during, each, edu, eg, eight, either, else, elsewhere, enough, entirely, especially, et, etc, even, ever, every, everybody, everyone, everything, everywhere, ex, exactly, example, except, far, few, fifth, first, five, followed, following, follows, for, former, formerly, forth, four, from, further, furthermore, get, gets, getting, given, gives, go, goes, going, gone, got, gotten, greetings, had, hadn?t, happens, hardly, has, hasn?t, have, haven?t, having, he, he?s, hello, help, hence, her, here, here?s, hereafter, hereby, herein, hereupon, hers, herself, hi, him, himself, his, hither, hopefully, how, howbeit, however, i?d, i?ll, i?m, i?ve, ie, if, ignored, immediate, in, inasmuch, inc, indeed, indicate, indicated, indicates, inner, insofar, instead, into, inward, is, isn?t, it, it?d, it?ll, it?s, its, itself, just, keep, keeps, kept, know, knows, known, last, lately, later, latter, latterly, least, less, lest, let, let?s, like, liked, likely, little, look, looking, looks, ltd, mainly, many, may, maybe, me, mean, meanwhile, merely, might, more, moreover, most, mostly, much, must, my, myself, name, namely, nd, near, nearly, necessary, need, needs, neither, never, nevertheless, new, next, nine, no, nobody, non, none, noone, nor, normally, not, nothing, novel, now, nowhere, obviously, of, off, often, oh, ok, okay, old, on, once, one, ones, only, onto, or, other, others, otherwise, ought, our, ours, ourselves, out, outside, over, overall, own, particular, particularly, per, perhaps, placed, please, plus, possible, presumably, probably, provides, que, quite, qv, rather, rd, re, really, reasonably, regarding, regardless, regards, relatively, respectively, right, said, same, saw, say, saying, says, second, secondly, see, seeing, seem, seemed, seeming, seems, seen, self, selves, sensible, sent, serious, seriously, seven, several, shall, she, should, shouldn?t, since, six, so, some, somebody, somehow, someone, something, sometime, sometimes, somewhat, somewhere, soon, sorry, specified, specify, specifying, still, sub, such, sup, sure, t?s, take, taken, tell, tends, th, than, thank, thanks, thanx, that, that?s, thats, the, their, theirs, them, themselves, then, thence, there, there?s, thereafter, thereby, therefore, therein, theres, thereupon, these, they, they?d, they?ll, they?re, they?ve, think, third, this, thorough, thoroughly, those, though, three, through, throughout, thru, thus, to, together, too, took, toward, towards, tried, tries, truly, try, trying, twice, two, un, under, unfortunately, unless, unlikely, until, unto, up, upon, us, use, used, useful, uses, using, usually, value, various, very, via, viz, vs, want, wants, was, wasn?t, way, we, we?d, we?ll, we?re, we?ve, welcome, well, went, were, weren?t, what, what?s, whatever, when, whence, whenever, where, where?s, whereafter, whereas, whereby, wherein, whereupon, wherever, whether, which, while, whither, who, who?s, whoever, whole, whom, whose, why, will, willing, wish, with, within, without, won?t, wonder, would, would, wouldn?t, yes, yet, you, you?d, you?ll, you?re, you?ve, your, yours, yourself, yourselves, zero".split(","))


```


```python
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import nltk
from nltk.corpus import stopwords
import string
from nltk.stem.porter import *

#Filter punctuation and stopwords, then word tokenize each sample to create new column 'word_features' for tf_idf/word2vec preprocessing for LDA topic modeling
nltk.download('stopwords')
stp = stopwords.words('english')
extra = ["?", "n", '?','\u2014', '\u2026', 'orignally', 'published'] + general_stopwords + mysqlStopwords
stp.extend(extra)

#Remove punctuation, word tokenize and optionally stem tokens 
def s_filter(x):
  nltk.download('punkt')
  translate_table = dict((ord(char), None) for char in string.punctuation)
  words = nltk.word_tokenize(x.translate(translate_table))
  clean_wrds = [i for i in words if not any(b.isdigit() for b in i)]
  
  a = []
  for i in clean_wrds:
    if i.lower() not in stp:
      a.append(i.lower())
  
  # stemmer = PorterStemmer()
#   s = [stemmer.stem(word) for word in a]
#   return s
  
  return a 
  
s_words = udf(lambda i: s_filter(i), pyspark.sql.types.ArrayType(StringType(), False))

```

# Write tokens to managed tables for analyses


```python
# Only articles tagged politics published between Oct 19, 2015 and Jan 1, 2016
politics_tokens = Oct_Jan_politics_dip_text.withColumn('tokens', s_words(Oct_Jan_politics_dip_text.text))
politics_tokens.write.saveAsTable("politics_tokens", mode='overwrite')
```


```python
#Articles published between Oct 19 and Jan 1, excluding politics
sent_dip_tokens = Oct_Jan_dip_text.withColumn('tokens', s_words(Oct_Jan_dip_text.text))
sent_dip_tokens.write.saveAsTable('snt_dip_tokens', mode='overwrite')
```


```python
#Articles from all 5 genres for full time period, Aug 2015 to May 2016
all_genre_tokens = raw_text.withColumn('tokens', s_words(raw_text.text))
all_genre_tokens.write.saveAsTable("five_genre_tokens", mode='overwrite')
```

## Compare negative and positive articles


```python
all_pos = sqlContext.sql("SELECT * from five_genre_sentiment WHERE Total_Sent > 0")
```


```python
all_neg = sqlContext.sql("SELECT * from five_genre_sentiment WHERE Total_Sent < 0")
```


```python
all_neg_tokens = all_neg.withColumn('tokens', s_words(all_neg.text))
all_neg_tokens.write.saveAsTable("all_neg_tokens", mode='overwrite')
```


```python
all_pos_tokens = all_pos.withColumn('tokens', s_words(all_pos.text))
all_pos_tokens.write.saveAsTable("all_pos_tokens", mode='overwrite')
```

## Tokens analyzed via LDA in separate notebook:
https://dbc-441a36cb-4656.cloud.databricks.com/#notebook/50333

# Investigate Part of Speech Tagging to find n-grams that may explain sentiment changes around topics and other named entities

## Politics articles posted between Oct 25, 2015 and April 1, 2016


```python
pos_data_raw = sqlContext.sql("SELECT f.post_ID, tokens, date FROM five_genre_tokens f JOIN genre_NLP_comparison g ON f.post_ID = g.post_ID WHERE date BETWEEN '2015-10-25' AND '2016-01-01' AND genre LIKE '%Politics%' ORDER BY date ASC")
```


```python
pos_data_raw.show(5)
```


```python
tagged_neg = sqlContext.sql("SELECT post_ID, tokens, date FROM all_neg_tokens WHERE date BETWEEN '2015-10-25' AND '2016-01-01' AND genre LIKE '%Politics%' ORDER BY date ASC")
```


```python
tagged_neg.count()
```


```python
tagged_pos = sqlContext.sql("SELECT post_ID, tokens, date FROM all_pos_tokens WHERE date BETWEEN '2015-10-25' AND '2016-01-01' AND genre LIKE '%Politics%' ORDER BY date ASC")
```


```python
tagged_pos.count()
```


```python
# nltk.download('averaged_perceptron_tagger')
tagger = nltk.PerceptronTagger()
# tagged =tagger.tag(test.tokens)
```


```python
tagged
```


```python
#Create separate column with PoS tagging to investigate NA or AN bigram frequencies 

import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField
import string
import nltk

def pos_tagger(x):
  nltk.download('averaged_perceptron_tagger')
  tagger = nltk.PerceptronTagger()
  s = tagger.tag(x)
  return s

tag_pos = udf(lambda i: pos_tagger(i), ArrayType(StructType([StructField("word", StringType(), False), StructField("pos", StringType(), False)])))
```


```python
pos_tags = pos_data_raw.select('post_ID', 'tokens')

pos_tagged_tokens_politics = pos_tags.withColumn('pos_tagged_tokens', tag_pos(pos_tags.tokens))
# politics_tokens.write.saveAsTable("politics_tokens", mode='overwrite')
```


```python
#Select words and POS tags from RDD
testu = pos_tagged_tokens_politics.select('pos_tagged_tokens')
POS = testu.flatMap(lambda i: i.pos_tagged_tokens)  

```


```python
# Collect POS tagged tokens into a list
POS_fin = POS.map(lambda i: (i.word, i.pos)).collect()
```


```python
POS_fin
```


```python
# Examine articles with negative sentiment
neg_tag_tokens = tagged_neg.withColumn('pos_tagged_tokens', tag_pos(tagged_neg.tokens))
Neg = neg_tag_tokens.select('pos_tagged_tokens')
Neggy = Neg.flatMap(lambda i: i.pos_tagged_tokens)
Neg_fin = Neggy.map(lambda i: (i.word, i.pos)).collect() 
```


```python
# Examine articles with positive sentiment

pos_tag_tokens = tagged_pos.withColumn('pos_tagged_tokens', tag_pos(tagged_pos.tokens))
posi = pos_tag_tokens.select('pos_tagged_tokens')
Posit = posi.flatMap(lambda i: i.pos_tagged_tokens)
Posi_fin = Posit.map(lambda i: (i.word, i.pos)).collect() 
```


```python
from nltk.metrics.association import QuadgramAssocMeasures

quadgram_measures = QuadgramAssocMeasures()

finder = QuadgramCollocationFinder.from_words(POS_fin, window_size=5)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally', 'violently', 'disagree', 'scroll']
ignored_words = nltk.corpus.stopwords.words('english') + extra
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

# finder only excludes patterns rather than selecting them. Need to use chunk module to find descriptive phrases about named entities http://www.nltk.org/api/nltk.chunk.html
# finder.apply_ngram_filter(lambda (w1,t1), (w2,t2): t2.startswith('NN') and t1.startswith('JJ'))

finder.apply_freq_filter(10)

print finder.nbest(quadgram_measures.pmi, 10) 
print finder.nbest(quadgram_measures.likelihood_ratio, 10) 
print finder.nbest(quadgram_measures.raw_freq, 10) 
print finder.nbest(quadgram_measures.chi_sq, 10) 
```


```python
quadgram_measures = QuadgramAssocMeasures()

finder = QuadgramCollocationFinder.from_words(Posi_fin, window_size=5)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally', 'violently', 'disagree', 'scroll']
ignored_words = nltk.corpus.stopwords.words('english') + extra
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

finder.apply_freq_filter(5)

# print finder.nbest(quadgram_measures.pmi, 10) 
print finder.nbest(quadgram_measures.likelihood_ratio, 10) 
print finder.nbest(quadgram_measures.raw_freq, 10) 
# print finder.nbest(quadgram_measures.chi_sq, 10)

b = []

for ((w1,t1), (w2,t2), (w3,t3), (w4,t4)) in finder.nbest(quadgram_measures.raw_freq, 100): 
  if (t2.startswith('NN') or t3.startswith('NN')) and (t1.startswith('JJ') or t4.startswith('JJ')): 
      b.append((w1, w2, w3, w4)) 
print b
```


```python
quadgram_measures = QuadgramAssocMeasures()

finder = QuadgramCollocationFinder.from_words(Neg_fin, window_size=5)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally', 'violently', 'disagree', 'scroll']
ignored_words = nltk.corpus.stopwords.words('english') + extra
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

finder.apply_freq_filter(5)

# print finder.nbest(quadgram_measures.pmi, 10) 
print finder.nbest(quadgram_measures.likelihood_ratio, 10) 
print finder.nbest(quadgram_measures.raw_freq, 10) 
# print finder.nbest(quadgram_measures.chi_sq, 10)

b = []

for ((w1,t1), (w2,t2), (w3,t3), (w4,t4)) in finder.nbest(quadgram_measures.raw_freq, 100): 
  if (t2.startswith('NN') or t3.startswith('NN')) and (t1.startswith('JJ') or t4.startswith('JJ')): 
      b.append((w1, w2, w3, w4)) 
print b
```


```python
from nltk.collocations import *

# bb = process(POS_fin)

bigram_measures = nltk.collocations.BigramAssocMeasures()

finder = BigramCollocationFinder.from_words(POS_fin)

ignored_words = nltk.corpus.stopwords.words('english')
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

# finder.apply_ngram_filter(lambda (w1,t1), (w2,t2): t2.startswith('NN') and t1.startswith('JJ'))

finder.apply_freq_filter(10)

print finder.nbest(bigram_measures.pmi, 10) 
print finder.nbest(bigram_measures.likelihood_ratio, 10) 
print finder.nbest(bigram_measures.raw_freq, 10) 
print finder.nbest(bigram_measures.chi_sq, 10) 
```


```python
from nltk.collocations import *

# bb = process(POS_fin)

trigram_measures = nltk.collocations.TrigramAssocMeasures()

finder = TrigramCollocationFinder.from_words(POS_fin, window_size=4)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally']
ignored_words = nltk.corpus.stopwords.words('english') + extra
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

finder.apply_ngram_filter(lambda (w1,t1), (w2,t2), (w3,t3): t2.startswith('NN') and (t1.startswith('JJ') or t3.startswith('JJ')))

finder.apply_freq_filter(10)

# print finder.nbest(trigram_measures.pmi, 10) 
print finder.nbest(trigram_measures.likelihood_ratio, 10) 
print finder.nbest(trigram_measures.raw_freq, 10) 
# print finder.nbest(trigram_measures.chi_sq, 10) 

# Appears that the Presidential election and the frontrunner democratic candidates (Hilary, Bernie) dominated discussion, as well as Trump's proposed ban on Muslims entering the U.S., though no specific mention of Trump. Unclear what "traditional public schools" might refer to. 
```


```python
# About 600 articles tagged "Politics" with negative sentiment published between Oct 25, 2015 and Jan 1, 2016
trigram_measures = nltk.collocations.TrigramAssocMeasures()

finder = TrigramCollocationFinder.from_words(Neg_fin, window_size=4)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally']
ignored_words = nltk.corpus.stopwords.words('english') + extra
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

# finder.apply_ngram_filter(lambda (w1,t1), (w2,t2), (w3,t3): t2.startswith('NN') and (t1.startswith('JJ') or t3.startswith('JJ')))

finder.apply_freq_filter(10)

print finder.nbest(trigram_measures.likelihood_ratio, 10) 
print finder.nbest(trigram_measures.raw_freq, 10) 
```


```python
#About 1300 articles tagged "Politics" with positive sentiment
trigram_measures = nltk.collocations.TrigramAssocMeasures()

finder = TrigramCollocationFinder.from_words(Posi_fin, window_size=4)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally']
ignored_words = nltk.corpus.stopwords.words('english') + extra
finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

# finder.apply_ngram_filter(lambda (w1,t1), (w2,t2), (w3,t3): t2.startswith('NN') and (t1.startswith('JJ') or t3.startswith('JJ')))

finder.apply_freq_filter(10)

print finder.nbest(trigram_measures.likelihood_ratio, 10) 
print finder.nbest(trigram_measures.raw_freq, 10) 
```


```python
#filtering for a specific noun like 'clinton' doesn't seem to work for some reason. 

trigram_measures = nltk.collocations.TrigramAssocMeasures()

finder = TrigramCollocationFinder.from_words(POS_fin, window_size=4)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally']
ignored_words = nltk.corpus.stopwords.words('english') + extra

finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

finder.apply_freq_filter(5)

# finder.apply_ngram_filter(lambda (w1,t1), (w2,t2), (w3,t3): (w2 == 'clinton') and (t1.startswith('JJ') or t3.startswith('JJ')))
 
print finder.nbest(trigram_measures.likelihood_ratio, 10) 
print finder.nbest(trigram_measures.raw_freq, 10) 

b = []

for ((w1,t1), (w2,t2), (w3,t3)) in finder.nbest(trigram_measures.raw_freq, 100): 
  if t2.startswith('NN') and (t1.startswith('JJ') or t3.startswith('JJ')): 
      b.append((w1, w2, w3)) 
print b
```


```python
trigram_measures = nltk.collocations.TrigramAssocMeasures()

finder = TrigramCollocationFinder.from_words(POS_fin, window_size=4)

extra = ['like', 'comment', 'follow', 'allen', 'mikeallen', 'mallenpoliticocom', 'daniel', 'lippman', 'dlippman', 'dlippmanpoliticocom', 'settings', 'httpwwwpoliticocomregistration', 'twitter', 'medium', 'semipartisansamcom', 'originally']
ignored_words = nltk.corpus.stopwords.words('english') + extra

finder.apply_word_filter(lambda (w,t): len(w) < 3 or w.lower() in ignored_words)

finder.apply_freq_filter(5)

b = []

for ((w1,t1), (w2,t2), (w3,t3)) in finder.nbest(trigram_measures.raw_freq, 100): 
  if t2.startswith('NN') and (t1.startswith('JJ') or t3.startswith('JJ')): 
      b.append((w1, w2, w3)) 
print b
```


```python
def process(sentence):
  b = []
  for (w1,t1), (w2,t2) in nltk.bigrams(sentence): 
    if (t1.startswith('JJ') and t2.startswith('NN')): 
      b.append((w1, w2)) 
  return b
```


```python
process(POS_fin)
```


```python
def trump_process(sentence):
  for (w1,t1), (w2,t2), (w3,t3) in nltk.trigrams(sentence): # [_three-word]
    if (t2 == 'trump') and (t1.startswith('JJ') or (t3.startswith('JJ'))): 
        print(w1, w2, w3) # [_print-words]
```


```python
trump_process(POS_fin)
```

# Examine n-Gram Frequencies to Sense Check LDA


```python
from nltk.collocations import *

#Examine associted term frequency during 'dip' sentiment period, 17940 total articles across 5 genres 
tf_data = sqlContext.table('snt_dip_tokens')l
tokens = tf_data.select('tokens').rdd.map(lambda j: j.tokens).collect()
```


```python
all_tokens = []
for item in tokens:
  all_tokens.extend(item)
```


```python
import nltk
# How to set "adjective noun" pattern? Need to do this from the same set of tokens as LDA. 
finder = BigramCollocationFinder.from_words(all_tokens)#, window_size=4)

bigram_measures = nltk.collocations.BigramAssocMeasures()
scored = finder.score_ngrams(bigram_measures.raw_freq)
```


```python
sorted(finder.nbest(bigram_measures.likelihood_ratio, 10))
```


```python
import nltk
#Create your bigrams
bgs = nltk.bigrams(nonum_tokens)

#compute frequency distribution for all the bigrams in the text
fdist = nltk.FreqDist(bgs)

for k,v in fdist.items():
    print k,v
```


```python
sorted(finder.nbest(bigram_measures.raw_freq, 10))
```


```python
sorted(finder.nbest(bigram_measures.pmi, 10))
```


```python
sorted(finder.nbest(bigram_measures.chi_sq, 10))
```


```python
sorted(finder.nbest(bigram_measures.student_t, 10))
```


```python
sorted(finder.ngram_fd.items(), key=lambda t: (-t[1], t[0]))[:10]
```


```python
trifinder = TrigramCollocationFinder.from_words(nonum_tokens)
sorted(trifinder.ngram_fd.items(), key=lambda t: (-t[1], t[0]))[:10]
```


```python
trigram_measures = nltk.collocations.TrigramAssocMeasures()
sorted(trifinder.nbest(trigram_measures.likelihood_ratio, 10))
```


```python
fdist.pformat(maxlen=100)
```


```python
from nltk.probability import *

fdist = FreqDist(w.lower() for w in nonum_tokens)
```


```python
fdist['paris']
```


```python
fdist['trump']
```


```python
fdist['clinton']
```


```python
fdist.N()
```


```python
fdist.B()
```


```python
fdist.freq('paris')
```


```python
fdist.freq('trump')
```


```python
fdist['terrorist']
```


```python
fdist['attack']
```


```python
fdist['shooting']
```


```python
finder.ngram_fd
```


```python
finder
```

# Unfinished Python LDA


```python
ag_df = sqlContext.sql("SELECT *, cast(ROW_NUMBER() OVER () AS BIGINT) AS id FROM five_genre_tokens")

#  Need to preprocess other dataset, since politics_tokens only has 2121 records in the subsample
# p_df = sqlContext.sql("SELECT *, cast(ROW_NUMBER() OVER () AS BIGINT) AS id FROM politics_tokens")
```


```python
from pyspark.ml.feature import CountVectorizer

ag_vectorizer = CountVectorizer(inputCol="tokens", outputCol="features").fit(ag_df)
all_genre_lda = ag_vectorizer.transform(ag_df)

# p_vectorizer = CountVectorizer(inputCol="tokens", outputCol="features").fit(p_df)
# politics_lda = p_vectorizer.transform(p_df) 

```


```python
ag_lda_input = all_genre_lda.select("id", "features").map(lambda x: [x[1], x[0]])
# p_lda_input = politics_lda.select("id", "features").map(lambda x: [x[1], x[0]])
```


```python
from pyspark.mllib.clustering import LDA, LDAModel

model = LDA.train(ag_lda_input, k=5)
```


```python
# # Output topics. Each is a distribution over words (matching word count vectors)
print("Learned topics (as distributions over vocab of " + str(model.vocabSize()) + " words):")
topics = model.topicsMatrix()
for topic in range(5):
    print("Topic " + str(topic) + ":")
    for word in range(0, model.vocabSize()):
        print(" " + str(topics[word][topic]))
```


```python
input = p_df.select("tokens").rdd.map(list)
model = Word2Vec().setMinCount(1000).setNumPartitions(20).fit(input)
```


```python
input.first()
```


```python
synonyms = model.findSynonyms("paris", 40)
for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))
```


```python
ag_lda_input.first()
```


```python
from pyspark.mllib.clustering import LDA, LDAModel

# Topic modeling with LDA
all_genres_model = LDA.train(ag_lda_input, k=5)
politics_model = LDA.train(p_lda_input, k=5)
```


```python
import nltk
import pyspark
from pyspark.sql.functions import udf
from nltk.data import load
from nltk.corpus import stopwords
# from nltk.sentiment.vader import SentimentIntensityAnalyzer
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment
from nltk.tokenize.treebank import TreebankWordTokenizer
from textstat.textstat import textstat
import string

datums = sqlContext.table("genre_NLP_comparison")

# Needed to install punkt tokenizer, as it wasn't available immediately from installing nltk. Upon cluster reboot or notebook re-attachment, command will need to be re-run
# nltk.download('punkt')
# nltk.download("stopwords")
tokenizer = load('tokenizers/punkt/english.pickle')

#May also be having an issue with NLTK https://github.com/nltk/nltk/issues/1228, so trying vaderSentiment library from PyPi rather than NLTK version
# nltk.download('vader_lexicon')
# nltk.download('vader_lexicon.txt')
# nltk.data.load('vader_sentiment_lexicon.txt')

# Define function to count number of sentences per article and return as integer
def gustavstokenizer(x):
    t = tokenizer.tokenize(x)
    return len(t) 

nsent = udf(lambda i: gustavstokenizer(i), pyspark.sql.types.IntegerType())

df = datums.withColumn('num_sent', nsent(data.text))

# Define function to count words per article and return as integer
_treebank_word_tokenize = TreebankWordTokenizer().tokenize

def numwords(x):
  z = [token for sent in tokenizer.tokenize(x)
            for token in _treebank_word_tokenize(sent)]
  return len(z)

nwords = udf(lambda i: numwords(i), pyspark.sql.types.IntegerType())

dftwo = df.withColumn('num_words', nwords(data.text))

# Filter text with fewer than 20 words in order to avoid articles with "0" sentences. Readability and Sentiment require dividing by sentence count, so this value must be >0. 
filtered = dftwo.filter(dftwo.num_sent > 1)

# Define function to calculate New Dale-Chall readability score as return as a Double
def readingL(j):
  l = textstat.dale_chall_readability_score(j)
  return l

readL = udf(lambda x: readingL(x), pyspark.sql.types.DoubleType())

dfthree = filtered.withColumn('Readability', readL(filtered.text))

# Text is in ASCII, needs to be Unicode - This may be an error in Vader's source code that uses str() http://stackoverflow.com/questions/9942594/unicodeencodeerror-ascii-codec-cant-encode-character-u-xa0-in-position-20. To correct this, vader was attached directly as a library rather than using the version integrated with NLTK. 

# Calculate compound sentiment scores 
def total_sent(x):
  y = x.encode('utf-8')
  sentan = vaderSentiment(y)
  l = sentan['compound']
  return l

totsent = udf(lambda x: total_sent(x), pyspark.sql.types.FloatType())

dffour = dfthree.withColumn('Total_Sentmt', totsent(data.text))

# Calculate negative sentiment scores
def neg_sent(x):
    y = x.encode('utf-8')
    sentan = vaderSentiment(y)
    l = sentan['neg']
    return l

negsent = udf(lambda x: neg_sent(x), pyspark.sql.types.FloatType())

dffive = dffour.withColumn('Neg_Sentmt', negsent(data.text))

# Calculate neutral sentiment
def neu_sent(x):
    y = x.encode('utf-8')
    sentan = vaderSentiment(y)
    l = sentan['neu']
    return l

neusent = udf(lambda x: neu_sent(x), pyspark.sql.types.FloatType())

dfsix = dffive.withColumn('Neu_Sentmt', neusent(data.text))

# Calculate positive sentiment 
def pos_sent(x):
    y = x.encode('utf-8')
    sentan = vaderSentiment(y)
    l = sentan['pos']
    return l

possent = udf(lambda x: pos_sent(x), pyspark.sql.types.FloatType())

dfsev = dfsix.withColumn('Pos_Sentmt', possent(data.text))    

#Filter punctuation and stopwords, then word tokenize each sample to create new column 'word_features' for tf_idf/word2vec preprocessing for LDA topic modeling
stp = set(stopwords.words('english'))

def s_filter(x):
  nltk.download('punkt')
  translate_table = dict((ord(char), None) for char in string.punctuation)
  words = nltk.word_tokenize(x.translate(translate_table))  
  a = []
  for i in words:
    if i.lower() not in stp:
      a.append(i)
      continue 
  return a

s_words = udf(lambda i: s_filter(i), pyspark.sql.types.ArrayType(StringType(), False))

dfate = dfsev.withColumn('word_features', s_words(data.text))
```


```python
# reformat dates into MM/dd/yyyy for plotting. 
from pyspark.sql.functions import to_date

sntmt_plot = dfate.select('post_ID', to_date('first_published_at').alias('date'), 'num_words', 'num_sent', 'Readability', 'Total_Sentmt', 'Neg_Sentmt', 'Neu_Sentmt', 'Pos_Sentmt', 'total_ttr', 'total_reccos', 'total_responses', 'drafting_time', 'tags', 'genre')
```


```python
from pyspark.sql import functions as F

#Separate word features from other data cols.  Change sentiment to binary classification label. 0 = negative, 1 = positive
feats = dfate.select(F.when(dfate.Total_Sentmt > 0, 1).when(dfate.Total_Sentmt < 0, 0).alias('Snt_Valence'), 'word_features')

```


```python
# 500 records took about 17.5 min to write
feats.write.saveAsTable("word_features", mode='overwrite')
```


```python
# 500 records requires 20 minutes to write. 10,000 records requires more than 3 hours. Got tired of waiting and canceled the job. 
sntmt_plot.write.saveAsTable("sentiment_analysis", mode='overwrite')
```


```python
type(sntmt_plot)
```


```python
snt2 = sqlContext.table("sentiment_analysis")
snt2.show(5)
```


```python
# Attempt to write as managed table, throwing an error about write permissions https://github.com/Stratio/Sparta/issues/413, http://stackoverflow.com/questions/31104125/apache-pyspark-lost-executor-failed-to-create-local-dir. "Caused by: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 86300.0 failed 4 times, most recent failure: Lost task 0.3 in stage 86300.0 (TID 55233, ip-10-50-244-34.us-west-2.compute.internal): java.io.IOException: Failed to create local dir in /local_disk0/spark-01ae408b-df5b-4e86-a803-5691d94cfca2/executor-c1760a67-e2e6-450b-80a1-3dfce0fd7eec/blockmgr-c5ff5729-e7f7-4972-9a22-e79719d63372/3b."

sntmt_plot.write.saveAsTable("sentiment_analysis", mode='overwrite')
```


```python
snt = sqlContext.table("sentiment_analysis")
snt.show(5)
```


```python
ft = sqlContext.table("word_features")
ft.show(5)
print ft.dtypes()
```


```python
print ft.describe()
print type(ft)
ft.printSchema()
```


```python
#Pre-process with TF_IDF or Word2vec on the 'word_features' column
#Word2vec
from pyspark.mllib.feature import Word2Vec

features = sqlContext.table("word_features")
           
ft = features.rdd.map(list)

# select('word_features').

# fg = ft.rdd.map(list)
wtv_model = Word2Vec().setMinCount(1).setNumPartitions(5).fit(ft)
# word2vec = Word2Vec()
# model = word2vec.fit(ft)
# model = Word2Vec().setVectorSize(10).setSeed(42).fit(ft)

wrd_vecs = wtv_model.getVectors()
```


```python
type(wrd_vecs)
wrd_vecs
```


```python
synonyms = wtv_model.findSynonyms('crowd', 1)

for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))
```


```python
print type(ft)
print ft.first()
```


```python
#TF_IDF
from pyspark import SparkContext
# from pyspark.mllib.feature import HashingTF
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import *

# from pyspark.ml.feature import StringIndexer

# sentenceData = sqlContext.createDataFrame([
#   (0, "Hi I heard about Spark"),
#   (0, "I wish Java could use case classes"),
#   (1, "Logistic regression models are neat")
# ], ["label", "sentence"])
# tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
# wordsData = tokenizer.transform(sentenceData)
# hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
# featurizedData = hashingTF.transform(wordsData)


# fields = [StructField('Snt_Valence', IntegerType(), False), StructField('word_features', ArrayType(StringType()), False)]
# schema = StructType(fields)
# values = sqlContext.table("word_features")

# features = sqlContext.createDataFrame(values, schema)

features = sqlContext.table("word_features")

vectorizer = CountVectorizer(inputCol="word_features", outputCol="vec_features").fit(features)
# vectorizer.vocabulary

lda = vectorizer.transform(features)
fe = lda.select("word_features", "vec_features").rdd
lda_feats = fe.map(lambda row: row.asDict())

# .select("vec_features").rdd

# Have not been able to get SparkMLib's Hashing_TF to produce usable output. 
# ft = features.rdd.map(list)
# hashingTF = HashingTF()

# tf = hashingTF.transform(features)

#  key, value structure of
# document_id, [token_ids]

# The second is an inverted index like
# token_id, [document_ids]
# I'll call those corpus and inv_index respectively.

# from collections import Counter
# def wc_per_row(row):
#     cnt = Counter()
#     for word in row:
#         cnt[word] += 1
#     return cnt.items() 

# tf = features.map(lambda (x, y): (x, wc_per_row(y)))

# ... continue from the previous example
# idf = IDF()
# idfmodel = idf.fit(tf)

# tfidf = idfmodel.transform(tf)

# indexer = StringIndexer(inputCol='post_ID',outputCol='KeyIndex')
# indexed_data = indexer.fit(tfidf).transform(tfidf)

```


```python
type(lda_feats)
lda_feats.first()
```


```python
# Topic modeling with LDA
from pyspark.mllib.clustering import LDA, LDAModel
# from pyspark.mllib.linalg import Vectors

# Parse data by line to convert to vectors
# parsedData = tf.map(lambda line: Vectors.dense([float(x) for x in line]))

# Index documents with unique IDs
# corpus = parsedData.zipWithIndex().map(lambda x: [x[1], x[0]]).cache()

# Cluster the documents into three topics using LDA
# rdd = sc.parallelize(tf)
# k = 5 # number of clusters
model = LDA.train(lda_feats, k=5)

# # Output topics. Each is a distribution over words (matching word count vectors)
# print("Learned topics (as distributions over vocab of " + str(ldaModel.vocabSize()) + " words):")
# topics = ldaModel.topicsMatrix()
# for topic in range(5):
#     print("Topic " + str(topic) + ":")
#     for word in range(0, ldaModel.vocabSize()):
#         print(" " + str(topics[word][topic]))
		
# Save and load model
# model.save(sc, "myModelPath")
# sameModel = LDAModel.load(sc, "myModelPath")
```
