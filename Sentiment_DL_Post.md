# Words with Feeling
Computational linguistics often depend on distributional semantics, the concept that texts with similar word counts (frequency distributions) are alike, an extension of [Leibniz's Law](https://en.wikipedia.org/wiki/Identity_of_indiscernibles) to language.  Informally, this is often referred to as the ["bag of words" model](https://en.wikipedia.org/wiki/Bag-of-words_model). While computers are still far away from comprehending human language, [much less human feelings](https://www.youtube.com/watch?v=kthHrC88K7c), distributional semantics allows us to compare documents and infer topics from words and word combinations that occur often within a document corpus. 

Sentiment analysis using distributional semantics (as opposed to a supervised machine learning method using a human labeled training set) simply counts "good" and "bad" words and infers the author's sentiment from the overall counts. Before computers became powerful enough to process large text corpora, literary theorists used a similar approach to interpret authors' tone - the feeling that the author wants to evoke in their readers.  Since sentiment analysis of texts can only tell us about the kind of words used and in what quantities, it's a leap in logic to say that these can determine how an author feels. However, it's logical to assume that authors will select descriptive words that reflect their opinions, which, to some extent, are influenced by their feelings. 

Despite this early technology's limitations, much has been made about using sentiment to predict evertyhing from riots to suicide to stock market shifts: 
	
* ["Social Media Sentiment and Consumer Confidence"](https://www.ecb.europa.eu/events/pdf/conferences/140407/Daas_Puts_Sociale_media_cons_conf_Stat_Neth.pdf)
* ["Predicting National Suicide Numbers with Social Media Data"](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0061809)
*["Tents, Tweets, and Events: The Interplay Between Ongoing Protests and Social Media"](http://onlinelibrary.wiley.com/doi/10.1111/jcom.12145/abstract)

So what can sentiment tell us about content posted on Medium? 

# Methodology
This analysis sampled 74,337 articles from Medium across 5 topics (Design, Technology, Education, Politics and Life) published between August 2015 and May 2016. This datasets sentiment scores were calculated using C.J. Hutto and Eric Gilbert's [VADER (Valence Aware Dictionary and sEntiment Reasoner) module](https://github.com/cjhutto/vaderSentiment), since it appeared to outperform other tools currently available and was designed and tested specifically for online social media. 
>7,500 lexical features with validated valence scores that indicated both the sentiment polarity (positive/negative), and the sentiment intensity on a scale from –4 to +4. For example, the word "okay" has a positive valence of 0.9, "good" is 1.9, and "great" is 3.1, whereas "horrible" is –2.5, the frowning emoticon :( is –2.2, and "sucks" and it's slang derivative "sux" are both –1.5.


Vader package - using compound scores that are normalized between -1 and 1, so the documents overall valence is exaggerated.  

Examples of positive and negative articles from each genre.  Big difference between a 'pure' 1 and something fractional. 


##Positive
* Life - compound .3182
https://medium.com/@cSuiteApp/achieving-work-life-balance-is-not-a-myth-8120e5b010c0#.7oaz1n5fd
Fairly business like, short 
* Design - compound .991
https://medium.com/@i3dvisual/rendering-is-the-impeccable-part-of-architecture-industry-caabe206a0df#.mtcyid32e
Informational article extolling the benefits of 3D rendering.
* Education - compound .9992
https://medium.com/open-edtech/our-education-turns-children-into-zombies-82cbe8e7e3bf#.su8st75jk
Critical, but the words used are positive. Except perhaps Zombies, but this isn't enough to bring down the score. 
* Life - compound 1
https://medium.com/@melissajoykong/don-t-wait-for-friday-a009fcac5c0a#.nvvoc95dj
Tips for relationships - very inspiring
* Tech - compound .0003
https://medium.com/@kalkineau/five-speculative-stocks-to-buy-in-february-2016-6c8c4a36be7f#.9jhwqxb6g
Almost neutral press release for financial results 

##Negative
* Politics - compound -.9884 
https://medium.com/@unbuttonmyeyes/fear-ana-mendieta-a728dfa939f3#.8kmc5z2q1
A pointed feminist critique of an article claiming females in the US are socialized to more afraid than males. 
* Life - compound -.68
https://medium.com/the-coffeelicious/welcome-to-this-world-37fdd282f5cf#.98rw0msif
A brief postcard about a "scared. Terrifed" puppy abandoned at a train station in India. Why is this less negative than the above? Perhaps length? It's shorter?
* Politics - compound -.9564
https://medium.com/@SloshednSeattle/and-your-mother-wears-army-boots-ee6da6959803#.hpbttxqhm
Actually satirical, though the sentiment alorithm can't tell the difference. 
* Life compound -1
https://medium.com/@PojacRaindrop/the-death-penalty-is-murder-c3806cdb01fc#.7u4xccsfc
Long, emotional argument against the death penalty 
* Design compound -.0072
https://medium.com/@lancepriebe/want-to-learn-a-little-secret-about-club-penguin-f3fe53b2846d#.ggsgmbfp7
Ambiguous. He is defensive about not creating kids games

# Results
Given the 2016 environment, we examined about 27,000 posts tagged "politics" published between August 2015 and April 2016.  Earlier data wasn't available in sufficient quantities, though political content has increased rapidly since early 2016. [Include chart?], now approaching 2-300 new articles per day. Between October 19, 2015 and January 1, sentiment dips.  

Many more 'positive' articles than negative. 

It's a stretch - 'trump' appears as a significant topic overall, though not when we separate positive articles from negative articles. Only the negative articles have trump as a topic. 



# Conclusion
Are people responding to a common issue? The data says no, yet we see a seasonal decline in sentiment to various degrees across different topic genres.  The most likely explanation is that writers were more negative during the winter, wihout necessarily reacting to a common event or topic. That 