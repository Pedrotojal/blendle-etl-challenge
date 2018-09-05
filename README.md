# blendle-etl-challenge
Getting hired by Blendle

1. [The Challenge Explained](The-Challenge.md)

2. About the data 

The original events.json was not prepared to be read, I had to tweak it. Did it with this:
```bash
sed '1s/^/[/;$!s/$/,/;$s/$/]/' events.json > events2.json
```

3. Decisions

3.1. Not Using Spark. Using Petl
- After wasting a lot of time trying to put Spark working on aws, I've decided to pick an etl library that was simple enough to solve this challenge;
- Petl was the one choosen, which is great for some manipulations but quite limited in terms of final integration;
- Bonobo would probably be a better option (or spark!).

3.2. Loading the time dimension
- It is of my opinion that the time dimension should be loaded with a procedure that can load specific intervals of data (like: 1 year from now). This wasn't coded;
- I didn't see any value in creating this from scratch, when it's a pretty well known "problem" with many available solutions;
- Also, before going further with suggestions, time dimension is something that has a huge impact on the analysis and it should be properly defined all the possible time analysis that we may want to do;
- I went with the most basic, also because of the datetime field we have;
- More complex analysis were though but not developed (ytd, mtd, qtd, this month).

3.3. Utm_campaign
- Thinking about campaigns, it came to my mind that this will become a slowly changing dimension, but not with this data. I've called it what we have here the campaign type.

3.4. Campaign
- While we don't have the campaigns defined with start and end date, I've hardcoded this to be 'none' from and to a specific date, so that by the time we start defining campaigns we can exclude from analysis the 'nones';
- Having this data would allows us to do analysis during the lifetime of a campaign.

3.5. Facts
- Code here could be way better and more simple, but with this library I've focused on achieving what was my goal and not so much in keeping it simple. Pretty sure there is a simple way of doing of of these transformations with Petl;
- Facts table is in fact a staging table for pre integration, everything we need to connect with the dimensions and get the id's is here.

3.6 . First load vs everyday load
- This was an hard one, no database structure, nothing loaded, trying to think of this as a daily load while working with the files as if it was a first load.
- I'm pretty sure that if I look into the code with some more time and attention I'll probably identify many steps were this process will not fit a daily load.
- So yes, this is more a first load without the part were you get the ids and create the relationships.

4. Code Review
4.1. What I could have done better

4.1.1. Config file
- Adding a config file with the paths and file names

4.1.2. Unit tests
- So, this wasn't for sure a Tdd, it was a let's get this outputing some files;
- Since I wanted to get this documentation done, I've skipped the tests

4.1.3. Log manager
- The try excepts used are quite trivial (maybe even useless if we're debugging an error), here I went with this just to get it working;
- A proper log manager should be used to log all exceptions.

4.1.4. DimensionFactory
- The dimensions have pretty much the same structure, so it would be way better to have a Dimension class, and a factory to create dimensions .

4.1.5. File workflow
- As we can see, files are consumed, not tagged, not moved to a processed file;
- Decision was to get this working, didn't focus on this;




