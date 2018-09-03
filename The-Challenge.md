# Data Engineering Challenge

We want you to build a small ETL job, that can run as a cron job (or a Spark job). As input data, we have a CSV file with user info, and a JSON Lines file with events. The output should be written to a SQL database (but feel free to write to CSV files if you run out of time). Running the job with updated input data should update an existing database.

The data warehouse should provide answers to the following questions:
  - How long do customers keep their subscription?
  - How many non-trial customers do we have now?
  - How many customers convert from trial to paid each month?
  - How many customers churn (i.e. cancel their subscription) each month?
  - Each of the above, but divided by UTM source/campaign.

## The Data

We have generated some fake data for you: `users.csv` and `events.jsonl`.

The user table includes:
  - user ID
  - tracking ID
  - email address

The event types are:
  - signup completed
  - trial started
  - subscription started
  - subscription cancelled

## Important

Pay attention to:
  - modelling (a good model should be future-proof)
  - unit tests (no end-to-end test required)
  - a README (explain how to use the code, how to read the code, and which design decisions you made)
  - try to spend at most 8 hours on the assignment
